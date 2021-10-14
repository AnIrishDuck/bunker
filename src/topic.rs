use crate::segment::Record;
use crate::slog::{Index, Slog};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::fs;
use std::ops::{Bound, Range, RangeBounds, RangeInclusive};
use std::path::Path;
use std::time::{Duration, Instant, SystemTime};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SegmentSpan {
    time: RangeInclusive<SystemTime>,
    index: Range<u128>,
}

struct Retention {
    max_size: usize,
    max_index: usize,
    max_duration: Option<Duration>,
}

impl Retention {
    pub const DEFAULT: Retention = Retention {
        max_size: 100 * 1024 * 1024,
        max_index: 100000,
        max_duration: None,
    };
}

struct Topic {
    name: String,
    messages: Slog,
    indices: Indices,
    last_roll: Instant,
    retain: Retention,
    state: State,
}

struct Indices {
    by_start_time: BTreeMap<SystemTime, Vec<usize>>,
    by_end_time: BTreeMap<SystemTime, Vec<usize>>,
    by_start_ix: BTreeMap<u128, usize>,
}

impl Indices {
    fn new() -> Self {
        Indices {
            by_start_time: BTreeMap::new(),
            by_end_time: BTreeMap::new(),
            by_start_ix: BTreeMap::new(),
        }
    }

    fn update(&mut self, segment_ix: usize, ixs: &SegmentSpan) {
        let t = *ixs.time.start();
        let v = self.by_start_time.entry(t).or_insert(vec![]);
        v.push(segment_ix);

        let t = *ixs.time.end();
        let v = self.by_end_time.entry(t).or_insert(vec![]);
        v.push(segment_ix);

        self.by_start_ix.insert(ixs.index.start, segment_ix);
    }
}

#[derive(Serialize, Deserialize)]
struct State {
    spans: Vec<SegmentSpan>,
}

impl Topic {
    fn attach(name: String, retain: Retention) -> Self {
        let path = Topic::named_state_path(&name);
        let (state, messages, indices) = if Path::new(&path).exists() {
            let file = fs::File::open(path).unwrap();
            let state: State = serde_json::from_reader(file).unwrap();
            let messages = Slog::attach(name.clone(), state.spans.len());
            let mut indices = Indices::new();
            for (ix, span) in state.spans.iter().enumerate() {
                indices.update(ix, span);
            }
            dbg!("read state", &state.spans);
            dbg!("indices", &indices.by_start_ix);
            (state, messages, indices)
        } else {
            dbg!("creating");
            (
                State { spans: vec![] },
                Slog::attach(name.clone(), 0),
                Indices::new(),
            )
        };

        Topic {
            name,
            messages,
            indices,
            last_roll: Instant::now(),
            retain,
            state,
        }
    }

    fn roll(&mut self, time: RangeInclusive<SystemTime>) {
        let start = self.open_index();
        let size = u128::try_from(self.messages.current_len()).unwrap();
        let span = SegmentSpan {
            time,
            index: (start..start + size),
        };
        self.indices
            .update(self.messages.current_segment_ix(), &span);
        self.state.spans.push(span);
        self.last_roll = Instant::now();
        self.messages.roll();
        self.sync_state();
    }

    fn roll_when_needed(&mut self) {
        if let Some(time) = self.messages.current_time_range() {
            if let Some(d) = self.retain.max_duration {
                if Instant::now() - self.last_roll > d {
                    return self.roll(time);
                }
            }

            if self.messages.current_len() > self.retain.max_index {
                return self.roll(time);
            }

            if self.messages.current_size() > self.retain.max_size {
                return self.roll(time);
            }
        }
    }

    fn open_index(&self) -> u128 {
        self.state.spans.last().map(|r| r.index.end).unwrap_or(0)
    }

    fn append(&mut self, r: Record) -> u128 {
        self.roll_when_needed();
        let index = self.messages.append(r);
        self.open_index() + u128::try_from(index.record).unwrap()
    }

    fn named_state_path(name: &str) -> String {
        format!("{}.json", name)
    }

    fn state_path(&self) -> String {
        Topic::named_state_path(&self.name)
    }

    fn sync_state(&mut self) {
        let file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(self.state_path())
            .unwrap();

        let open = self.open_index();
        serde_json::to_writer_pretty(&file, &self.state).unwrap();
        file.sync_data().unwrap();
    }

    fn get_record_by_index(&self, index: u128) -> Option<Record> {
        let open = self.open_index();
        let slog_index = if index >= open {
            Some(Index {
                record: usize::try_from(index - open).unwrap(),
                segment: self.state.spans.len(),
            })
        } else {
            self.indices
                .by_start_ix
                .range(..=index)
                .last()
                .map(|(start, segment)| {
                    let segment = segment.clone();
                    Index {
                        record: usize::try_from(index - start).unwrap(),
                        segment,
                    }
                })
        };
        dbg!(&slog_index);

        slog_index.and_then(|ix| self.messages.get_record(ix))
    }

    fn get_records_by_time<R>(&self, query: R) -> Vec<Record>
    where
        R: RangeBounds<SystemTime> + Clone,
    {
        // all segments where the end is after the start of our range
        let start_range = (Bound::Unbounded, query.end_bound());
        let end_during = self
            .indices
            .by_end_time
            .range(start_range)
            .flat_map(|(_, segments)| segments.iter().cloned());

        // all segments where the start is before the end of our range, that weren't previously enumerated.
        let start_during = self
            .indices
            .by_start_time
            .range((Bound::Unbounded, query.end_bound()))
            .flat_map(|(_, segments)| segments.iter().cloned())
            .filter(|segment| {
                self.state
                    .spans
                    .get(*segment)
                    .map(|span| !start_range.contains(span.time.end()))
                    .unwrap_or(false)
            });

        end_during
            .chain(start_during)
            .flat_map(|segment| {
                self.messages
                    .get_segment(segment.clone())
                    .read()
                    .read_all()
                    .into_iter()
                    .filter(|r| query.contains(&r.time))
            })
            .collect()
    }

    fn close(mut self) {
        let range = self.messages.current_time_range();
        if let Some(time) = range {
            self.roll(time);
        }
        self.sync_state();
        // TODO order of operations is wrong here, due to ownership issues
        self.messages.close();
    }
}

mod test {
    use super::*;
    use parquet::data_type::ByteArray;

    #[test]
    fn test_append_get() {
        let mut t = Topic::attach(String::from("testing"), Retention::DEFAULT);

        let records: Vec<_> = vec!["abc", "def", "ghi", "jkl", "mno", "p"]
            .into_iter()
            .map(|message| Record {
                time: SystemTime::UNIX_EPOCH,
                message: ByteArray::from(message),
            })
            .collect();

        for record in records.iter() {
            t.append(record.clone());
        }

        for (ix, record) in records.iter().enumerate() {
            assert_eq!(
                t.get_record_by_index(u128::try_from(ix).unwrap()),
                Some(record.clone())
            );
        }
        assert_eq!(
            t.get_record_by_index(u128::try_from(records.len()).unwrap()),
            None
        );
    }

    #[test]
    fn test_rolling_get() {
        let mut t = Topic::attach(
            String::from("testing-roll"),
            Retention {
                max_index: 2,
                max_size: 100000,
                max_duration: None,
            },
        );

        let records: Vec<_> = vec!["abc", "def", "ghi", "jkl", "mno", "p"]
            .into_iter()
            .map(|message| Record {
                time: SystemTime::UNIX_EPOCH,
                message: ByteArray::from(message),
            })
            .collect();

        for record in records.iter() {
            t.append(record.clone());
        }

        for (ix, record) in records.iter().enumerate() {
            assert_eq!(
                t.get_record_by_index(u128::try_from(ix).unwrap()),
                Some(record.clone())
            );
        }
        assert_eq!(
            t.get_record_by_index(u128::try_from(records.len()).unwrap()),
            None
        );
    }

    #[test]
    fn test_durability() {
        let records: Vec<_> = vec!["abc", "def", "ghi", "jkl", "mno", "p"]
            .into_iter()
            .map(|message| Record {
                time: SystemTime::UNIX_EPOCH,
                message: ByteArray::from(message),
            })
            .collect();
        {
            let mut t = Topic::attach(
                String::from("testing-store"),
                Retention {
                    max_index: 2,
                    max_size: 100000,
                    max_duration: None,
                },
            );

            for record in records.iter() {
                t.append(record.clone());
            }
            t.close();
        }

        {
            let t = Topic::attach(
                String::from("testing-store"),
                Retention {
                    max_index: 2,
                    max_size: 100000,
                    max_duration: None,
                },
            );

            for (ix, record) in records.iter().enumerate() {
                assert_eq!(
                    t.get_record_by_index(u128::try_from(ix).unwrap()),
                    Some(record.clone())
                );
            }
            assert_eq!(
                t.get_record_by_index(u128::try_from(records.len()).unwrap()),
                None
            );
        }
    }
}
