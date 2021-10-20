use crate::segment::Record;
use crate::slog::{Index, Slog};
use crate::topic_state::TopicState;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::fs;
use std::ops::{Bound, Range, RangeBounds, RangeInclusive};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SegmentSpan<R>
where
    R: Clone + RangeBounds<u128>,
{
    pub time: RangeInclusive<SystemTime>,
    pub index: R,
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
    state: TopicState,
    last_roll: Instant,
    retain: Retention,
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

    fn update(&mut self, segment_ix: usize, ixs: &SegmentSpan<Range<u128>>) {
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
    spans: Vec<SegmentSpan<Range<u128>>>,
}

impl Topic {
    fn attach(name: String, retain: Retention) -> Self {
        let path = Topic::named_state_path(&name);
        let state = TopicState::attach(PathBuf::from(path));
        let messages = Slog::attach(name.clone(), state.get_active_segment().unwrap_or(0));

        Topic {
            name,
            messages,
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
            index: start..start + size,
        };
        self.state.update(self.messages.current_segment_ix(), &span);
        self.last_roll = Instant::now();
        self.messages.roll();
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
        self.state
            .get_active_segment()
            .and_then(|segment_ix| {
                self.state
                    .get_segment_span(segment_ix - 1)
                    .map(|span| span.index.end)
            })
            .unwrap_or(0)
    }

    fn append(&mut self, r: Record) -> u128 {
        self.roll_when_needed();
        let index = self.messages.append(r);
        self.open_index() + u128::try_from(index.record).unwrap()
    }

    fn named_state_path(name: &str) -> String {
        format!("{}-meta.sqlite", name)
    }

    fn state_path(&self) -> String {
        Topic::named_state_path(&self.name)
    }

    fn get_record_by_index(&self, index: u128) -> Option<Record> {
        let open = self.open_index();
        let slog_index = if index >= open {
            Some(Index {
                record: usize::try_from(index - open).unwrap(),
                segment: self.state.get_active_segment().unwrap_or(0),
            })
        } else {
            self.state
                .get_segment_for_ix(u64::try_from(index).unwrap())
                .map(|segment| {
                    let span = self.state.get_segment_span(segment).unwrap();
                    Index {
                        record: usize::try_from(index - span.index.start).unwrap(),
                        segment,
                    }
                })
        };

        slog_index.and_then(|ix| self.messages.get_record(ix))
    }

    fn commit(&mut self) {
        let range = self.messages.current_time_range();
        if let Some(time) = range {
            // TODO order of operations is wonky here, first commit clears any pending roll,
            // then roll is forced, then next commit waits for that roll to complete.
            self.messages.commit();
            self.roll(time);
            self.messages.commit();
        }
    }
}

mod test {
    use super::*;
    use parquet::data_type::ByteArray;
    use std::sync::mpsc::channel;
    use std::thread;

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
        t.commit();

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
            t.commit();
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

    #[test]
    fn test_bench() {
        let partitions = 4;
        let sample = 40 * 1000;
        let total = sample * 15;

        let mut handles = vec![];
        let (otx, rx) = channel();
        for part in 0..partitions {
            let tx = otx.clone();
            let data = vec!["x"; 128].join("");
            let handle = thread::spawn(move || {
                let mut t = Topic::attach(format!("testing-{}", part), Retention::DEFAULT);

                let seed = 0..sample;
                let records: Vec<_> = seed
                    .clone()
                    .into_iter()
                    .map(|message| Record {
                        time: SystemTime::UNIX_EPOCH,
                        message: ByteArray::from(
                            format!("{{ \"data\": \"{}-{}\" }}", data, message).as_str(),
                        ),
                    })
                    .collect();

                let mut prev = None;
                for (ix, record) in records.iter().cycle().take(total).enumerate() {
                    let mut r = record.clone();
                    r.time = SystemTime::now();
                    t.append(r);
                    if ix % 1000 == 0 {
                        if let Some(prev_ix) = prev {
                            tx.send(ix - prev_ix).unwrap();
                        }
                        prev = Some(ix);
                    }
                }
                t.commit();
                tx.send(total - prev.unwrap_or(0)).unwrap();
            });
            handles.push(handle);
        }

        let start = Instant::now();
        let mut written = 0;
        while written < total * partitions {
            written += rx.recv_timeout(Duration::from_secs(10)).unwrap();
            println!(
                "{}/{} elapsed: {}ms",
                written,
                total * partitions,
                (Instant::now() - start).as_millis()
            );
        }
        let elapsed_ms = (Instant::now() - start).as_millis();
        println!(
            "written: {} / elapsed: {}ms ({:.2}kw/s)",
            written,
            elapsed_ms,
            f64::from(i32::try_from(written).unwrap())
                / f64::from(i32::try_from(elapsed_ms).unwrap())
        );

        for handle in handles {
            handle.join().unwrap();
        }
    }
}
