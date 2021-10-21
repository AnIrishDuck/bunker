pub use crate::segment::Record;
pub use crate::slog::Index;
use crate::slog::Slog;
use crate::topic_state::TopicState as PartitionState;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fs;
use std::ops::{RangeBounds, RangeInclusive};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SegmentSpan<R>
where
    R: Clone + RangeBounds<u128>,
{
    pub time: RangeInclusive<SystemTime>,
    pub index: R,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Retention {
    pub max_size: usize,
    pub max_index: usize,
    pub max_duration: Option<Duration>,
}

impl Retention {
    pub const DEFAULT: Retention = Retention {
        max_size: 100 * 1024 * 1024,
        max_index: 100000,
        max_duration: None,
    };
}

pub struct Topic {
    root: PathBuf,
    name: String,
    state: RwLock<TopicState>,
    partitions: RwLock<HashMap<String, RwLock<Partition>>>,
    retain: Retention,
}

struct Partition {
    topic: String,
    name: String,
    messages: Slog,
    open_index: u128,
    last_roll: Instant,
    retain: Retention,
    state: Mutex<PartitionState>,
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct TopicState {
    partitions: Vec<String>,
}

impl Topic {
    pub fn attach(root: PathBuf, name: String, retain: Retention) -> Self {
        if !Path::exists(&root) {
            fs::create_dir(&root).unwrap();
        }
        let path = Topic::state_path(&root, &name);
        let state = if Path::exists(&path) {
            serde_json::from_reader(fs::File::open(path).unwrap()).unwrap()
        } else {
            TopicState::default()
        };

        let partitions = state
            .partitions
            .iter()
            .map(|partition| {
                let slog = Partition::attach(
                    Topic::partition_root(&root, &name),
                    &name,
                    partition,
                    retain.clone(),
                );
                (partition.clone(), RwLock::new(slog))
            })
            .collect();

        Topic {
            root,
            name,
            state: RwLock::new(state),
            partitions: RwLock::new(partitions),
            retain,
        }
    }

    fn partition_root(root: &PathBuf, name: &str) -> PathBuf {
        root.join(name)
    }

    fn state_path(root: &PathBuf, name: &str) -> PathBuf {
        root.join(format!("{}.json", name))
    }

    pub fn append(&self, partition_name: &str, rs: &[Record]) -> Option<u128> {
        let partitions = self.partitions.read().expect("partition map read");
        if let Some(part) = partitions.get(partition_name) {
            part.write().expect("partition write").append(rs)
        } else {
            drop(partitions);
            let mut partitions = self.partitions.write().expect("partition map write");
            let mut part = Partition::attach(
                Topic::partition_root(&self.root, &self.name),
                &self.name,
                partition_name,
                self.retain.clone(),
            );
            let ix = part.append(rs);
            partitions.insert(partition_name.to_string(), RwLock::new(part));

            {
                let mut state = self.state.write().expect("state write");
                state.partitions.push(partition_name.to_string());
                let path = Topic::state_path(&self.root, &self.name);
                let file = fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&path)
                    .unwrap();
                serde_json::to_writer(&file, &state.clone()).unwrap();
                file.sync_data().unwrap();
            }
            ix
        }
    }

    pub fn get_record_by_index(&self, partition_name: &str, index: u128) -> Option<Record> {
        self.partitions
            .read()
            .expect("partition map read")
            .get(partition_name)
            .and_then(|part| {
                part.read()
                    .expect("partition read")
                    .get_record_by_index(index)
            })
    }

    pub fn commit(&self) {
        for (_, part) in self.partitions.read().expect("partition map read").iter() {
            part.write().expect("partition write").commit();
        }
    }
}

impl Partition {
    fn attach(root: PathBuf, topic: &str, name: &str, retain: Retention) -> Partition {
        if !Path::exists(&root) {
            fs::create_dir(&root).unwrap();
        }
        let path = Partition::state_path(&root, name);
        let state = PartitionState::attach(PathBuf::from(path));
        let segment = state.get_active_segment(name).unwrap_or(0);
        let messages = Slog::attach(root.clone(), Partition::slog_name(&topic, name), segment);
        let open_index = state.open_index(name);
        Partition {
            state: Mutex::new(state),
            open_index,
            topic: String::from(topic),
            name: String::from(name),
            messages,
            last_roll: Instant::now(),
            retain,
        }
    }

    fn state_path(root: &PathBuf, name: &str) -> PathBuf {
        let name = format!("{}-meta.sqlite", name);
        root.join(name)
    }

    fn slog_name(topic: &str, name: &str) -> String {
        format!("{}-{}", topic, name)
    }

    fn append(&mut self, rs: &[Record]) -> Option<u128> {
        let start = self.open_index;
        let mut index = None;
        for r in rs {
            let index = Some(self.messages.append(r));
            self.roll_when_needed(start);
        }
        index.map(|i: Index| start + u128::try_from(i.record).unwrap())
    }

    fn get_record_by_index(&self, index: u128) -> Option<Record> {
        let open = self.open_index;
        let state = self.state.lock().expect("partition state");
        let slog_index = if index >= open {
            Some(Index {
                record: usize::try_from(index - open).unwrap(),
                segment: state.get_active_segment(&self.name).unwrap_or(0),
            })
        } else {
            state
                .get_segment_for_ix(&self.name, u64::try_from(index).unwrap())
                .map(|segment| {
                    let span = state.get_segment_span(&self.name, segment).unwrap();
                    Index {
                        record: usize::try_from(index - span.index.start).unwrap(),
                        segment,
                    }
                })
        };

        slog_index.and_then(|ix| self.messages.get_record(ix))
    }

    fn roll(&mut self, start: u128, time: RangeInclusive<SystemTime>) {
        let size = u128::try_from(self.messages.current_len()).unwrap();
        let span = SegmentSpan {
            time,
            index: start..start + size,
        };
        self.state.lock().expect("partition state").update(
            &self.name,
            self.messages.current_segment_ix(),
            &span,
        );
        self.last_roll = Instant::now();
        self.open_index = span.index.end;
        self.messages.roll();
    }

    fn roll_when_needed(&mut self, start: u128) {
        if let Some(time) = self.messages.current_time_range() {
            if let Some(d) = self.retain.max_duration {
                if Instant::now() - self.last_roll > d {
                    return self.roll(start, time);
                }
            }

            if self.messages.current_len() > self.retain.max_index {
                return self.roll(start, time);
            }

            if self.messages.current_size() > self.retain.max_size {
                return self.roll(start, time);
            }
        }
    }

    fn commit(&mut self) {
        // await completion of any pending write
        self.messages.commit();
        if let Some(time) = self.messages.current_time_range() {
            // flush remaining messages
            let start = self.open_index;
            self.roll(start, time);
            self.messages.commit();
        }
    }
}

mod test {
    use super::*;
    use parquet::data_type::ByteArray;
    use std::ops::Deref;
    use std::sync::mpsc::channel;
    use std::thread;
    use tempfile::tempdir;

    #[test]
    fn test_append_get() {
        let dir = tempdir().unwrap();
        let root = PathBuf::from(dir.path());
        let mut t = Topic::attach(root, String::from("testing"), Retention::DEFAULT);
        let p = "default";

        let records: Vec<_> = vec!["abc", "def", "ghi", "jkl", "mno", "p"]
            .into_iter()
            .map(|message| Record {
                time: SystemTime::UNIX_EPOCH,
                message: ByteArray::from(message),
            })
            .collect();

        for record in records.iter() {
            t.append(p, &vec![record.clone()]);
        }

        for (ix, record) in records.iter().enumerate() {
            assert_eq!(
                t.get_record_by_index(p, u128::try_from(ix).unwrap()),
                Some(record.clone())
            );
        }
        assert_eq!(
            t.get_record_by_index(p, u128::try_from(records.len()).unwrap()),
            None
        );
    }

    #[test]
    fn test_rolling_get() {
        let dir = tempdir().unwrap();
        let root = PathBuf::from(dir.path());
        let mut t = Topic::attach(
            root,
            String::from("testing-roll"),
            Retention {
                max_index: 2,
                max_size: 100000,
                max_duration: None,
            },
        );
        let p = "default";

        let records: Vec<_> = vec!["abc", "def", "ghi", "jkl", "mno", "p"]
            .into_iter()
            .map(|message| Record {
                time: SystemTime::UNIX_EPOCH,
                message: ByteArray::from(message),
            })
            .collect();

        for record in records.iter() {
            t.append(p, &vec![record.clone()]);
        }
        t.commit();

        for (ix, record) in records.iter().enumerate() {
            assert_eq!(
                t.get_record_by_index(p, u128::try_from(ix).unwrap()),
                Some(record.clone())
            );
        }
        assert_eq!(
            t.get_record_by_index(p, u128::try_from(records.len()).unwrap()),
            None
        );
    }

    #[test]
    fn test_durability() {
        let dir = tempdir().unwrap();
        let root = PathBuf::from(dir.path());
        let p = "default";
        let records: Vec<_> = vec!["abc", "def", "ghi", "jkl", "mno", "p"]
            .into_iter()
            .map(|message| Record {
                time: SystemTime::UNIX_EPOCH,
                message: ByteArray::from(message),
            })
            .collect();
        {
            let mut t = Topic::attach(
                root.clone(),
                String::from("testing-store"),
                Retention {
                    max_index: 2,
                    max_size: 100000,
                    max_duration: None,
                },
            );

            for record in records.iter() {
                t.append(p, &vec![record.clone()]);
            }
            t.commit();
        }

        {
            let t = Topic::attach(
                root,
                String::from("testing-store"),
                Retention {
                    max_index: 2,
                    max_size: 100000,
                    max_duration: None,
                },
            );

            for (ix, record) in records.iter().enumerate() {
                assert_eq!(
                    t.get_record_by_index(p, u128::try_from(ix).unwrap()),
                    Some(record.clone())
                );
            }
            assert_eq!(
                t.get_record_by_index(p, u128::try_from(records.len()).unwrap()),
                None
            );
        }
    }

    #[ignore]
    #[test]
    fn test_bench() {
        let dir = tempdir().unwrap();
        let root = PathBuf::from(dir.path());
        let partitions = 4;
        let sample = 40 * 1000;
        let total = sample * 15;

        let mut handles = vec![];
        let (otx, rx) = channel();
        for part in 0..partitions {
            let tx = otx.clone();
            let data = vec!["x"; 128].join("");
            let thread_root = root.clone();
            let p = format!("part-{}", part);
            let handle = thread::spawn(move || {
                let mut t = Topic::attach(thread_root, String::from("testing"), Retention::DEFAULT);

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

                let mut ix = 0;
                let mut prev = None;
                use itermore::IterMore;
                for rs in records.iter().cycle().take(total).chunks::<1000>() {
                    let now = SystemTime::now();
                    let rs: Vec<Record> = rs
                        .iter()
                        .cloned()
                        .map(|r| {
                            let mut c = r.clone();
                            c.time = now;
                            c
                        })
                        .collect();

                    t.append(&p, &rs);
                    if let Some(prev_ix) = prev {
                        tx.send(ix - prev_ix).unwrap();
                    }
                    prev = Some(ix);
                    ix += rs.len();
                }
                t.commit();
                tx.send(total - prev.unwrap_or(0)).unwrap();
            });
            handles.push(handle);
        }

        let start = Instant::now();
        let mut written = 0;
        while written < total * partitions {
            written += rx.recv_timeout(Duration::from_secs(60)).unwrap();
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
