use crate::segment::{Record, Segment, SegmentWriter};
use serde::{Deserialize, Serialize};
use std::cmp::{max, min};
use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::sync::{mpsc, Mutex};
use std::thread::{spawn, JoinHandle};
use std::time::{Duration, SystemTime};

/// A slog (segment log) is a named and ordered series of segments.
pub(crate) struct Slog {
    root: PathBuf,
    name: String,
    current: usize,
    pending: Vec<Record>,
    pending_size: usize,
    time_range: Option<RangeInclusive<SystemTime>>,
    writer: SlogThreadControl,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Index {
    pub segment: usize,
    pub record: usize,
}

impl Slog {
    pub fn attach(root: PathBuf, name: String, current: usize) -> Self {
        let writer = SlogThread::spawn(root.clone(), name.clone(), current);
        Slog {
            root,
            name,
            current,
            writer,
            pending: vec![],
            pending_size: 0,
            time_range: None,
        }
    }

    pub(crate) fn segment_from_name(root: &PathBuf, name: &str, segment_ix: usize) -> Segment {
        let file = PathBuf::from(format!("{}-{}", name, segment_ix));
        Segment::at(root.join(file))
    }

    pub(crate) fn get_segment(&self, segment_ix: usize) -> Segment {
        Slog::segment_from_name(&self.root, &self.name, segment_ix)
    }

    pub(crate) fn get_record(&self, ix: Index) -> Option<Record> {
        assert!(ix.segment <= self.current);
        if ix.segment == self.current {
            self.pending.get(ix.record).cloned()
        } else {
            self.get_segment(ix.segment)
                .read()
                .read_all()
                .get(ix.record)
                .cloned()
        }
    }

    pub(crate) fn append(&mut self, r: &Record) -> Index {
        let record = self.pending.len();
        self.pending_size += r.message.len();
        self.pending.push(r.clone());
        self.time_range = self
            .time_range
            .clone()
            .map(|range| (min(*range.start(), r.time)..=max(*range.end(), r.time)))
            .or(Some(r.time..=r.time));
        Index {
            segment: self.current,
            record,
        }
    }

    pub(crate) fn current_segment_ix(&self) -> usize {
        self.current
    }

    pub(crate) fn current_len(&self) -> usize {
        self.pending.len()
    }

    pub(crate) fn current_size(&self) -> usize {
        self.pending_size
    }

    pub(crate) fn current_time_range(&self) -> Option<RangeInclusive<SystemTime>> {
        self.time_range.clone()
    }

    pub(crate) fn roll(&mut self) -> () {
        if !self
            .writer
            .try_send(std::mem::replace(&mut self.pending, vec![]))
        {
            panic!("log overrun")
        }
        self.pending_size = 0;
        self.time_range = None;
        self.current += 1;
    }

    pub(crate) fn commit(&mut self) {
        self.writer.commit();
    }
}

struct SlogThread {
    writer: SegmentWriter,
}

enum SlogThreadMessage {
    Write(Vec<Record>),
    Close,
}

struct SlogThreadControl {
    write_handle: JoinHandle<()>,
    ready: bool,
    tx: Mutex<mpsc::Sender<SlogThreadMessage>>,
    rx: Mutex<mpsc::Receiver<()>>,
}

impl SlogThreadControl {
    fn try_send(&mut self, rs: Vec<Record>) -> bool {
        if !self.ready {
            match self
                .rx
                .lock()
                .expect("rx lock")
                .recv_timeout(Duration::from_millis(1000))
            {
                Ok(()) => self.ready = true,
                Err(_) => return false,
            }
        }
        self.tx
            .lock()
            .expect("tx lock")
            .send(SlogThreadMessage::Write(rs));
        self.ready = false;
        true
    }

    fn commit(&mut self) {
        if !self.ready {
            self.rx.lock().expect("rx lock").recv().unwrap();
            self.ready = true;
        }
    }
}

impl Drop for SlogThreadControl {
    fn drop(&mut self) {
        self.tx
            .lock()
            .expect("tx lock")
            .send(SlogThreadMessage::Close);
    }
}

impl SlogThread {
    fn spawn(root: PathBuf, name: String, mut current: usize) -> SlogThreadControl {
        let (tx, rx_records) = mpsc::channel();
        let (tx_done, rx) = mpsc::channel();

        let write_handle = spawn(move || {
            let mut active = true;
            while active {
                let mut segment = Slog::segment_from_name(&root, &name, current).create();
                match rx_records.recv().unwrap() {
                    SlogThreadMessage::Write(rs) => {
                        segment.log(rs);
                        segment.close();
                        tx_done.send(()).unwrap();
                        current += 1;
                    }
                    SlogThreadMessage::Close => active = false,
                }
            }
        });

        SlogThreadControl {
            write_handle,
            ready: true,
            tx: Mutex::new(tx),
            rx: Mutex::new(rx),
        }
    }
}
