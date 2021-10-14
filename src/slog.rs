use crate::segment::{Record, Segment, SegmentWriter};
use std::cmp::{max, min};
use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::time::SystemTime;

/// A slog (segment log) is a named and ordered series of segments.
pub(crate) struct Slog {
    name: String,
    current: usize,
    writer: SegmentWriter,
    pending: Vec<Record>,
    pending_size: usize,
    time_range: Option<RangeInclusive<SystemTime>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Index {
    pub segment: usize,
    pub record: usize,
}

impl Slog {
    pub fn attach(name: String, current: usize) -> Self {
        let writer = Slog::segment_from_name(&name, current).create();
        Slog {
            name,
            current,
            writer,
            pending: vec![],
            pending_size: 0,
            time_range: None,
        }
    }

    pub(crate) fn segment_from_name(name: &str, segment_ix: usize) -> Segment {
        Segment::at(PathBuf::from(format!("{}-{}", name, segment_ix)))
    }

    pub(crate) fn get_segment(&self, segment_ix: usize) -> Segment {
        Slog::segment_from_name(&self.name, segment_ix)
    }

    pub(crate) fn get_record(&self, ix: Index) -> Option<Record> {
        dbg!(self.current);
        assert!(ix.segment <= self.current);
        if ix.segment == self.current {
            dbg!("pending-get");
            self.pending.get(ix.record).cloned()
        } else {
            dbg!("stored-get", self.get_segment(ix.segment).path());
            self.get_segment(ix.segment)
                .read()
                .read_all()
                .get(ix.record)
                .cloned()
        }
    }

    pub(crate) fn append(&mut self, r: Record) -> Index {
        let record = self.pending.len();
        self.pending_size += r.message.len();
        self.pending.push(r.clone());
        self.time_range = self
            .time_range
            .clone()
            .map(|range| (min(*range.start(), r.time)..=max(*range.end(), r.time)))
            .or(Some(r.time..=r.time));
        dbg!(self.current, &r);
        self.writer.log(r);
        Index {
            segment: self.current,
            record,
        }
    }

    pub(crate) fn sync(&mut self) -> () {
        self.writer.sync().unwrap()
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
        self.pending_size = 0;
        self.pending = vec![];
        self.time_range = None;
        self.current += 1;
        let new_writer = self.get_segment(self.current).create();
        let old = std::mem::replace(&mut self.writer, new_writer);
        old.close();
    }

    pub(crate) fn close(self) -> () {
        self.writer.close()
    }
}
