use raft::Log;
use std::cmp::min;

pub struct MemoryLog<Record> {
    pub term: u64,
    pub voted_for: Option<String>,
    pub records: Vec<(u64, Box<Record>)>
}

impl<Record> MemoryLog<Record> {
    pub fn new () -> Self {
        MemoryLog { term: 0, voted_for: None, records: vec![] }
    }
}

impl<Record: Clone> Log<Record> for MemoryLog<Record> {
    fn get_current_term (&self) -> u64 {
        self.term
    }

    fn set_current_term (&mut self, term: u64) {
        self.term = term;
    }

    fn get_voted_for (&self) -> &Option<String> {
        &self.voted_for
    }

    fn set_voted_for (&mut self, candidate: Option<String>) {
        self.voted_for = candidate
    }

    fn get_index (&self) -> u64 {
        self.records.len() as u64
    }

    fn get_entry (&self, index: u64) -> &(u64, Box<Record>) {
        &self.records[index as usize]
    }

    fn insert (&mut self, index: u64, records: Vec<(u64, Box<Record>)>) {
        self.records.truncate(index as usize);
        self.records.extend(records);
    }

    fn get_batch (&self, index: u64) -> Vec<(u64, Box<Record>)> {
        let end = min((index + 5) as usize, self.records.len());
        self.records[index as usize..end].to_vec()
    }
}
