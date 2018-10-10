use std::cmp::min;
use std::fmt::Debug;

/* prelude: definitions from page 4 of the raft paper */
pub trait Log<Record> {
    fn get_current_term (&mut self) -> u64;
    fn set_current_term (&mut self, term: u64);

    fn get_voted_for (&mut self) -> &Option<String>;
    fn set_voted_for (&mut self, candidate: Option<String>);

    fn get_index (&mut self) -> u64;
    fn get_entry (&mut self, index: u64) -> &(u64, Box<Record>);
    fn insert (&mut self, index: u64, records: Vec<(u64, Box<Record>)>);
}

pub trait StateMachine<Record> {
    fn apply (record: Box<Record>) -> bool;
}

pub struct VolatileState {
    commit_index: u64,
    // we will track last_applied in the state machine
}

#[derive(Debug)]
pub struct LogEntry {
    index: u64,
    term: u64
}

pub struct AppendEntries<Record> {
    term: u64,
    // we never ended up needing leader_id
    previous_entry: LogEntry,
    entries: Vec<(u64, Box<Record>)>,
    leader_commit: u64
}

#[derive(Debug)]
pub struct Append {
    term: u64,
    success: bool
}

#[derive(Debug)]
pub struct RequestVote {
    term: u64,
    candidate_id: String,
    last_log: LogEntry
}

#[derive(Debug)]
pub struct Vote {
    term: u64,
    vote_granted: bool
}

pub trait Link<Record> {
    fn append_entries(id: String, request: AppendEntries<Record>) -> Append;

    fn request_vote(id: String, request: RequestVote) -> Vote;
}

pub struct Raft<'a, Record> {
    volatile_state: VolatileState,
    log: &'a mut Log<Record>
}

impl<'a, Record: Debug> Raft<'a, Record> {
    pub fn new (log: &'a mut Log<Record>) -> Self {
        let state = VolatileState {
            commit_index: 0
        };

        Raft { log: log, volatile_state: state }
    }

    pub fn append_entries (&mut self, request: AppendEntries<Record>) -> Append {
        let current_term = self.log.get_current_term();

        let request_from_prior_term = request.term < current_term;
        let prior_index = request.previous_entry.index;
        let count = request.entries.len() as u64;

        debug!(
            "RX AppendEntries: {} at index {:?}",
            count,
            request.previous_entry,
        );
        let inconsistent = {
            if self.log.get_index() == 0 {
                prior_index != 0
            } else if self.log.get_index() <= prior_index {
                true
            } else {
                let (check_term, _record) = self.log.get_entry(prior_index);
                trace!("(prior entry term: {})", check_term);
                *check_term != request.previous_entry.term
            }
        };


        let success = if request_from_prior_term || inconsistent {
            false
        } else {
            let prior_index = request.previous_entry.index;
            let last_index = prior_index + count;

            self.log.insert(prior_index + 1, request.entries);
            trace!("Log updated; new length: {}", last_index);

            if request.leader_commit > self.volatile_state.commit_index {
                let min_index = min(request.leader_commit, last_index);
                self.volatile_state.commit_index = min_index;
                trace!("Committed entries, new commit index: {}", min_index);
            }

            true
        };

        let response = Append {
            term: current_term,
            success: success
        };
        debug!("TX: {:?}", response);
        response
    }

    pub fn request_vote (&mut self, request: RequestVote) -> Vote {
        let current_term = self.log.get_current_term();

        debug!("RX: {:?}", request);

        let vote_granted = if request.term < current_term { false } else {
            let prior_vote = {
                let voted_for = self.log.get_voted_for();

                trace!("prior vote: {:?}", voted_for);
                match voted_for {
                    Some(ref vote) => *vote == request.candidate_id,
                    None => true
                }
            };

            let index = self.log.get_index();
            let term = if index == 0 { 0 } else {
                let (last, _record) = self.log.get_entry(index - 1);
                *last
            };
            trace!("current log: term {} index {}", term, index);
            let log_current = if request.last_log.term == term {
                request.last_log.index >= index
            } else { request.last_log.term > term };

            prior_vote || log_current
        };

        if vote_granted {
            self.log.set_voted_for(Some(request.candidate_id));
        }

        let response = Vote {
            term: current_term,
            vote_granted: vote_granted
        };
        debug!("TX: {:?}", response);
        response
    }
}

pub struct MemoryLog<Record> {
    term: u64,
    voted_for: Option<String>,
    records: Vec<(u64, Box<Record>)>
}

impl<Record> MemoryLog<Record> {
    fn new () -> Self {
        MemoryLog { term: 0, voted_for: None, records: vec![] }
    }
}

impl<Record> Log<Record> for MemoryLog<Record> {
    fn get_current_term (&mut self) -> u64 {
        self.term
    }

    fn set_current_term (&mut self, term: u64) {
        self.term = term;
    }

    fn get_voted_for (&mut self) -> &Option<String> {
        &self.voted_for
    }

    fn set_voted_for (&mut self, candidate: Option<String>) {
        self.voted_for = candidate
    }

    fn get_index (&mut self) -> u64 {
        self.records.len() as u64
    }

    fn get_entry (&mut self, index: u64) -> &(u64, Box<Record>) {
        &self.records[index as usize]
    }

    fn insert (&mut self, index: u64, records: Vec<(u64, Box<Record>)>) {
        self.records.truncate(index as usize);
        self.records.extend(records);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    extern crate env_logger;

    fn boxed(raw: Vec<(u64, u64)>) -> Vec<(u64, Box<u64>)> {
        raw.iter().map(|(t, v)| (*t, Box::new(*v))).collect()
    }

    fn unboxed(records: &Vec<(u64, Box<u64>)>) -> Vec<(u64, u64)> {
        records.iter().map(|&(t, ref v)| (t, *v.clone())).collect()
    }

    #[test]
    fn append_entries_from_empty() {
        let _ = env_logger::try_init();
        let mut log: MemoryLog<u64> = MemoryLog::new();
        {
            let mut raft: Raft<u64> = Raft::new(&mut log);

            let response = raft.append_entries(AppendEntries {
                term: 0,
                previous_entry: LogEntry { term: 0, index: 0 },
                entries: boxed(vec![(0, 1), (0, 2), (0, 3)]),
                leader_commit: 10
            });

            assert_eq!(response.term, 0);
            assert_eq!(response.success, true);
            assert_eq!(raft.volatile_state.commit_index, 3);
        }
        assert_eq!(unboxed(&log.records), vec![(0, 1), (0, 2), (0, 3)]);
    }

    fn fill_term (raft: &mut Raft<u64>, term: u64, prior: LogEntry, count: u64) {
        let mut fill = Vec::new();
        fill.resize(count as usize, (term, 0));
        let response = raft.append_entries(AppendEntries {
            term: term,
            previous_entry: prior,
            entries: boxed(fill),
            leader_commit: 20
        });
        assert_eq!(response.success, true);
    }

    #[test]
    fn append_inconsistent_entries () {
        let _ = env_logger::try_init();
        let mut log: MemoryLog<u64> = MemoryLog::new();
        {
            let mut raft: Raft<u64> = Raft::new(&mut log);

            let response = raft.append_entries(AppendEntries {
                term: 0,
                previous_entry: LogEntry { term: 0, index: 5 },
                entries: boxed(vec![(0, 1),(0, 2),(0, 3)]),
                leader_commit: 0
            });
            assert_eq!(response.success, false);

            // see example f, figure 7 (page 7)
            fill_term(&mut raft, 1, LogEntry { term: 0, index: 0 }, 3);
            fill_term(&mut raft, 2, LogEntry { term: 1, index: 2 }, 3);
            fill_term(&mut raft, 3, LogEntry { term: 2, index: 5 }, 3);

            // per the protocol, a new leader initially would attempt to append
            // results from term 6
            let response = raft.append_entries(AppendEntries {
                term: 8,
                previous_entry: LogEntry { term: 5, index: 6 },
                entries: boxed(vec![(6, 1),(6, 2),(6, 3)]),
                leader_commit: 0
            });
            assert_eq!(response.success, false);

            // the leader would then iterate backwards until finding the index
            // where the logs are consistent
            let response = raft.append_entries(AppendEntries {
                term: 8,
                previous_entry: LogEntry { term: 1, index: 2 },
                entries: boxed(vec![(4, 1), (4, 2)]),
                leader_commit: 0
            });
            assert_eq!(response.success, true);
        }

        assert_eq!(unboxed(&log.records), vec![
            (1, 0),
            (1, 0),
            (1, 0),
            (4, 1),
            (4, 2)
        ]);
    }

    #[test]
    fn vote_granted () {
        let _ = env_logger::try_init();
        let mut log: MemoryLog<u64> = MemoryLog::new();
        {
            let mut raft: Raft<u64> = Raft::new(&mut log);
            let response = raft.request_vote(RequestVote {
                term: 0,
                candidate_id: "george michael".to_string(),
                last_log: LogEntry { term: 0, index: 5 }
            });
            assert_eq!(response.vote_granted, true);
        }
        assert_eq!(log.voted_for, Some("george michael".to_string()));
    }
}
