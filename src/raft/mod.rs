use std::cmp::min;
use std::fmt::Debug;

mod follower;
mod log;

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
    fn append_entries(&self,id: String, request: AppendEntries<Record>) -> Append;

    fn request_vote(&self, id: String, request: RequestVote) -> Vote;
}

pub enum State { Follower, Candidate, Leader }

pub struct Raft<'a, Record: 'a> {
    volatile_state: VolatileState,
    log: &'a mut Log<Record>,
    link: &'a Link<Record>,
    state: State
}

impl<'a, Record: Debug + 'a> Raft<'a, Record> {
    pub fn new (log: &'a mut Log<Record>, link: &'a Link<Record>) -> Self {
        let volatile = VolatileState {
            commit_index: 0
        };

        Raft {
            link: link,
            log: log,
            state: State::Follower,
            volatile_state: volatile
        }
    }

    pub fn append_entries (&mut self, request: AppendEntries<Record>) -> Append {
        let current_term = self.log.get_current_term();
        let count = request.entries.len() as u64;
        debug!(
            "RX AppendEntries: {} at index {:?}",
            count,
            request.previous_entry,
        );

        let success = match self.state {
            State::Follower => follower::append_entries(self, request),
            _ => false
        };
        let response = Append { term: current_term, success: success };

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

            let last = self.get_last_log_entry();
            trace!("last log entry: {:?}", last);
            let log_current = if request.last_log.term == last.term {
                request.last_log.index >= last.index
            } else { request.last_log.term > last.term };

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

    fn get_last_log_entry (&mut self) -> LogEntry {
        let index = self.log.get_index();
        let term = if index == 0 { 0 } else {
            let (last, _record) = self.log.get_entry(index - 1);
            *last
        };
        LogEntry { index: index, term: term }
    }
}


struct NullLink {}

impl NullLink {
    pub fn new () -> Self {
        NullLink { }
    }
}

impl<Record> Link<Record> for NullLink {
    fn append_entries(&self, _id: String, _request: AppendEntries<Record>) -> Append {
        Append { term: 0, success: false }
    }

    fn request_vote(&self, _id: String, _request: RequestVote) -> Vote {
        Vote { term: 0, vote_granted: false }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::log::MemoryLog;

    extern crate env_logger;

    #[test]
    fn vote_granted () {
        let _ = env_logger::try_init();
        let mut log: MemoryLog<u64> = MemoryLog::new();
        let link = NullLink::new();
        {
            let mut raft: Raft<u64> = Raft::new(&mut log, &link);
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
