use std::collections::HashSet;
use tokio::prelude::*;
use raft::*;

struct Pending<'a> {
    id: &'a String,
    response: Box<VoteResponse>
}

pub struct State<'a> {
    votes: HashSet<&'a String>,
    ticks: usize,
    pending: Vec<Pending<'a>>
}

impl<'a> State<'a> {
    pub fn new () -> Self {
        State {
            votes: HashSet::new(),
            ticks: 0,
            pending: vec![]
        }
    }
}

pub fn become_candidate<'a, Record: Debug> (raft: &'a mut Raft<'a, Record>) {
    raft.role = Role::Candidate;
    start_election(raft);
}

pub fn start_election<'a, Record: Debug> (raft: &'a mut Raft<'a, Record>) {
    let term = raft.log.get_current_term();
    raft.log.set_current_term(term + 1);

    let last_log = raft.get_last_log_entry();

    let ref mut state = raft.volatile_state;
    let ref cluster = raft.cluster;

    let ref mut election = state.candidate;
    election.votes = HashSet::new();
    election.votes.insert(&cluster.id);
    election.ticks = 0;

    let link = raft.link;

    election.pending = cluster.peers.iter().map(|id| {
        let response: Box<VoteResponse> = link.request_vote(id, RequestVote {
            candidate_id: cluster.id.to_string(),
            last_log: last_log.clone(),
            term: term
        });

        Pending { id: id, response: response }
    }).collect();
}

pub fn tick_election<'a, Record: Debug> (raft: &'a mut Raft<'a, Record>) -> bool {
    let (majority, timeout) = {
        let ref config = raft.config;
        let ref mut state = raft.volatile_state;
        let ref mut election = state.candidate;

        election.ticks += 1;

        for p in &mut election.pending {
            let id = p.id;
            match p.response.poll() {
                Ok(Async::Ready(message)) => {
                    if message.vote_granted {
                        election.votes.insert(&id);
                    }
                }
                _ => ()
            }
        }

        (
            election.votes.len() > (raft.cluster.peers.len() / 2) + 1,
            election.ticks > config.election_restart_ticks
        )
    };

    if majority {
        true
    } else {
        if timeout {
            start_election(raft);
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::log::MemoryLog;

    extern crate env_logger;

    fn cluster () -> Cluster {
        Cluster {
            id: "me".to_string(),
            peers: vec!["other".to_string()]
        }
    }

    fn boxed(raw: Vec<(u64, u64)>) -> Vec<(u64, Box<u64>)> {
        raw.iter().map(|(t, v)| (*t, Box::new(*v))).collect()
    }

    fn unboxed(records: &Vec<(u64, Box<u64>)>) -> Vec<(u64, u64)> {
        records.iter().map(|&(t, ref v)| (t, *v.clone())).collect()
    }

    #[test]
    fn majority_election_succeeds() {
        let _ = env_logger::try_init();
        let mut log: MemoryLog<u64> = MemoryLog::new();
        let link: NullLink = NullLink::new();
        {
            let mut raft: Raft<u64> = Raft::new(cluster(), &DEFAULT_CONFIG, &mut log, &link);

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
}
