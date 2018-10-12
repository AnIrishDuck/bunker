use std::cmp::{min, max};
use tokio::prelude::*;
use raft::*;

struct Follower<'a> {
    id: &'a String,
    next_index: u64,
    match_index: u64,
    pending: Option<Box<AppendResponse>>
}

pub struct State<'a> {
    followers: Vec<Follower<'a>>
}

impl<'a> State<'a> {
    pub fn new () -> Self {
        State {
            followers: vec![]
        }
    }
}

pub fn become_leader<'a, Record> (raft: &'a mut Raft<'a, Record>) {
    raft.role = Role::Leader;

    let followers = raft.cluster.peers.iter().map(|id| {
        Follower {
            id: id,
            next_index: raft.volatile_state.commit_index + 1,
            match_index: 0,
            pending: None
        }
    }).collect();

    raft.volatile_state.leader = State { followers: followers };
}

pub fn leader_push<'a, Record> (raft: &'a mut Raft<'a, Record>) {
    let term = raft.log.get_current_term();

    {
        let ref mut leader = raft.volatile_state.leader;
        for ref mut follower in &mut leader.followers {
            let send_more = match follower.pending {
                Some(ref mut response) => {
                    match response.poll() {
                        Ok(Async::NotReady) => false,
                        Ok(Async::Ready(append)) => {
                            if append.success {
                                follower.match_index = follower.next_index;
                                trace!(
                                    "appended to {} (now at {})",
                                    follower.id,
                                    follower.next_index
                                );
                            } else {
                                follower.next_index = min(0, follower.next_index - 1);
                                trace!(
                                    "{} rejected append, rewinding (now at {})",
                                    follower.id,
                                    follower.next_index
                                );
                            }
                            true
                        },
                        Err(string) => {
                            error!("Error pushing records: {}", string);
                            true
                        }
                    }
                },
                None => false
            };

            if send_more {
                let missing_entries = raft.log.get_index() <= follower.next_index;
                let (prior, records) = if missing_entries {
                    let (term, _) = raft.log.get_entry(follower.next_index);
                    (
                        LogEntry { index: follower.next_index, term: *term },
                        raft.log.get_batch(follower.next_index)
                    )
                } else { (LogEntry { index: 0, term: 0}, vec![]) };

                let count = records.len();
                trace!(
                    "sending {} to {} (at {:?})",
                    count,
                    follower.id,
                    prior
                );

                let response = raft.link.append_entries(follower.id, AppendEntries {
                    term: term,
                    previous_entry: prior,
                    entries: records,
                    leader_commit: raft.volatile_state.commit_index
                });

                follower.pending = Some(response);
            }
        }
    }

    {
        let ref leader = raft.volatile_state.leader;
        let mut matches: Vec<u64> = leader.followers.iter().map(|follower| {
            let (entry_term, _) = raft.log.get_entry(follower.match_index);
            if *entry_term == term {
                follower.match_index
            } else { 0 }
        }).collect();
        matches.sort_unstable();


        let ref mut commit = raft.volatile_state.commit_index;
        let middle = (matches.len() / 2) + if matches.len() % 2 == 0 { 0 } else { 1 };
        let next_commit = max(matches[middle], *commit);

        trace!("follower indices: {:?}; next commit: {}", matches, commit);
        *commit = next_commit;
    }
}
