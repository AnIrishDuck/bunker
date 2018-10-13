use raft::*;

pub struct State {
    heartbeat_ticks: u64
}

impl State {
    pub fn new () -> Self {
        State { heartbeat_ticks: 0 }
    }
}

pub fn become_follower<Record> (raft: &mut Raft<Record>) {
    raft.volatile_state.follower = State::new();
    raft.role = Role::Follower;
}

pub fn tick<'a, Record: Debug> (raft: &mut Raft<'a, Record>) {
    let ticks = {
        let ref mut ticks = raft.volatile_state.follower.heartbeat_ticks;
        *ticks += 1;
        *ticks
    };

    let timeout = raft.config.election_restart_ticks as u64;
    if ticks > timeout {
        info!("Leader timed out, becoming candidate");
        candidate::become_candidate(raft);
    }
}

pub fn append_entries<Record> (raft: &mut Raft<Record>, request: AppendEntries<Record>) -> bool {
    let current_term = raft.log.get_current_term();

    raft.volatile_state.follower.heartbeat_ticks = 0;

    let request_from_prior_term = request.term < current_term;
    let prior_index = request.previous_entry.index;
    let count = request.entries.len() as u64;

    let inconsistent = {
        if raft.log.get_index() == 0 {
            prior_index != 0
        } else if raft.log.get_index() <= prior_index {
            true
        } else {
            let entry = raft.log.get_entry(prior_index);
            let check_term = entry.map(|(t, _)| t).unwrap_or(0);
            trace!("Prior entry term: {}", check_term);
            check_term != request.previous_entry.term
        }
    };


    let success = if request_from_prior_term || inconsistent {
        false
    } else {
        let prior_index = request.previous_entry.index;
        let last_index = prior_index + count;

        raft.log.insert(prior_index + 1, request.entries);
        trace!("Log updated; new length: {}", last_index);

        if request.leader_commit > raft.volatile_state.commit_index {
            let min_index = min(request.leader_commit, last_index);
            raft.volatile_state.commit_index = min_index;
            trace!("Committed entries, new commit index: {}", min_index);
        }

        true
    };

    success
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::log::MemoryLog;

    extern crate env_logger;


    fn single_node_cluster<'a> (id: &'a String) -> Cluster<'a> {
        Cluster {
            id: &id,
            peers: vec![&id]
        }
    }

    fn boxed(raw: Vec<(u64, u64)>) -> Vec<(u64, Box<u64>)> {
        raw.iter().map(|(t, v)| (*t, Box::new(*v))).collect()
    }

    fn unboxed(records: &Vec<(u64, Box<u64>)>) -> Vec<(u64, u64)> {
        records.iter().map(|&(t, ref v)| (t, *v.clone())).collect()
    }

    fn record_vec(log: &MemoryLog<u64>) -> Vec<(u64, u64)> {
        let ref records = log.state.borrow().records;
        unboxed(records)
    }

    #[test]
    fn append_entries_from_empty() {
        let _ = env_logger::try_init();
        let id = "me".to_owned();
        let cluster = single_node_cluster(&id);
        let log: MemoryLog<u64> = MemoryLog::new();
        let link: NullLink = NullLink::new();
        {
            let mut raft: Raft<u64> = Raft::new(cluster, &DEFAULT_CONFIG, Box::new(log.clone()), Box::new(link));

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
        assert_eq!(record_vec(&log), vec![(0, 1), (0, 2), (0, 3)]);
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
        let id = "me".to_owned();
        let cluster = single_node_cluster(&id);
        let log: MemoryLog<u64> = MemoryLog::new();
        let link: NullLink = NullLink::new();
        {
            let mut raft: Raft<u64> = Raft::new(cluster, &DEFAULT_CONFIG, Box::new(log.clone()), Box::new(link));

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

        assert_eq!(record_vec(&log), vec![
            (1, 0),
            (1, 0),
            (1, 0),
            (4, 1),
            (4, 2)
        ]);
    }
}
