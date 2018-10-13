use futures::{future, Future};
use std::cmp::min;
use std::fmt::{Debug};

mod follower;
mod candidate;
mod leader;
mod log;

/* prelude: definitions from page 4 of the raft paper */
pub trait Log<Record> {
    fn get_current_term (&self) -> u64;
    fn set_current_term (&mut self, term: u64);

    fn get_voted_for (&self) -> Option<String>;
    fn set_voted_for (&mut self, candidate: Option<String>);

    fn get_index (&self) -> u64;
    fn get_entry (&self, index: u64) -> Option<(u64, Box<Record>)>;
    fn insert (&mut self, index: u64, records: Vec<(u64, Box<Record>)>);

    fn get_batch (&self, index: u64) -> Vec<(u64, Box<Record>)>;
}

pub trait StateMachine<Record> {
    fn apply (record: Box<Record>) -> bool;
}

pub struct VolatileState<'a> {
    commit_index: u64,
    // we will track last_applied in the state machine
    candidate: candidate::State<'a>,
    leader: leader::State<'a>,
    follower: follower::State
}

pub struct Cluster<'a> {
    id: &'a String,
    peers: Vec<&'a String>
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    index: u64,
    term: u64
}

#[derive(Debug, Clone)]
pub struct AppendEntries<Record> {
    term: u64,
    // we never ended up needing leader_id
    previous_entry: LogEntry,
    entries: Vec<(u64, Box<Record>)>,
    leader_commit: u64
}

#[derive(Debug, Clone)]
pub struct Append {
    term: u64,
    success: bool
}

type AppendResponse = Future<Item=Append, Error=String>;

#[derive(Debug, Clone)]
pub struct RequestVote {
    term: u64,
    candidate_id: String,
    last_log: LogEntry
}

#[derive(Debug, Clone)]
pub struct Vote {
    term: u64,
    vote_granted: bool
}

type VoteResponse = Future<Item=Vote, Error=String>;

pub trait Link<Record> {
    fn append_entries(&self,id: &String, request: AppendEntries<Record>) -> Box<AppendResponse>;

    fn request_vote (&self, id: &String, request: RequestVote) -> Box<VoteResponse>;
}

#[derive(PartialEq)]
pub enum Role { Follower, Candidate, Leader }

pub struct Config {
    election_restart_ticks: usize
}

static DEFAULT_CONFIG: Config = Config {
    election_restart_ticks: 10
};

pub struct Raft<'a, Record: 'a> {
    config: &'a Config,
    cluster: Cluster<'a>,
    volatile_state: VolatileState<'a>,
    log: Box<Log<Record> + 'a>,
    link: Box<Link<Record> + 'a>,
    role: Role
}

impl<'a, Record: Debug + 'a> Raft<'a, Record> {
    pub fn new (cluster: Cluster<'a>, config: &'a Config, log: Box<Log<Record> + 'a>, link: Box<Link<Record> + 'a>) -> Self {
        let volatile = VolatileState {
            candidate: candidate::State::new(),
            commit_index: 0,
            follower: follower::State::new(),
            leader: leader::State::new()
        };

        Raft {
            config: config,
            cluster: cluster,
            link: link,
            log: log,
            role: Role::Follower,
            volatile_state: volatile
        }
    }

    pub fn check_term(&mut self, message_term: u64) -> u64 {
        let term = self.log.get_current_term();
        // # Rules for Servers / All Servers
        // If RPC request or response contains term T > currentTerm:
        // set currentTerm = T, convert to follower (§5.1)
        let new_leader = message_term > term;
        // # Rules for Servers / Candidates:
        // If AppendEntries RPC received from new leader: convert to follower
        let candidate = self.role == Role::Candidate;

        let election_lost = candidate && message_term == term;
        if new_leader || election_lost {
            if new_leader {
                trace!("following new leader with term {}", message_term);
            } else {
                trace!("lost election for term {}", message_term);
            }

            self.log.set_current_term(message_term);
            follower::become_follower(self);
            message_term
        } else {
            term
        }
    }

    pub fn append_entries (&mut self, request: AppendEntries<Record>) -> Append {
        let current_term = self.check_term(request.term);
        let count = request.entries.len() as u64;
        debug!(
            "RX AppendEntries: {} at index {:?}",
            count,
            request.previous_entry,
        );

        let success = match self.role {
            Role::Follower => follower::append_entries(self, request),
            _ => false
        };
        let response = Append { term: current_term, success: success };

        debug!("TX: {:?}", response);
        response
    }

    pub fn request_vote (&mut self, request: RequestVote) -> Vote {
        let current_term = self.check_term(request.term);

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

            let log_current = self.get_last_log_entry().map(|last| {
                trace!("last log entry: {:?}", last);
                if request.last_log.term == last.term {
                    request.last_log.index >= last.index
                } else { request.last_log.term > last.term }
            }).unwrap_or(true);

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

    fn get_last_log_entry<'b> (&'b mut self) -> Option<LogEntry> {
        let index = self.log.get_index();

        if index > 0 {
            self.log.get_entry(index - 1).map(|(term, _)| {
                LogEntry { index: index, term: term }
            })
        } else {
            None
        }
    }

    fn tick (&mut self) {
        match self.role {
            Role::Follower => follower::tick(self),
            Role::Candidate => candidate::tick(self),
            Role::Leader => leader::tick(self)
        }
    }
}


struct NullLink {}

impl NullLink {
    pub fn new () -> Self {
        NullLink { }
    }
}

impl<Record> Link<Record> for NullLink {
    fn append_entries(&self, _id: &String, _request: AppendEntries<Record>) -> Box<AppendResponse> {
        Box::new(future::ok(Append { term: 0, success: false }))
    }

    fn request_vote (&self, _id: &String, _request: RequestVote) -> Box<VoteResponse> {
        Box::new(future::ok(Vote { term: 0, vote_granted: false }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::log::MemoryLog;
    use std::collections::HashMap;
    use std::cell::RefCell;
    use std::rc::Rc;
    use tokio::prelude::*;

    extern crate env_logger;

    #[derive(Clone)]
    struct Call<Request, Response> {
        request: Request,
        response: Rc<RefCell<Option<Result<Response, String>>>>
    }

    impl<Request, Response> Call<Request, Response> {
        fn new(r: Request) -> Self {
            Call {
                request: r,
                response: Rc::new(RefCell::new(None))
            }
        }

        fn resolve(&self, r: Result<Response, String>) {
            *self.response.borrow_mut() = Some(r);
        }
    }

    impl<X, T: Clone> Future for Call<X, T>
    {
        type Item = T;
        type Error = String;

        fn poll (&mut self) -> Result<Async<T>, String> {
            match self.response.borrow().as_ref() {
                Some(Ok(r)) => {
                    let o: T = r.clone();
                    Ok(Async::Ready(o))
                },
                Some(Err(e)) => Err(e.clone()),
                None => Ok(Async::NotReady)
            }
        }
    }

    type Incoming<Request, Response> = Rc<RefCell<HashMap<
        String, Call<Request, Response>
    >>>;

    #[derive(Clone)]
    struct SwitchLink {
        id: String,
        append: Incoming<AppendEntries<u64>, Append>,
        vote: Incoming<RequestVote, Vote>,
    }

    impl SwitchLink {
        fn new (id: &String) -> Self {
            SwitchLink {
                id: id.clone(),
                append: Rc::new(RefCell::new(HashMap::new())),
                vote: Rc::new(RefCell::new(HashMap::new()))
            }
        }
    }

    impl Link<u64> for SwitchLink {
        fn append_entries(&self, id: &String, r: AppendEntries<u64>) -> Box<AppendResponse> {
            trace!("{} => {:?} => {}", self.id, r, id);
            let ref mut calls = self.append.borrow_mut();
            assert!(!calls.contains_key(id));
            let call = Call::new(r);
            calls.insert(id.clone(), call.clone());
            Box::new(call)
        }

        fn request_vote (&self, id: &String, r: RequestVote) -> Box<VoteResponse> {
            trace!("{} => {:?} => {}", self.id, r, id);
            let ref mut calls = self.vote.borrow_mut();
            assert!(!calls.contains_key(id));
            let call = Call::new(r);
            calls.insert(id.clone(), call.clone());
            Box::new(call)
        }
    }

    struct Node<'a> {
        raft: Raft<'a, u64>,
        link: SwitchLink
    }

    struct Switchboard<'a> {
        nodes: HashMap<String, RefCell<Node<'a>>>
    }

    fn others<'a> (id: &'a String, ids: &Vec<&'a String>) -> Vec<&'a String> {
        ids.iter().filter(|peer_id| **peer_id != id).map(|i| i.clone()).collect()
    }

    impl<'a> Switchboard<'a> {
        fn new (ids: Vec<&'a String>) -> Self {
            let nodes: HashMap<String, RefCell<Node<'a>>> = ids.iter().map(|id| {
                let cluster: Cluster<'a> = Cluster {
                    id: id,
                    peers: others(id, &ids)
                };
                let log = MemoryLog::new();
                let link = SwitchLink::new(&id);
                let raft = Raft::new(
                    cluster,
                    &DEFAULT_CONFIG,
                    Box::new(log),
                    Box::new(link.clone())
                );

                let n: Node<'a> = Node {
                    raft: raft,
                    link: link,
                };

                (
                    id.to_string(),
                    RefCell::new(n)
                )
            }).collect();

            Switchboard {
                nodes: nodes
            }
        }

        fn tick (&self) {
            debug!("tick tock");
            for node in self.nodes.values() {
                let ref mut n = node.borrow_mut();
                let ref mut raft: Raft<'a, _> = n.raft;
                raft.tick();
            }

            assert!(self.leaders().len() <= 1);
        }

        fn process_messages (&self) {
            for node in self.nodes.values() {
                {
                    let mut inner = node.borrow_mut();
                    let append = inner.link.append.borrow();
                    for (id, call) in append.iter() {
                        trace!("resolving append {} => {}", inner.link.id, id);
                        let request = call.request.clone();

                        let mut other = self.nodes.get(id).unwrap().borrow_mut();

                        call.resolve(Ok(other.raft.append_entries(request)));
                    }
                }

                {
                    let mut inner = node.borrow_mut();
                    let votes = inner.link.vote.borrow();
                    for (id, call) in votes.iter() {
                        trace!("resolving vote {} => {}", inner.link.id, id);
                        let request = call.request.clone();

                        let mut other = self.nodes.get(id).unwrap().borrow_mut();

                        call.resolve(Ok(other.raft.request_vote(request)));
                    }
                }

                {
                    let mut inner = node.borrow();
                    *inner.link.vote.borrow_mut() = HashMap::new();
                    *inner.link.append.borrow_mut() = HashMap::new();
                }
            }
        }

        fn leaders (&self) -> Vec<&'a String> {
            self.nodes.values().flat_map(|n| {
                let ref raft = n.borrow().raft;
                if raft.role == Role::Leader {
                    let x: &'a String = raft.cluster.id;
                    Some(x)
                } else {
                    None
                }
            }).collect()
        }

        fn leader (&self) -> Option<&'a String> {
            self.leaders().get(0).map(|id| *id)
        }
    }


    fn single_node_cluster<'a> (id: &'a String) -> Cluster<'a> {
        Cluster {
            id: &id,
            peers: vec![&id]
        }
    }

    #[test]
    fn leader_elected () {
        let _ = env_logger::try_init();
        let a: String = "a".to_owned();
        let b: String = "b".to_owned();
        let c: String = "c".to_owned();
        let ids: Vec<&String> = vec![&a, &b, &c];

        {
            let switch = Switchboard::new(ids);
            {
                let node = switch.nodes.get(&a).unwrap();
                let ref mut raft = node.borrow_mut().raft;
                raft.tick();
                raft.tick();
                raft.tick();
            }

            for _ in 0..100 {
                switch.tick();
                switch.process_messages();
            }

            assert!(switch.leader() != None);
        }
    }

    #[test]
    fn vote_granted () {
        let _ = env_logger::try_init();
        let log: MemoryLog<u64> = MemoryLog::new();
        let link = NullLink::new();
        {
            let id = "me".to_owned();
            let cluster = single_node_cluster(&id);
            let mut raft: Raft<u64> = Raft::new(cluster, &DEFAULT_CONFIG, Box::new(log.clone()), Box::new(link));
            let response = raft.request_vote(RequestVote {
                term: 0,
                candidate_id: "george michael".to_string(),
                last_log: LogEntry { term: 0, index: 5 }
            });
            assert_eq!(response.vote_granted, true);
        }
        assert_eq!(log.get_voted_for(), Some("george michael".to_string()));
    }
}
