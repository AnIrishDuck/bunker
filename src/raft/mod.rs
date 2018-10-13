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

    fn get_voted_for (&self) -> &Option<String>;
    fn set_voted_for (&mut self, candidate: Option<String>);

    fn get_index (&self) -> u64;
    fn get_entry (&self, index: u64) -> &(u64, Box<Record>);
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

pub struct Cluster {
    id: String,
    peers: Vec<String>
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
    cluster: Cluster,
    volatile_state: VolatileState<'a>,
    log: &'a mut Log<Record>,
    link: &'a Link<Record>,
    role: Role
}

impl<'a, Record: Debug + 'a> Raft<'a, Record> {
    pub fn new (cluster: Cluster, config: &'a Config, log: &'a mut Log<Record>, link: &'a Link<Record>) -> Self {
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
            self.role = Role::Follower;
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

    fn get_last_log_entry<'b> (&'b mut self) -> LogEntry {
        let index = self.log.get_index();
        let term = if index == 0 { 0 } else {
            let (last, _record) = self.log.get_entry(index - 1);
            *last
        };
        LogEntry { index: index, term: term }
    }

    fn tick (&'a mut self) {
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
    use std::rc::{Rc, Weak};
    use tokio::prelude::*;

    extern crate env_logger;

    #[derive(Clone)]
    struct Call<Request, Response> {
        request: RefCell<Option<Request>>,
        response: RefCell<Option<Result<Response, String>>>
    }

    impl<Request, Response> Call<Request, Response> {
        fn new() -> Self {
            Call {
                request: RefCell::new(None),
                response: RefCell::new(None)
            }
        }

        fn request(&self, r: Request) {
            assert!(self.request.replace(Some(r)).is_none());
        }

        fn resolve(&self, r: Result<Response, String>) {
            *self.response.borrow_mut() = Some(r);
        }

        fn reset(&self) {
            *self.request.borrow_mut() = None;
            *self.response.borrow_mut() = None;
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

    type Incoming<Request, Response> = HashMap<
        String, RefCell<Call<Request, Response>>
    >;

    struct SwitchLink<'a, Record: Debug + Clone> {
        id: String,
        append: Incoming<AppendEntries<Record>, Append>,
        vote: Incoming<RequestVote, Vote>,
        switch: RefCell<Weak<Switchboard<'a, Record>>>
    }

    fn blank<Req, Res> (peers: &Vec<String>) -> Incoming<Req, Res> {
        peers.iter().map(|i| {
            (i.to_string(), RefCell::new(Call::new()))
        }).collect()
    }

    impl<'a, Record: Debug + Clone> SwitchLink<'a, Record> {
        fn new (id: &String, peers: &Vec<String>) -> Self {
            SwitchLink {
                id: id.clone(),
                append: blank(&peers),
                vote: blank(&peers),
                switch: RefCell::new(Weak::new())
            }
        }

        fn node(&self, id: &String) -> Rc<Node<'a, Record>> {
            let parent = self.switch.borrow().upgrade().unwrap();
            (*parent.nodes.get(id).unwrap()).clone()
        }
    }

    impl<'a, Record: Debug + Clone + 'static> Link<Record> for SwitchLink<'a, Record> {
        fn append_entries(&self, id: &String, r: AppendEntries<Record>) -> Box<AppendResponse> {
            let peer = &self.node(id).link;
            let call = peer.append.get(&self.id).unwrap().borrow();
            call.request(r);
            Box::new(call.clone())
        }

        fn request_vote (&self, id: &String, r: RequestVote) -> Box<VoteResponse> {
            let peer = &self.node(id).link;
            let call = peer.vote.get(&self.id).unwrap().borrow();
            call.request(r);
            Box::new(call.clone())
        }
    }

    struct Node<'a, Record: Debug + Clone> {
        raft: Raft<'a, Record>,
        link: &'a SwitchLink<'a, Record>,
    }

    struct Switchboard<'a, Record: Debug + Clone> {
        nodes: HashMap<String, Rc<Node<'a, Record>>>,
        ids: Vec<String>
    }

    impl<'a, Record: Debug + Clone + 'static> Switchboard<'a, Record> {
        fn new (ids: Vec<String>, data: Vec<(String, &'a mut MemoryLog<Record>, &'a mut SwitchLink<'a, Record>)>) -> Rc<Self> {
            let nodes = data.into_iter().map(|(id, log, link)| {
                let cluster = Cluster {
                    id: id.to_string(),
                    peers: ids.clone()
                };
                let raft = Raft::new(cluster, &DEFAULT_CONFIG, log, link);
                (
                    id.to_string(),
                    Rc::new(Node {
                        raft: raft,
                        link: link,
                    })
                )
            }).collect();

            let switch = Rc::new(Switchboard { nodes: nodes, ids: ids.clone() });

            for ref node in switch.nodes.values() {
                *node.link.switch.borrow_mut() = Rc::downgrade(&switch);
            }

            switch
        }

        fn tick (&'a mut self) {
            for node in self.nodes.values_mut() {
                Rc::get_mut(node).unwrap().raft.tick();
            }
        }

        fn process_messages (&'a mut self) {
            for node in self.nodes.values_mut() {
                let n = Rc::get_mut(node).unwrap();
                let r: &'a mut Raft<'a, Record> = &mut n.raft;
                for cell in n.link.append.values() {
                    let mut call = cell.borrow_mut();
                    let request = call.request.borrow().clone().unwrap();
                    call.resolve(Ok(r.append_entries(request)));
                    *call = Call::new();
                }

                for cell in n.link.vote.values() {
                    let mut call = cell.borrow_mut();
                    let request = call.request.borrow().clone().unwrap();
                    call.resolve(Ok(r.request_vote(request)));
                    *call = Call::new();
                }
            }
        }
    }

    fn cluster () -> Cluster {
        Cluster {
            id: "me".to_string(),
            peers: vec!["other".to_string()]
        }
    }

    #[test]
    fn leader_elected () {
        let ids: Vec<String> = vec!["a", "b", "c"].iter().map(|v| {
            v.to_string()
        }).collect();

        let mut data: Vec<(String, MemoryLog<u64>, SwitchLink<u64>)> = ids.iter().map(|id| {
            let log = MemoryLog::new();
            let link = SwitchLink::new(&id, &ids);
            (id.clone(), log, link)
        }).collect();

        {
            let x: &mut Vec<_> = &mut data;
            let refs: Vec<_> = x.iter_mut().map(|(ref id, ref mut log, ref mut link)| {
                (id.clone(), log, link)
            }).collect();

            let switch = Switchboard::new(ids, refs);
            drop(x);
            drop(switch);
        }

        // drop(switch);
        drop(data);
    }

    #[test]
    fn vote_granted () {
        let _ = env_logger::try_init();
        let mut log: MemoryLog<u64> = MemoryLog::new();
        let link = NullLink::new();
        {
            let mut raft: Raft<u64> = Raft::new(cluster(), &DEFAULT_CONFIG, &mut log, &link);
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
