use log::info;
use parquet::data_type::ByteArray;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;
use warp::{http::StatusCode, reply, Filter};

mod segment;
mod slog;
mod topic;
mod topic_state;

use topic::{Config, Record, Topic};

#[derive(Deserialize)]
struct Insert {
    records: Vec<String>,
    partition: Option<String>,
}

#[derive(Serialize)]
struct Inserted {
    partition: Option<String>,
    final_index: Option<u128>,
    time: SystemTime,
}

#[tokio::main]
async fn main() {
    let root = Arc::new(PathBuf::from("topics")).clone();
    let topics: HashMap<String, Topic> = HashMap::new();
    let topics = Arc::new(RwLock::new(topics));
    let topics = topics.clone();
    // POST /topic/<name> => appends on topic
    let route = warp::path!("topic" / String)
        .and(warp::body::content_length_limit(1024 * 32))
        .and(warp::body::json())
        .map(move |topic_name: String, request: Insert| {
            let time = SystemTime::now();
            let rs: Vec<_> = request
                .records
                .into_iter()
                .map(|m| Record {
                    time,
                    message: ByteArray::from(m.as_str()),
                })
                .collect();
            let default = String::from("");
            let partition = request.partition.unwrap_or(default);
            let read = topics.read().expect("topic map read");
            let final_index = if let Some(topic) = read.get(&topic_name) {
                topic.append(&partition, &rs)
            } else {
                drop(read);
                let mut write = topics.write().expect("topic map write");
                let topic = Topic::attach((*root).clone(), topic_name.clone(), Config::default());
                let index = topic.append(&partition, &rs);
                write.insert(topic_name, topic);
                index
            };

            let result = Inserted {
                partition: Some(partition),
                final_index,
                time,
            };

            reply::with_status(reply::json(&result), StatusCode::OK)
        });

    warp::serve(route).run(([127, 0, 0, 1], 3030)).await;
}
