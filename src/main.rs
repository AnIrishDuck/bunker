#[macro_use]
extern crate log;
extern crate env_logger;
extern crate rand;
extern crate tokio;
extern crate futures;

mod raft;
/*
extern crate rocket;
extern crate rocksdb;

use rocket::request::State;
use rocksdb::DB;

#[derive(Deserialize, StateData, StaticResponseExtender)]
struct LogIndexExtractor {
    key: String,
    index: u64
}

fn get_log(db: State<DB>, key: String, index: u64) -> String {
    match db.get(&key.into_bytes()) {
        Ok(Some(value)) =>
            format!("200 {}\n", value.to_utf8().unwrap()),
        Ok(None) => format!("404\n"),
        Err(e) => format!("500: {}\n", e),
    }
}

#[put("/log/<key>/<index>")]
fn put_log(db: State<DB>, key: String, index: u64) -> String {
    let value: String = format!("{}", index);
    db.put(&key.into_bytes(), &value.clone().into_bytes()).expect("db put");
    format!("{}\n", value)
}

static DATA_PATH: &'static str = "borneo.rocksdb";

/// Initializes a database pool.
fn init_db() -> DB {
    DB::open_default(DATA_PATH).expect("opening database")
}
*/

fn main() {
    /*
    rocket::ignite()
        .mount("/", routes![get_log, put_log])
        .manage(init_db())
        .launch();
    */
    println!("hello cruel world!");
}
