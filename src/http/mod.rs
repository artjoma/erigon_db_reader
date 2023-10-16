mod types;

use crate::engine::{SearchEngine};
use ethers::types::{Address};
use rocket::{get, routes, State};
use std::str::FromStr;
use std::sync::Arc;
use crate::http::types::ExecJobRequest;

pub struct HttpApi {}

impl HttpApi {
    pub async fn new(port: u16, address: String, search_engine: Arc<SearchEngine>) {
        rocket::build()
            .configure(rocket::Config {
                address: address.parse().unwrap(),
                port,
                ..rocket::Config::default()
            })
            .manage(search_engine)
            .mount("/", routes![exec_job])
            .launch()
            .await
            .expect("Err setup");
    }
}

// Example: api/v1/exec-job?job_id=test_1&block_number_start=10000&block_number_end=3000000
#[get("/api/v1/exec-job?<query..>")]
async fn exec_job(search_engine: &State<Arc<SearchEngine>>, query: ExecJobRequest) -> String {
    let _search_engine = search_engine.inner().clone();
    let _job_id = query.job_id.clone();

    tokio::spawn(async move {
        _search_engine.execute_job(
            query.job_id,
            query.block_number_start,
            query.block_number_end,
            query
                .contract
                .map(|c| Address::from_str(c.as_str()).unwrap())
        ).await.unwrap();
    });


    return _job_id;
}
