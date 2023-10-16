
use rocket::{FromForm};

#[derive(FromForm, Debug)]
pub struct ExecJobRequest {
    pub job_id: String,
    pub block_number_start: u64,
    pub block_number_end: u64,
    pub contract: Option<String>,
}
