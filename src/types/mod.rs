use bytes::Bytes;
use clap::Parser;
use ethers::types::{Address, H256};
use serde::Deserialize;

pub struct LogChunk {
    // block n, tx_n logs
    pub txs: Vec<(u64, u32, KvLog)>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KvLog {
    // Consensus fields:
    // address of the contract that generated the event
    pub address: Address,
    // list of topics provided by the contract.
    pub topics: Vec<H256>,
    // supplied by the contract, usually ABI-encoded
    pub data: Option<Bytes>,
}

#[derive(Debug, Parser, Deserialize)]
#[clap(author, version, about = "ErigonDbReader", long_about = None)]
#[serde(rename_all = "kebab-case")]
pub struct AppCfg {
    #[clap(long, env)]
    pub http_port: u16,
    #[clap(long, env)]
    pub http_address: String,
    #[clap(long, env)]
    pub db_path: String,
    #[clap(long, env)]
    pub result_path: String,
}
