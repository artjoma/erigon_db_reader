mod engine;
mod storage;
mod types;
mod util;

use std::path::PathBuf;

use crate::engine::Engine;
use crate::storage::ResultStorage;
use crate::types::AppCfg;
use crate::util::setup_log;
use clap::Parser;
use log::info;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    setup_log();
    info!("Start app");
    let app_cfg = AppCfg::parse();
    let result_storage = Arc::new(ResultStorage::new(PathBuf::from(app_cfg.result_path)));
    let engine = Arc::new(Engine::new(
        PathBuf::from(app_cfg.db_path),
        result_storage.clone(),
    ));
}
