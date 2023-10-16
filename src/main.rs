mod engine;
mod storage;
mod types;
mod util;
mod http;

use std::path::PathBuf;

use crate::engine::SearchEngine;
use crate::storage::ResultStorage;
use crate::types::AppCfg;
use crate::util::setup_log;
use clap::Parser;
use log::info;
use std::sync::Arc;
use crate::http::HttpApi;

#[tokio::main]
async fn main() {
    setup_log();
    info!("Start app");
    let app_cfg = AppCfg::parse();
    let result_storage = Arc::new(ResultStorage::new(PathBuf::from(app_cfg.result_path)));
    let search_engine = Arc::new(SearchEngine::new(
        PathBuf::from(app_cfg.db_path),
        result_storage.clone(),
    ));

    HttpApi::new(
        app_cfg.http_port,
        app_cfg.http_address,
        search_engine.clone(),
    )
        .await;
}
