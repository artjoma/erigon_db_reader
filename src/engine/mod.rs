use crate::storage::ResultStorage;
use crate::types::{KvLog, LogChunk};
use anyhow::Result;
use byteorder::{BigEndian, ByteOrder};
use ethers::abi::Address;
use futures::join;
use libmdbx::{Environment, EnvironmentFlags, Mode, NoWriteMap};
use log::{error, info};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::time::Instant;

pub struct SearchEngine {
    db_path: PathBuf,
    result_storage: Arc<ResultStorage>,
}

pub const LOG_CHUNK_SIZE: usize = 10_000;

impl SearchEngine {
    pub fn new(db_path: PathBuf, result_storage: Arc<ResultStorage>) -> Self {
        info!("Db path:{}", db_path.to_str().unwrap());

        SearchEngine {
            db_path,
            result_storage,
        }
    }

    pub async fn execute_job(
        &self,
        job_id: String,
        from_block: u64,
        to_block: u64,
        contract_address: Option<Address>,
    ) -> Result<()> {
        let _storage = self.result_storage.clone();
        let _db_path = self.db_path.clone();

        let flags = EnvironmentFlags {
            mode: Mode::ReadOnly,
            exclusive: false,
            accede: true,
            no_rdahead: true,
            no_meminit: true,
            ..Default::default()
        };
        let db_env: Environment<NoWriteMap> = Environment::new()
            .set_flags(flags)
            .set_max_dbs(4)
            .open(_db_path.as_path())
            .unwrap();

        let open_result = db_env.begin_ro_txn();
        if open_result.is_err() {
            panic!("Invalid db {}", open_result.err().unwrap().to_string());
        }

        let txn = open_result.unwrap();
        let db = txn.open_db(Some("TransactionLog")).unwrap();
        let cursor = txn.cursor(&db).unwrap();
        let mut key = [0; 8];
        let mut log_count: u64 = 0;
        // set from-block number
        BigEndian::write_u64(&mut key, from_block);
        info!(
            "[{}] Start job {}-{} {:?}",
            job_id, from_block, to_block, contract_address
        );

        let (a_writer, worker) = _storage.open_writer(job_id.clone()).await;
        let now = Instant::now();
        let mut chunk = Vec::with_capacity(LOG_CHUNK_SIZE);
        for item in cursor.into_iter_from::<Vec<u8>, Vec<u8>>(&key) {
            //if item.is_err() {
            //    panic!("Critical err:{}", item.err().unwrap().to_string());
            //}
            let item = item.unwrap();
            let val = item.1;
            if val.len() > 0 {
                let block_n = BigEndian::read_u64(&item.0[0..8]);
                if block_n > to_block {
                    break;
                }
                let tx_index = BigEndian::read_u32(&item.0[8..]);
                let mut logs: Vec<KvLog> = serde_cbor::from_slice(val.as_slice()).unwrap();
                // filter by contract address
                if contract_address.is_some() {
                    let filter_addr = *contract_address.as_ref().unwrap();
                    logs.retain(|l| l.address == filter_addr)
                }
                if logs.is_empty() {
                    continue;
                }
                for log in logs {
                    chunk.push((block_n, tx_index, log));
                    log_count += 1;
                }
                if chunk.len() > LOG_CHUNK_SIZE {
                    info!("[{}] block:{} logs count:{}", job_id, block_n, log_count);
                    // send chunk to async file writer
                    a_writer.send(Some(LogChunk { txs: chunk })).await.unwrap();
                    // allocate new chunk
                    chunk = Vec::with_capacity(LOG_CHUNK_SIZE);
                }
            }
        }
        if !chunk.is_empty() {
            // send buffered records
            a_writer.send(Some(LogChunk { txs: chunk })).await.unwrap();
        }

        // terminate async writer
        if let Err(err) = a_writer.send(None).await {
            error!("Send message err:{}", err.to_string())
        }
        // wait writer
        let _ = join!(worker);

        info!(
            "[{}] End job. Took:{}mils. Logs count:{}",
            job_id,
            now.elapsed().as_millis(),
            log_count
        );

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::engine::SearchEngine;
    use crate::storage::ResultStorage;
    use crate::util::setup_log;
    use std::path::PathBuf;
    use std::sync::Arc;

    #[tokio::test]
    async fn execute_job() {
        setup_log();
        let result_path = "/tmp";
        let db_path = "/home/art/dev/sepolia-chaindata/";
        let result_storage = Arc::new(ResultStorage::new(PathBuf::from(result_path)));
        let engine = Arc::new(SearchEngine::new(PathBuf::from(db_path), result_storage.clone()));

        engine
            .execute_job("test-1".to_string(), 10_000, 3_000_000, None)
            .await
            .unwrap();
    }
}
