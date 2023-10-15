use crate::engine::LOG_CHUNK_SIZE;
use crate::types::LogChunk;
use bytes::Bytes;
use ethers::types::H256;
use log::info;
use parquet::basic::{Compression, Encoding};
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{WriterProperties, WriterVersion};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::spawn;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;

/*
Tables arch: Erigon accessors_chain.go

libmdbx LCK file should be same version for reader app and writer app!
erigon libmdbx 0.27.0:

mdbx_chk version 0.12.0.71
 - source: v0.12.0-71-g1cac6536 2022-07-28T09:57:31+07:00, commit 1cac65363763e7523ed3b52eed8f2c617cead973, tree 9a6d7e5b917e5fbd14dc51835fa749d092aa1d72
 - anchor: 99d371079af9d05b2f0e8751b4ee8fd75986ecba45c5a7f9807d154a86cff0eb_v0_12_0_71_g1cac6536
 - build: 2022-12-21T21:14:13+0200 for x86_64-linux-gnu by cc (Ubuntu 11.3.0-1ubuntu1~22.04) 11.3.0
 */
pub struct ResultStorage {
    result_path: PathBuf,
}

const LOGS_MESSAGE_TYPE: &str = "
              message schema {
                REQUIRED INT64 block_n;
                REQUIRED INT32 tx_n;
                REQUIRED BYTE_ARRAY contract;
                REQUIRED INT32 op_code;
                OPTIONAL BYTE_ARRAY topic0;
                OPTIONAL BYTE_ARRAY topic1;
                OPTIONAL BYTE_ARRAY topic2;
                OPTIONAL BYTE_ARRAY topic3;
                OPTIONAL BYTE_ARRAY data;
                }
            ";

impl ResultStorage {
    pub fn new(result_path: PathBuf) -> Self {
        info!("Result storage:{}", result_path.to_str().unwrap());
        ResultStorage { result_path }
    }
    // Return mq sender and corutine
    pub async fn open_writer(&self, job_id: String) -> (Sender<Option<LogChunk>>, JoinHandle<()>) {
        let (sender, mut rcv) = mpsc::channel::<Option<LogChunk>>(8);
        let result_file = self.result_path.join(Path::new(&job_id));
        let _job_id = job_id.clone();
        info!(
            "[{}] Start result writer. Result file:{}",
            job_id,
            result_file.to_str().unwrap()
        );
        let worker: JoinHandle<()> = spawn(async move {
            let schema = Arc::new(parse_message_type(LOGS_MESSAGE_TYPE).unwrap());
            let file = fs::File::create(&result_file).unwrap();
            let props = WriterProperties::builder()
                .set_writer_version(WriterVersion::PARQUET_2_0)
                .set_encoding(Encoding::PLAIN)
                .set_compression(Compression::SNAPPY)
                .build();
            let mut writer = SerializedFileWriter::new(file, schema, props.into()).unwrap();

            while let Some(chunk) = rcv.recv().await {
                if chunk.is_none() {
                    break;
                }
                let chunk = chunk.unwrap();
                info!("[{}] Chunk size: {:?}", _job_id, chunk.txs.len());
                let mut block_n_col = Vec::with_capacity(LOG_CHUNK_SIZE);
                let mut tx_n_col = Vec::with_capacity(LOG_CHUNK_SIZE);
                let mut ctr_col = Vec::with_capacity(LOG_CHUNK_SIZE);
                let mut op_code_col = Vec::with_capacity(LOG_CHUNK_SIZE);
                let mut topic_0_col = Vec::with_capacity(LOG_CHUNK_SIZE);
                let mut topic_0_def_level_col = Vec::with_capacity(LOG_CHUNK_SIZE);
                let mut topic_1_col = Vec::with_capacity(LOG_CHUNK_SIZE);
                let mut topic_1_def_level_col = Vec::with_capacity(LOG_CHUNK_SIZE);
                let mut topic_2_col = Vec::with_capacity(LOG_CHUNK_SIZE);
                let mut topic_2_def_level_col = Vec::with_capacity(LOG_CHUNK_SIZE);
                let mut topic_3_col = Vec::with_capacity(LOG_CHUNK_SIZE);
                let mut topic_3_def_level_col = Vec::with_capacity(LOG_CHUNK_SIZE);
                let mut data_col = Vec::with_capacity(LOG_CHUNK_SIZE);
                let mut data_def_level_col = Vec::with_capacity(LOG_CHUNK_SIZE);

                for _log_model in chunk.txs {
                    let (block_n, tx_n, logs) = _log_model;
                    block_n_col.push(block_n as i64);
                    tx_n_col.push(tx_n as i32);
                    ctr_col.push(ByteArray::from(logs.address.as_bytes()));
                    op_code_col.push(logs.topics.len() as i32);

                    Self::save_topic(
                        &mut topic_0_col,
                        &mut topic_0_def_level_col,
                        logs.topics.get(0),
                    );
                    Self::save_topic(
                        &mut topic_1_col,
                        &mut topic_1_def_level_col,
                        logs.topics.get(1),
                    );
                    Self::save_topic(
                        &mut topic_2_col,
                        &mut topic_2_def_level_col,
                        logs.topics.get(2),
                    );
                    Self::save_topic(
                        &mut topic_3_col,
                        &mut topic_3_def_level_col,
                        logs.topics.get(3),
                    );

                    Self::save_binary(&mut data_col, &mut data_def_level_col, logs.data);
                }

                let mut row_group_writer = writer.next_row_group().unwrap();
                let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
                col_writer
                    .typed::<Int64Type>()
                    .write_batch(&block_n_col, None, None)
                    .unwrap();
                col_writer.close().unwrap();

                let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
                col_writer
                    .typed::<Int32Type>()
                    .write_batch(&tx_n_col, None, None)
                    .unwrap();
                col_writer.close().unwrap();

                let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
                col_writer
                    .typed::<ByteArrayType>()
                    .write_batch(&ctr_col, None, None)
                    .unwrap();
                col_writer.close().unwrap();

                let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
                col_writer
                    .typed::<Int32Type>()
                    .write_batch(&op_code_col, None, None)
                    .unwrap();
                col_writer.close().unwrap();

                let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
                col_writer
                    .typed::<ByteArrayType>()
                    .write_batch(&topic_0_col, Some(&topic_0_def_level_col), None)
                    .unwrap();
                col_writer.close().unwrap();

                let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
                col_writer
                    .typed::<ByteArrayType>()
                    .write_batch(&topic_1_col, Some(&topic_1_def_level_col), None)
                    .unwrap();
                col_writer.close().unwrap();

                let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
                col_writer
                    .typed::<ByteArrayType>()
                    .write_batch(&topic_2_col, Some(&topic_2_def_level_col), None)
                    .unwrap();
                col_writer.close().unwrap();

                let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
                col_writer
                    .typed::<ByteArrayType>()
                    .write_batch(&topic_3_col, Some(&topic_3_def_level_col), None)
                    .unwrap();
                col_writer.close().unwrap();

                let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
                col_writer
                    .typed::<ByteArrayType>()
                    .write_batch(&data_col, Some(&data_def_level_col), None)
                    .unwrap();
                col_writer.close().unwrap();
                row_group_writer.close().unwrap();
            }

            info!("[{}] Stop result writer.", job_id);
            writer.close().unwrap();
        });

        (sender, worker)
    }

    fn save_topic(col: &mut Vec<ByteArray>, def_level_col: &mut Vec<i16>, topic: Option<&H256>) {
        if let Some(data) = topic {
            col.push(ByteArray::from(data.as_bytes()));
            def_level_col.push(1_i16);
        } else {
            col.push(ByteArray::from(vec![]));
            def_level_col.push(0_i16);
        }
    }

    fn save_binary(col: &mut Vec<ByteArray>, def_level_col: &mut Vec<i16>, binary: Option<Bytes>) {
        if let Some(data) = binary {
            if data.is_empty() {
                col.push(ByteArray::from(vec![]));
                def_level_col.push(0_i16);
            } else {
                col.push(ByteArray::from(data));
                def_level_col.push(1_i16);
            }
        } else {
            col.push(ByteArray::from(vec![]));
            def_level_col.push(0_i16);
        }
    }
}
#[cfg(test)]
mod test {
    use ethers::types::{Address, H256};
    use parquet::basic::{Compression, Encoding, ZstdLevel};
    use parquet::data_type;
    use parquet::data_type::{ByteArrayType, Int32Type};
    use parquet::file::properties::{WriterProperties, WriterVersion};
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;
    use std::fs;
    use std::path::Path;
    use std::sync::Arc;

    #[test]
    fn test_parquet() {
        let path = Path::new("/tmp/sample.parquet");

        let message_type = "
          message schema {
            REQUIRED INT32 block_n;
            REQUIRED INT32 tx_n;
            REQUIRED BYTE_ARRAY contract;
            OPTIONAL BYTE_ARRAY topic;
            }
        ";
        //  OPTIONAL BYTE_ARRAY topic;
        let schema = Arc::new(parse_message_type(message_type).unwrap());
        let file = fs::File::create(&path).unwrap();
        let props = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_encoding(Encoding::PLAIN)
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(16).unwrap()))
            .build();
        let mut writer = SerializedFileWriter::new(file, schema, props.into()).unwrap();
        let mut row_group_writer = writer.next_row_group().unwrap();
        let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
        col_writer
            .typed::<Int32Type>()
            .write_batch(&[1, 2, 3], None, None)
            .unwrap();
        col_writer.close().unwrap();

        let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
        col_writer
            .typed::<Int32Type>()
            .write_batch(&[100, 200, 300], None, None)
            .unwrap();
        col_writer.close().unwrap();

        let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
        col_writer
            .typed::<ByteArrayType>()
            .write_batch(
                &[
                    data_type::ByteArray::from(Address::default().as_bytes()),
                    data_type::ByteArray::from(Address::default().as_bytes()),
                    data_type::ByteArray::from(Address::default().as_bytes()),
                ],
                None,
                None,
            )
            .unwrap();
        col_writer.close().unwrap();

        let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
        col_writer
            .typed::<ByteArrayType>()
            .write_batch(
                &[
                    data_type::ByteArray::from(H256::default().as_bytes()),
                    data_type::ByteArray::from(vec![]),
                    data_type::ByteArray::from(H256::default().as_bytes()),
                ],
                Some(&[1, 0, 1]),
                None,
            )
            .unwrap();
        col_writer.close().unwrap();

        row_group_writer.close().unwrap();

        writer.close().unwrap();

        let bytes = fs::read(&path).unwrap();
        assert_eq!(&bytes[0..4], &[b'P', b'A', b'R', b'1']);
    }
}
