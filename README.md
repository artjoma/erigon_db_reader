# Ethereum logs extractor

This application extracting EVM logs from Erigon database(libmdbx) and store it Parquet binary format data file.
Example: ```asset/sepolia_10000_500000_all_logs.parquet```
Parquet table schema:
```
message schema {
    REQUIRED INT64 block_n;
    REQUIRED INT32 tx_n; -- transaction index at block
    REQUIRED BYTE_ARRAY contract; -- contract address
    REQUIRED INT32 op_code; -- EVM opcode LOG0/LOG1/LOG2/LOG3
    OPTIONAL BYTE_ARRAY topic0;
    OPTIONAL BYTE_ARRAY topic1;
    OPTIONAL BYTE_ARRAY topic2;
    OPTIONAL BYTE_ARRAY topic3;
    OPTIONAL BYTE_ARRAY data; -- log data
}
```
![dbeaver.png](asset%2Fdbeaver.png)

Feature:
 * Extract logs by block number range: from - to
 * Filter logs by contract address: optional


TODO: Use http rpc for creating new job
