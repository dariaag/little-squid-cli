use crossbeam::channel::Sender;
use reqwest::{self, Client};
use serde::Deserialize;
use serde_json::{json, to_string, Map, Value};
use std::{
    io::{self, Result as IoResult},
    time::Instant,
};
use tokio;
/// Chunk of blocks
use utils::archive::{get_height, get_worker};

use crate::export::fields;
#[derive(Debug, Clone)]

struct Range(u64, u64);
const MAX_CHUNK_SIZE: usize = 10 * 1024 * 1024; // 10 MB in bytes

fn create_block_query(start_block: u64, fields: Vec<String>) {}

fn create_json_object(start_block: u64, fields: &Vec<String>) -> Value {
    let mut json_map = Map::new();

    for field in fields {
        // Insert default value for each field
        // Here, I'm using an empty string as a default value, but you can modify it as needed
        json_map.insert(field.to_string(), json!(true));
    }

    let block_query = json!({
        "fields": {
            "block":
                Value::Object(json_map),
        },
        "fromBlock": start_block,
        //"toBlock": start_block + 1000000,
        "includeAllBlocks": true,
    });
    block_query
}

//fetch block chunk that worker has
pub async fn fetch_block_chunk(
    start_block: u64,
    fields: &Vec<String>,
    client: Client,
) -> Result<(Vec<Value>, u64), reqwest::Error> {
    let block_query = create_json_object(start_block, fields);
    let worker = get_worker(
        "https://v2.archive.subsquid.io/network/ethereum-mainnet",
        &start_block.to_string(),
    )
    .await
    .unwrap();
    //println!("WORKER: {:?}", worker);
    let result = client
        .post(worker)
        .json::<serde_json::Value>(&block_query)
        .send()
        .await?
        .text()
        .await?;

    let blocks_value: Value = serde_json::from_str::<Value>(&result).unwrap();
    let blocks = blocks_value.as_array().unwrap();

    let next_block = blocks[blocks.len() - 1]["header"]["number"]
        .as_u64()
        .unwrap()
        + 1;
    // println!("NEXT BLOCK: {:?}", next_block);
    Ok((blocks.to_vec(), next_block))
}
fn normalize_progess(start_block: u64, end_block: u64, current_block: u64) -> u64 {
    let total_blocks = end_block - start_block;
    let current_progress = current_block - start_block;
    let normalized_progress = (current_progress * 100) / total_blocks;
    normalized_progress
}
pub async fn block_loop(
    mut start_block: u64,
    end_block: u64,
    fields: Vec<String>,
    write_tx: Sender<Vec<Value>>,
    stats_tx: Sender<u64>,
) -> IoResult<()> {
    loop {
        let client: Client = reqwest::Client::new();
        let (block_chunk, next_block) = fetch_block_chunk(start_block, &fields, client)
            .await
            .unwrap();
        let mut data_chunk = Vec::new();
        let mut current_size = 0;

        for data in block_chunk {
            let serialized: String = to_string(&data).unwrap();
            if current_size + serialized.len() > MAX_CHUNK_SIZE {
                write_tx.send(data_chunk).unwrap();
                data_chunk = Vec::new();
                current_size = 0;
            }
            current_size += serialized.len();
            data_chunk.push(data);
        }
        // Send any remaining data
        if !data_chunk.is_empty() {
            write_tx.send(data_chunk).unwrap();
        }
        let normalized_progress = normalize_progess(start_block, end_block, next_block);
        let _ = stats_tx.send(normalized_progress);
        //break or continue
        match next_block {
            _ if next_block > end_block => {
                break;
            }
            _ => {
                start_block = next_block;
            }
        }
    }
    let _ = write_tx.send(Vec::new());
    let _ = stats_tx.send(0);

    Ok(())
}

#[tokio::test]

async fn test_fetch_block_chunk() {
    //let archive_url = "https://v2.archive.subsquid.io/network/ethereum-mainnet";
    let start_time = Instant::now();
    let fields = vec![
        "hash".to_owned(),
        "number".to_owned(),
        "parentHash".to_owned(),
    ];
    let client: Client = reqwest::Client::new();
    let (block_chunk, next_block) = fetch_block_chunk(2000000, &fields, client).await.unwrap();
    print!("START BLOCK: {:?}\n ", 2000000);
    println!("LENGTH OF BLOCK CHUNK: {:?} ", block_chunk.len());
    print!("LAST BLOCK: {:?}\n ", next_block);
    let client: Client = reqwest::Client::new();
    let fields1 = vec![
        "hash".to_owned(),
        "number".to_owned(),
        "parentHash".to_owned(),
    ];
    let (block_chunk, next_block) = fetch_block_chunk(14000000, &fields1, client).await.unwrap();
    print!("START BLOCK: {:?}\n ", 14000000);
    println!("LENGTH OF BLOCK CHUNK: {:?} ", block_chunk.len());
    print!("LAST BLOCK: {:?}\n ", next_block);
    let fields2 = vec![
        "hash".to_owned(),
        "number".to_owned(),
        "parentHash".to_owned(),
    ];
    let client: Client = reqwest::Client::new();
    let (block_chunk, next_block) = fetch_block_chunk(18000000, &fields2, client).await.unwrap();
    print!("START BLOCK: {:?}\n ", 18000000);
    println!("LENGTH OF BLOCK CHUNK: {:?} ", block_chunk.len());
    print!("LAST BLOCK: {:?}\n ", next_block);
    let elapsed_time = start_time.elapsed();
    println!("ELAPSED TIME: {:?}\n", elapsed_time);
}
