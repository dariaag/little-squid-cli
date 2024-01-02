use crossbeam::channel::Sender;
use reqwest::{self, Client};
use serde::Deserialize;
use serde_json::{json, to_string, Map, Value};
use std::{
    collections::HashMap,
    io::{self, ErrorKind, Result as IoResult},
    time::Instant,
};
use tokio;
/// Chunk of blocks
use utils::archive::{get_height, get_worker};
use utils::utils::normalize_progess;

use crate::{cli::config::Dataset, export::fields};
#[derive(Debug, Clone)]

struct Range(u64, u64);
const MAX_CHUNK_SIZE: usize = 10 * 1024 * 1024; // 10 MB in bytes

fn create_query_json(
    dataset: Dataset,
    start_block: u64,
    fields: &[String],
    options: &HashMap<String, Vec<String>>,
) -> Value {
    let field_map = fields
        .iter()
        .map(|field| (field.clone(), json!(true)))
        .collect::<Map<_, _>>();

    match dataset {
        Dataset::Blocks => json!({
            "fields": {"block": field_map},
            "fromBlock": start_block,
            "includeAllBlocks": true,
        }),

        Dataset::Transactions => {
            let options_json = json!(options);
            json!({
                "transactions": [options_json],
                "fields": {
                    "block": {},
                    "transaction": field_map
                },
                "fromBlock": start_block,
                "includeAllBlocks": true,
            })
        }

        Dataset::Logs => {
            // Return a Result::Err or use todo!() if not yet implemented
            todo!("Logs dataset handling not implemented")
        }
    }
}

pub async fn fetch_block_chunk(
    dataset: Dataset,
    start_block: u64,
    fields: &Vec<String>,
    options: &HashMap<String, Vec<String>>,
    client: Client,
) -> Result<(Vec<Value>, u64), reqwest::Error> {
    let block_query = create_query_json(dataset, start_block, fields, &options);
    println!("BLOCK QUERY: {:?}", block_query);
    let worker = get_worker(
        "https://v2.archive.subsquid.io/network/ethereum-mainnet",
        &start_block.to_string(),
    )
    .await?;
    //println!("WORKER: {:?}", worker);
    let result = client
        .post(worker)
        .json::<serde_json::Value>(&block_query)
        .send()
        .await?
        .text()
        .await?;

    let blocks_value: Value = serde_json::from_str::<Value>(&result).unwrap();
    //println!("BLOCKS VALUE: {:?}", result);

    let blocks = blocks_value.as_array().unwrap();

    let next_block = blocks
        .last()
        .and_then(|b| b["header"]["number"].as_u64())
        .unwrap()
        + 1;
    // println!("NEXT BLOCK: {:?}", next_block);
    Ok((blocks.to_vec(), next_block))
}

pub async fn block_loop(
    dataset: Dataset,
    mut start_block: u64,
    end_block: u64,
    fields: Vec<String>,
    options: HashMap<String, Vec<String>>,
    write_tx: Sender<Vec<Value>>,
    stats_tx: Sender<u64>,
) -> IoResult<()> {
    loop {
        let client: Client = reqwest::Client::new();
        let (block_chunk, next_block) =
            fetch_block_chunk(dataset, start_block, &fields, &options, client)
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
        //break or continues
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

// #[tokio::test]

// async fn test_fetch_block_chunk() {
//     //let archive_url = "https://v2.archive.subsquid.io/network/ethereum-mainnet";
//     let start_time = Instant::now();
//     let fields = vec![
//         "hash".to_owned(),
//         "number".to_owned(),
//         "parentHash".to_owned(),
//     ];
//     let client: Client = reqwest::Client::new();
//     let (block_chunk, next_block) = fetch_block_chunk(2000000, &fields, client).await.unwrap();
//     print!("START BLOCK: {:?}\n ", 2000000);
//     println!("LENGTH OF BLOCK CHUNK: {:?} ", block_chunk.len());
//     print!("LAST BLOCK: {:?}\n ", next_block);
//     let client: Client = reqwest::Client::new();
//     let fields1 = vec![
//         "hash".to_owned(),
//         "number".to_owned(),
//         "parentHash".to_owned(),
//     ];
//     let (block_chunk, next_block) = fetch_block_chunk(14000000, &fields1, client).await.unwrap();
//     print!("START BLOCK: {:?}\n ", 14000000);
//     println!("LENGTH OF BLOCK CHUNK: {:?} ", block_chunk.len());
//     print!("LAST BLOCK: {:?}\n ", next_block);
//     let fields2 = vec![
//         "hash".to_owned(),
//         "number".to_owned(),
//         "parentHash".to_owned(),
//     ];
//     let client: Client = reqwest::Client::new();
//     let (block_chunk, next_block) = fetch_block_chunk(18000000, &fields2, client).await.unwrap();
//     print!("START BLOCK: {:?}\n ", 18000000);
//     println!("LENGTH OF BLOCK CHUNK: {:?} ", block_chunk.len());
//     print!("LAST BLOCK: {:?}\n ", next_block);
//     let elapsed_time = start_time.elapsed();
//     println!("ELAPSED TIME: {:?}\n", elapsed_time);
// }
