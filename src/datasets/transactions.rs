use crossbeam::channel::Sender;
use reqwest::{self, Client};
use serde::Deserialize;
use serde_json::{json, to_string, Map, Value};
use std::{
    io::{self, Result as IoResult},
    time::Instant,
};
use tokio;

use utils::archive::{get_height, get_worker};

use crate::{datasets::transactions, export::fields};

fn create_json_object(start_block: u64, fields: &Vec<String>, options: Value) -> Value {
    let mut json_map = Map::new();

    for field in fields {
        // Insert default value for each field
        // Here, I'm using an empty string as a default value, but you can modify it as needed
        json_map.insert(field.to_string(), json!(true));
    }

    let transactions_query = json!({
        "transactions": options,
        "fields": {
            "transaction":
                Value::Object(json_map),
        },

        "fromBlock": start_block,
        //"toBlock": start_block + 1000000,
        "includeAllBlocks": true,
    });
    transactions_query
}

// {
//     // data requests
//     from?: string[]
//     to?: string[]
//     sighash?: string[]
//     range?: {from: number, to?: number}

//     // related data retrieval
//     logs?: boolean
//     stateDiffs?: boolean
//     traces?: boolean
//   }

pub async fn fetch_transactions_chunk(
    start_block: u64,
    fields: &Vec<String>,
    options: Value,
    client: Client,
) -> Result<(Vec<Value>, u64), reqwest::Error> {
    let transactions_query = create_json_object(start_block, fields, options);
    let worker = get_worker(
        "https://v2.archive.subsquid.io/network/ethereum-mainnet",
        &start_block.to_string(),
    )
    .await
    .unwrap();

    let result = client
        .post(worker)
        .json::<serde_json::Value>(&transactions_query)
        .send()
        .await?
        .text()
        .await?;
    let transactions_value: Value = serde_json::from_str::<Value>(&result).unwrap();
    let transactions = transactions_value.as_array().unwrap();

    let next_block = transactions[transactions.len() - 1]["header"]["number"]
        .as_u64()
        .unwrap()
        + 1;
    //println!("NEXT BLOCK: {:?}", next_block);
    Ok((transactions.to_vec(), next_block))
}
