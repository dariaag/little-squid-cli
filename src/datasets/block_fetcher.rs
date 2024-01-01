use crate::datasets::traits::DataFetcher;
use crate::export::fields;
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

pub struct BlockFetcher;

#[async_trait::async_trait]
impl DataFetcher for BlockFetcher {
    fn new() -> Self {
        BlockFetcher
    }
    /*   async fn fetch_data(
           start_block: u64,
           fields: &Vec<String>,
           client: Client,
           options: Option<Value>,
       ) -> Result<(Vec<Value>, u64), reqwest::Error> {
           let block_query = Self::create_json_object(start_block, fields, None);
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
    */
    fn create_json_object(
        &self,
        start_block: u64,
        fields: &Vec<String>,
        _options: Option<Value>,
    ) -> Value {
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
}
