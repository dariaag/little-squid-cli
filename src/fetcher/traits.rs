use async_trait::async_trait;
use reqwest::{self, Client};
use serde_json::Value;
use utils::archive::get_worker;

/// Chunk of blocks

#[async_trait]
pub trait DataFetcher: Send + Sync {
    fn new() -> Self
    where
        Self: Sized;

    async fn fetch_data(
        &self,
        start_block: u64,
        fields: &Vec<String>,
        client: Client,
        options: Option<Value>,
    ) -> Result<(Vec<Value>, u64), reqwest::Error> {
        let query = Self::create_json_object(self, start_block, fields, options);
        let worker = get_worker(
            "https://v2.archive.subsquid.io/network/ethereum-mainnet",
            &start_block.to_string(),
        )
        .await
        .unwrap();
        //println!("WORKER: {:?}", worker);
        let result = client
            .post(worker)
            .json::<serde_json::Value>(&query)
            .send()
            .await?
            .text()
            .await?;

        let value: Value = serde_json::from_str::<Value>(&result).unwrap();
        let items = value.as_array().unwrap();

        let next_block = items[items.len() - 1]["header"]["number"].as_u64().unwrap() + 1;
        // println!("NEXT BLOCK: {:?}", next_block);
        Ok((items.to_vec(), next_block))
    }

    fn create_json_object(
        &self,
        start_block: u64,
        fields: &Vec<String>,
        options: Option<Value>,
    ) -> Value;
}
