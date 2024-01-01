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

pub struct TransactionFetcher;

#[async_trait::async_trait]
impl DataFetcher for TransactionFetcher {
    fn new() -> Self {
        TransactionFetcher
    }
    fn create_json_object(
        &self,
        start_block: u64,
        fields: &Vec<String>,
        options: Option<Value>,
    ) -> Value {
        let mut json_map = Map::new();

        for field in fields {
            // Insert default value for each field
            // Here, I'm using an empty string as a default value, but you can modify it as needed
            json_map.insert(field.to_string(), json!(true));
        }

        match options {
            Some(options) => {
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
            None => {
                let transactions_query = json!({
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
        }
    }
}
