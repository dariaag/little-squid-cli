use std::collections::HashMap;

use polars::prelude::{NamedFrom, Series};

use crate::cli::config::Dataset;

#[derive(Debug)]
pub enum FieldData {
    BlocksData(BlockFieldData),
    TransactionsData(TransactionsFieldData),
}

#[derive(Debug)]
pub enum BlockFieldData {
    Hash(Vec<String>),
    Number(Vec<u64>),
    ParentHash(Vec<String>),
    Timestamp(Vec<u64>),
    Miner(Vec<String>),
    StateRoot(Vec<String>),
    TransactionsRoot(Vec<String>),
    ReceiptsRoot(Vec<String>),
    GasUsed(Vec<u64>),
    ExtraData(Vec<String>),
    BaseFeePerGas(Vec<u64>),
    LogsBloom(Vec<String>),
    TotalDifficulty(Vec<u64>),
    Size(Vec<u64>),
}
#[derive(Debug)]
pub enum TransactionsFieldData {
    Id(Vec<String>),
    TransactionIndex(Vec<u64>),
    From(Vec<String>),
    To(Vec<String>),
    Hash(Vec<String>),
    Gas(Vec<u64>),
    GasPrice(Vec<u64>),
    MaxFeePerGas(Vec<u64>),
    MaxPriorityFeePerGas(Vec<u64>),
    Input(Vec<String>),
    Nonce(Vec<u64>),
    Value(Vec<String>),
    V(Vec<u64>),
    R(Vec<String>),
    S(Vec<String>),
    YParity(Vec<u64>),
    ChainId(Vec<u64>),
    GasUsed(Vec<u64>),
    CumulativeGasUsed(Vec<u64>),
    EffectiveGasPrice(Vec<u64>),
    ContractAddress(Vec<String>),
    Type(Vec<u64>),
    Status(Vec<u64>),
    Sighash(Vec<String>),
}

impl FieldData {
    pub fn add_value(&mut self, value: &serde_json::Value) {
        match self {
            FieldData::BlocksData(_data) => self.add_blocks_value(value),
            FieldData::TransactionsData(_data) => self.add_transactions_value(value),
            //logs
            //traces
            //_ => panic!("Unsupported type"),
        }
    }

    pub fn add_blocks_value(&mut self, value: &serde_json::Value) {
        match self {
            Self::BlocksData(data) => match data {
                BlockFieldData::Hash(vec) => vec.push(value.as_str().unwrap().to_string()),
                BlockFieldData::Number(vec) => vec.push(value.as_u64().unwrap()),
                BlockFieldData::ParentHash(vec) => vec.push(value.as_str().unwrap().to_string()),
                BlockFieldData::Timestamp(vec) => vec.push(value.as_f64().unwrap() as u64),
                BlockFieldData::Miner(vec) => vec.push(value.as_str().unwrap().to_string()),
                BlockFieldData::StateRoot(vec) => vec.push(value.as_str().unwrap().to_string()),
                BlockFieldData::TransactionsRoot(vec) => {
                    vec.push(value.as_str().unwrap().to_string())
                }
                BlockFieldData::ReceiptsRoot(vec) => vec.push(value.as_str().unwrap().to_string()),
                BlockFieldData::GasUsed(vec) => vec.push(value.as_u64().unwrap()),
                BlockFieldData::ExtraData(vec) => vec.push(value.as_str().unwrap().to_string()),
                BlockFieldData::BaseFeePerGas(vec) => vec.push(value.as_u64().unwrap_or(0)),
                BlockFieldData::LogsBloom(vec) => vec.push(value.as_str().unwrap().to_string()),
                BlockFieldData::TotalDifficulty(vec) => vec.push(value.as_u64().unwrap()),
                BlockFieldData::Size(vec) => vec.push(value.as_u64().unwrap()),
            },
            _ => panic!("Unsupported type"),
        }
    }
    pub fn add_transactions_value(&mut self, value: &serde_json::Value) {
        match self {
            Self::TransactionsData(data) => match data {
                TransactionsFieldData::Id(vec) => vec.push(value.as_str().unwrap().to_string()),
                TransactionsFieldData::TransactionIndex(vec) => vec.push(value.as_u64().unwrap()),
                TransactionsFieldData::From(vec) => vec.push(value.as_str().unwrap().to_string()),
                TransactionsFieldData::To(vec) => {
                    vec.push(value.as_str().unwrap_or("").to_string())
                }
                TransactionsFieldData::Hash(vec) => vec.push(value.as_str().unwrap().to_string()),
                TransactionsFieldData::Gas(vec) => vec.push(value.as_u64().unwrap()),
                TransactionsFieldData::GasPrice(vec) => vec.push(value.as_u64().unwrap()),
                TransactionsFieldData::MaxFeePerGas(vec) => vec.push(value.as_u64().unwrap_or(0)),
                TransactionsFieldData::MaxPriorityFeePerGas(vec) => {
                    vec.push(value.as_u64().unwrap_or(0))
                }
                TransactionsFieldData::Input(vec) => vec.push(value.as_str().unwrap().to_string()),
                TransactionsFieldData::Nonce(vec) => vec.push(value.as_u64().unwrap()),
                TransactionsFieldData::Value(vec) => {
                    vec.push(value.as_str().unwrap_or("").to_string());
                }
                TransactionsFieldData::V(vec) => vec.push(value.as_u64().unwrap_or(0)),
                TransactionsFieldData::R(vec) => vec.push(value.as_str().unwrap_or("").to_string()),
                TransactionsFieldData::S(vec) => vec.push(value.as_str().unwrap_or("").to_string()),
                TransactionsFieldData::YParity(vec) => vec.push(value.as_u64().unwrap_or(0)),
                TransactionsFieldData::ChainId(vec) => vec.push(value.as_u64().unwrap_or(0)),
                TransactionsFieldData::GasUsed(vec) => vec.push(value.as_u64().unwrap_or(0)),
                TransactionsFieldData::CumulativeGasUsed(vec) => {
                    vec.push(value.as_u64().unwrap_or(0))
                }
                TransactionsFieldData::EffectiveGasPrice(vec) => {
                    vec.push(value.as_u64().unwrap_or(0))
                }
                TransactionsFieldData::ContractAddress(vec) => {
                    vec.push(value.as_str().unwrap_or("").to_string())
                }
                TransactionsFieldData::Type(vec) => vec.push(value.as_u64().unwrap_or(0)),
                TransactionsFieldData::Status(vec) => vec.push(value.as_u64().unwrap_or(0)),
                TransactionsFieldData::Sighash(vec) => {
                    vec.push(value.as_str().unwrap().to_string())
                }
            },
            _ => panic!("Unsupported type"),
        }
    }
}

pub fn create_field_data(field: &str, dataset: Dataset) -> FieldData {
    match dataset {
        Dataset::Blocks => create_block_field_data(field),
        Dataset::Transactions => create_transaction_field_data(field),
        _ => panic!("Dataset not found"),
    }
}

macro_rules! create_transaction_field_data {
    ($variant:ident) => {
        FieldData::TransactionsData(TransactionsFieldData::$variant(vec![]))
    };
}

fn create_transaction_field_data(field: &str) -> FieldData {
    match field {
        "id" => create_transaction_field_data!(Id),
        "transactionIndex" => create_transaction_field_data!(TransactionIndex),
        "from" => create_transaction_field_data!(From),
        "to" => create_transaction_field_data!(To),
        "hash" => create_transaction_field_data!(Hash),
        "gas" => create_transaction_field_data!(Gas),
        "gasPrice" => create_transaction_field_data!(GasPrice),
        "maxFeePerGas" => create_transaction_field_data!(MaxFeePerGas),
        "maxPriorityFeePerGas" => create_transaction_field_data!(MaxPriorityFeePerGas),
        "input" => create_transaction_field_data!(Input),
        "nonce" => create_transaction_field_data!(Nonce),
        "value" => create_transaction_field_data!(Value),
        "v" => create_transaction_field_data!(V),
        "r" => create_transaction_field_data!(R),
        "s" => create_transaction_field_data!(S),
        "yParity" => create_transaction_field_data!(YParity),
        "chainId" => create_transaction_field_data!(ChainId),
        "gasUsed" => create_transaction_field_data!(GasUsed),
        "cumulativeGasUsed" => create_transaction_field_data!(CumulativeGasUsed),
        "effectiveGasPrice" => create_transaction_field_data!(EffectiveGasPrice),
        "contractAddress" => create_transaction_field_data!(ContractAddress),
        "type" => create_transaction_field_data!(Type),
        "status" => create_transaction_field_data!(Status),
        "sighash" => create_transaction_field_data!(Sighash),
        _ => panic!("Field '{}' not found", field),
    }
}
macro_rules! create_block_field_data {
    ($variant:ident) => {
        FieldData::BlocksData(BlockFieldData::$variant(vec![]))
    };
}

pub fn create_block_field_data(field: &str) -> FieldData {
    match field {
        "hash" => create_block_field_data!(Hash),
        "number" => create_block_field_data!(Number),
        "parentHash" => create_block_field_data!(ParentHash),
        "timestamp" => create_block_field_data!(Timestamp),
        "miner" => create_block_field_data!(Miner),
        "stateRoot" => create_block_field_data!(StateRoot),
        "transactionsRoot" => create_block_field_data!(TransactionsRoot),
        "receiptsRoot" => create_block_field_data!(ReceiptsRoot),
        "gasUsed" => create_block_field_data!(GasUsed),
        "extraData" => create_block_field_data!(ExtraData),
        "baseFeePerGas" => create_block_field_data!(BaseFeePerGas),
        "logsBloom" => create_block_field_data!(LogsBloom),
        "totalDifficulty" => create_block_field_data!(TotalDifficulty),
        "size" => create_block_field_data!(Size),
        _ => panic!("Field '{}' not found", field),
    }
}

pub fn create_columns_from_field_data(
    field_map: &HashMap<String, FieldData>,
    fields: &[&str],
    //data: FieldData,
) -> Vec<Series> {
    let mut columns: Vec<Series> = vec![];
    //get dataset type here
    fields.iter().for_each(|field| match field_map.get(*field) {
        Some(FieldData::BlocksData(data)) => {
            match data {
                BlockFieldData::Hash(vec) => columns.push(Series::new(*field, vec)),
                BlockFieldData::Number(vec) => columns.push(Series::new(*field, vec)),
                BlockFieldData::ParentHash(vec) => columns.push(Series::new(*field, vec)),
                BlockFieldData::Timestamp(vec) => columns.push(Series::new(*field, vec)),
                BlockFieldData::Miner(vec) => columns.push(Series::new(*field, vec)),
                BlockFieldData::StateRoot(vec) => columns.push(Series::new(*field, vec)),
                BlockFieldData::TransactionsRoot(vec) => columns.push(Series::new(*field, vec)),
                BlockFieldData::ReceiptsRoot(vec) => columns.push(Series::new(*field, vec)),
                BlockFieldData::GasUsed(vec) => columns.push(Series::new(*field, vec)),
                BlockFieldData::ExtraData(vec) => columns.push(Series::new(*field, vec)),
                BlockFieldData::BaseFeePerGas(vec) => columns.push(Series::new(*field, vec)),
                BlockFieldData::LogsBloom(vec) => columns.push(Series::new(*field, vec)),
                BlockFieldData::TotalDifficulty(vec) => columns.push(Series::new(*field, vec)),
                BlockFieldData::Size(vec) => columns.push(Series::new(*field, vec)),
                //_ => panic!("{} not found", field),
            };
        }

        Some(FieldData::TransactionsData(data)) => match data {
            TransactionsFieldData::Id(vec) => columns.push(Series::new(*field, vec)),
            TransactionsFieldData::TransactionIndex(vec) => columns.push(Series::new(*field, vec)),
            TransactionsFieldData::From(vec) => columns.push(Series::new(*field, vec)),
            TransactionsFieldData::To(vec) => columns.push(Series::new(*field, vec)),
            TransactionsFieldData::Hash(vec) => columns.push(Series::new(*field, vec)),
            TransactionsFieldData::Gas(vec) => columns.push(Series::new(*field, vec)),
            TransactionsFieldData::GasPrice(vec) => columns.push(Series::new(*field, vec)),
            TransactionsFieldData::MaxFeePerGas(vec) => columns.push(Series::new(*field, vec)),
            TransactionsFieldData::MaxPriorityFeePerGas(vec) => {
                columns.push(Series::new(*field, vec))
            }
            TransactionsFieldData::Input(vec) => columns.push(Series::new(*field, vec)),
            TransactionsFieldData::Nonce(vec) => columns.push(Series::new(*field, vec)),
            TransactionsFieldData::Value(vec) => columns.push(Series::new(*field, vec)),
            TransactionsFieldData::V(vec) => columns.push(Series::new(*field, vec)),
            TransactionsFieldData::R(vec) => columns.push(Series::new(*field, vec)),
            TransactionsFieldData::S(vec) => columns.push(Series::new(*field, vec)),
            TransactionsFieldData::YParity(vec) => columns.push(Series::new(*field, vec)),
            TransactionsFieldData::ChainId(vec) => columns.push(Series::new(*field, vec)),
            TransactionsFieldData::GasUsed(vec) => columns.push(Series::new(*field, vec)),
            TransactionsFieldData::CumulativeGasUsed(vec) => columns.push(Series::new(*field, vec)),
            TransactionsFieldData::EffectiveGasPrice(vec) => columns.push(Series::new(*field, vec)),
            TransactionsFieldData::ContractAddress(vec) => columns.push(Series::new(*field, vec)),
            TransactionsFieldData::Type(vec) => columns.push(Series::new(*&field, vec)),
            TransactionsFieldData::Status(vec) => columns.push(Series::new(*field, vec)),
            TransactionsFieldData::Sighash(vec) => columns.push(Series::new(*field, vec)),
        },

        _ => panic!("{} not found", field),
    });
    columns
}
