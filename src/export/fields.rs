use polars::prelude::{
    DataFrame, NamedFrom, ParquetCompression, ParquetWriter, Result as PolarsResult, Series,
    ZstdLevel,
};
use serde_json::Value;
use std::collections::HashMap;

use std::fs::{self, File};
use std::io::Error;
use std::path::Path;
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
    Value(Vec<u64>),
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
            FieldData::BlocksData(data) => self.add_blocks_value(value),
            FieldData::TransactionsData(data) => self.add_transactions_value(value),
            _ => panic!("Unsupported type"),
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
                _ => panic!("Unsupported type"),
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
                TransactionsFieldData::Value(vec) => vec.push(value.as_u64().unwrap()),
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
                _ => panic!("Unsupported type"),
            },
            _ => panic!("Unsupported type"),
        }
    }
}
