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

impl FieldData {
    pub fn add_value(&mut self, value: &serde_json::Value) {
        match self {
            FieldData::Hash(vec) => vec.push(value.as_str().unwrap().to_string()),
            FieldData::Number(vec) => vec.push(value.as_u64().unwrap()),
            FieldData::ParentHash(vec) => vec.push(value.as_str().unwrap().to_string()),
            FieldData::Timestamp(vec) => vec.push(value.as_f64().unwrap() as u64),
            FieldData::Miner(vec) => vec.push(value.as_str().unwrap().to_string()),
            FieldData::StateRoot(vec) => vec.push(value.as_str().unwrap().to_string()),
            FieldData::TransactionsRoot(vec) => vec.push(value.as_str().unwrap().to_string()),
            FieldData::ReceiptsRoot(vec) => vec.push(value.as_str().unwrap().to_string()),
            FieldData::GasUsed(vec) => vec.push(value.as_u64().unwrap()),
            FieldData::ExtraData(vec) => vec.push(value.as_str().unwrap().to_string()),
            FieldData::BaseFeePerGas(vec) => vec.push(value.as_u64().unwrap_or(0)),
            FieldData::LogsBloom(vec) => vec.push(value.as_str().unwrap().to_string()),
            FieldData::TotalDifficulty(vec) => vec.push(value.as_u64().unwrap()),
            FieldData::Size(vec) => vec.push(value.as_u64().unwrap()),
            _ => panic!("Unsupported type"),
        }
    }
}
