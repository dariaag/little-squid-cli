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
            FieldData::Id(vec) => vec.push(value.as_str().unwrap().to_string()),
            FieldData::TransactionIndex(vec) => vec.push(value.as_u64().unwrap()),
            FieldData::From(vec) => vec.push(value.as_str().unwrap().to_string()),
            FieldData::To(vec) => vec.push(value.as_str().unwrap_or("").to_string()),
            FieldData::Hash(vec) => vec.push(value.as_str().unwrap().to_string()),
            FieldData::Gas(vec) => vec.push(value.as_u64().unwrap()),
            FieldData::GasPrice(vec) => vec.push(value.as_u64().unwrap()),
            FieldData::MaxFeePerGas(vec) => vec.push(value.as_u64().unwrap_or(0)),
            FieldData::MaxPriorityFeePerGas(vec) => vec.push(value.as_u64().unwrap_or(0)),
            FieldData::Input(vec) => vec.push(value.as_str().unwrap().to_string()),
            FieldData::Nonce(vec) => vec.push(value.as_u64().unwrap()),
            FieldData::Value(vec) => vec.push(value.as_u64().unwrap()),
            FieldData::V(vec) => vec.push(value.as_u64().unwrap_or(0)),
            FieldData::R(vec) => vec.push(value.as_str().unwrap_or("").to_string()),
            FieldData::S(vec) => vec.push(value.as_str().unwrap_or("").to_string()),
            FieldData::YParity(vec) => vec.push(value.as_u64().unwrap_or(0)),
            FieldData::ChainId(vec) => vec.push(value.as_u64().unwrap_or(0)),
            FieldData::GasUsed(vec) => vec.push(value.as_u64().unwrap_or(0)),
            FieldData::CumulativeGasUsed(vec) => vec.push(value.as_u64().unwrap_or(0)),
            FieldData::EffectiveGasPrice(vec) => vec.push(value.as_u64().unwrap_or(0)),
            FieldData::ContractAddress(vec) => vec.push(value.as_str().unwrap_or("").to_string()),
            FieldData::Type(vec) => vec.push(value.as_u64().unwrap_or(0)),
            FieldData::Status(vec) => vec.push(value.as_u64().unwrap_or(0)),
            FieldData::Sighash(vec) => vec.push(value.as_str().unwrap().to_string()),
        }
    }
}
