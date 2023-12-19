//use polars::prelude::*;
use polars::prelude::{
    DataFrame, NamedFrom, ParquetCompression, ParquetWriter, Result as PolarsResult, Series,
    ZstdLevel,
};
use serde_json::Value;
use std::collections::HashMap;

use crate::export::fields::FieldData;
use std::fs::{self, File};
use std::io::Error;
use std::path::Path;

use super::fields;

fn create_field_data(field: &str) -> FieldData {
    match field {
        "hash" => FieldData::Hash(vec![]),
        "number" => FieldData::Number(vec![]),
        "parentHash" => FieldData::ParentHash(vec![]),
        "timestamp" => FieldData::Timestamp(vec![]),
        "miner" => FieldData::Miner(vec![]),
        "stateRoot" => FieldData::StateRoot(vec![]),
        "transactionsRoot" => FieldData::TransactionsRoot(vec![]),
        "receiptsRoot" => FieldData::ReceiptsRoot(vec![]),
        "gasUsed" => FieldData::GasUsed(vec![]),
        "extraData" => FieldData::ExtraData(vec![]),
        "baseFeePerGas" => FieldData::BaseFeePerGas(vec![]),
        "logsBloom" => FieldData::LogsBloom(vec![]),
        "totalDifficulty" => FieldData::TotalDifficulty(vec![]),
        "size" => FieldData::Size(vec![]),
        _ => panic!("{} not found", field),
    }
}

fn blocks_to_dataframe(json_data: Vec<Value>, fields: Vec<&str>) -> Result<DataFrame, Error> {
    //let block_fields: Vec<(&str, FieldData)> = vec![(fields[0], FieldData::Hash(vec![]))];
    let block_fields: Vec<(&str, FieldData)> = fields
        .iter()
        .map(|&field| (field, create_field_data(field)))
        .collect();

    let mut field_map: HashMap<String, FieldData> = block_fields
        .into_iter()
        .map(|(name, data)| (name.to_string(), data))
        .collect();

    for json_obj in json_data {
        if let Some(header) = json_obj.get("header") {
            /* if let Some(FieldData::Hash(vec)) = field_map.get_mut("hash") {
            vec.push(
                header
                    .get("hash")
                    .and_then(Value::as_str)
                    .map(String::from)
                    .unwrap(),
            ); */

            //check types here TODO
            fields.iter().for_each(|field| {
                if let Some(data) = field_map.get_mut(*field) {
                    if let Some(value) = header.get(*field) {
                        data.add_value(value);
                    }
                }
                // if let Some(FieldData::Hash(vec)) = field_map.get_mut(*field) {
                //     vec.push(
                //         header
                //             .get(*field)
                //             .and_then(Value::as_str)
                //             .map(String::from)
                //             .unwrap(),
                //     );
                // }
                // if let Some(FieldData::Number(vec)) = field_map.get_mut(*field) {
                //     vec.push(header.get(*field).and_then(Value::as_u64).unwrap());
                // }
                // if let Some(FieldData::ParentHash(vec)) = field_map.get_mut(*field) {
                //     vec.push(
                //         header
                //             .get(*field)
                //             .and_then(Value::as_str)
                //             .map(String::from)
                //             .unwrap(),
                //     );
                // }
            });
            //println!("FIELD MAP: {:?}", field_map);
        }
    }
    //create series from fields

    let mut columns: Vec<Series> = vec![];
    fields.iter().for_each(|field| match field_map.get(*field) {
        Some(FieldData::Hash(vec)) => {
            columns.push(Series::new(*field, vec));
        }
        Some(FieldData::Number(vec)) => columns.push(Series::new(*field, vec)),
        Some(FieldData::ParentHash(vec)) => columns.push(Series::new(*field, vec)),
        Some(FieldData::Timestamp(vec)) => columns.push(Series::new(*field, vec)),
        Some(FieldData::Miner(vec)) => columns.push(Series::new(*field, vec)),
        Some(FieldData::StateRoot(vec)) => columns.push(Series::new(*field, vec)),
        Some(FieldData::TransactionsRoot(vec)) => columns.push(Series::new(*field, vec)),
        Some(FieldData::ReceiptsRoot(vec)) => columns.push(Series::new(*field, vec)),
        Some(FieldData::GasUsed(vec)) => columns.push(Series::new(*field, vec)),
        Some(FieldData::ExtraData(vec)) => columns.push(Series::new(*field, vec)),
        Some(FieldData::BaseFeePerGas(vec)) => columns.push(Series::new(*field, vec)),
        Some(FieldData::LogsBloom(vec)) => columns.push(Series::new(*field, vec)),
        Some(FieldData::TotalDifficulty(vec)) => columns.push(Series::new(*field, vec)),
        Some(FieldData::Size(vec)) => columns.push(Series::new(*field, vec)),
        _ => panic!("{} not found", field),
    });

    // Create DataFrames

    //let df = DataFrame::new(columns).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    let df = DataFrame::new(columns).unwrap();

    Ok(df)
}

pub fn save_to_file(json_data: Vec<Value>, counter: usize) -> Result<(), Error> {
    //let fields = vec!["hash", "number", "parentHash", "timestamp", "miner", "stateRoot", "transactionsRoot", "receiptsRoot", "gasUsed", "extraData", "baseFeePerGas", "logsBloom", "totalDifficulty", "size"];
    //let fields = vec!["number", "hash", "timestamp", "baseFeePerGas"];
    let json_element = json_data[0].get("header").unwrap().clone();
    let fields = get_json_fields(&json_element);

    let df = blocks_to_dataframe(json_data, fields)?;
    let folder = Path::new("../data");

    if !folder.exists() {
        fs::create_dir_all(folder)?;
    }
    //TODO name file with blocks num and data name
    let file_path = format!("../data/my_dataframe_{}.parquet", counter);

    let file =
        File::create(file_path).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    ParquetWriter::new(file)
        .with_compression(ParquetCompression::Snappy)
        .finish(&mut df.clone())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    Ok(())
}

fn get_json_fields(value: &Value) -> Vec<&str> {
    let mut fields = vec![];
    match value {
        Value::Object(map) => {
            for (key, _value) in map {
                fields.push(key.as_str());
            }
        }
        _ => println!("The provided JSON value is not an object."),
    }
    fields
}
