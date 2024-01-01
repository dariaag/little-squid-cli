//use polars::prelude::*;
use crate::cli::config::Dataset;

use polars::prelude::{DataFrame, NamedFrom, ParquetCompression, ParquetWriter, Series};
use serde_json::Value;
use std::collections::HashMap;

use crate::export::fields::{create_field_data, FieldData};
use std::fs::{self, File};
use std::io::Error;
use std::path::Path;

use super::fields;

fn blocks_to_dataframe(
    dataset: Dataset,
    json_data: Vec<Value>,
    fields: Vec<&str>,
) -> Result<DataFrame, Error> {
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
        process_json_object(&json_obj, &mut field_map, &fields, &dataset)?;
    }
    //create series from fields
    let columns: Vec<Series> = create_columns_from_field_data(&field_map, &fields);
    // Create DataFrames
    //let df = DataFrame::new(columns).unwrap();
    let df = DataFrame::new(columns)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    println!("DF: {:?}", df);
    Ok(df)
}
//pass fields here
pub fn save_to_file(
    dataset: Dataset,
    fields_vec: &Vec<String>,
    json_data: Vec<Value>,
    counter: usize,
) -> Result<(), Error> {
    //let fields = vec!["hash", "number", "parentHash", "timestamp", "miner", "stateRoot", "transactionsRoot", "receiptsRoot", "gasUsed", "extraData", "baseFeePerGas", "logsBloom", "totalDifficulty", "size"];
    //let json_element = json_data[0].get("header").unwrap().clone();
    let fields = fields_vec.iter().map(|s| s.as_str()).collect();
    //let tx = json_data[0].get("transactions").unwrap().clone();

    //let df = blocks_to_dataframe(json_data, fields)?;
    let df = blocks_to_dataframe(dataset, json_data, fields)?;
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

fn create_columns_from_field_data(
    field_map: &HashMap<String, FieldData>,
    fields: &[&str],
) -> Vec<Series> {
    let mut columns: Vec<Series> = vec![];
    //get dataset type here
    fields.iter().for_each(|field| match field_map.get(*field) {
        Some(FieldData::BlocksData(data)) => {
            match data {
                fields::BlockFieldData::Hash(vec) => columns.push(Series::new(*field, vec)),
                fields::BlockFieldData::Number(vec) => columns.push(Series::new(*field, vec)),
                fields::BlockFieldData::ParentHash(vec) => columns.push(Series::new(*field, vec)),
                fields::BlockFieldData::Timestamp(vec) => columns.push(Series::new(*field, vec)),
                fields::BlockFieldData::Miner(vec) => columns.push(Series::new(*field, vec)),
                fields::BlockFieldData::StateRoot(vec) => columns.push(Series::new(*field, vec)),
                fields::BlockFieldData::TransactionsRoot(vec) => {
                    columns.push(Series::new(*field, vec))
                }
                fields::BlockFieldData::ReceiptsRoot(vec) => columns.push(Series::new(*field, vec)),
                fields::BlockFieldData::GasUsed(vec) => columns.push(Series::new(*field, vec)),
                fields::BlockFieldData::ExtraData(vec) => columns.push(Series::new(*field, vec)),
                fields::BlockFieldData::BaseFeePerGas(vec) => {
                    columns.push(Series::new(*field, vec))
                }
                fields::BlockFieldData::LogsBloom(vec) => columns.push(Series::new(*field, vec)),
                fields::BlockFieldData::TotalDifficulty(vec) => {
                    columns.push(Series::new(*field, vec))
                }
                fields::BlockFieldData::Size(vec) => columns.push(Series::new(*field, vec)),
                //_ => panic!("{} not found", field),
            };
        }

        Some(FieldData::TransactionsData(data)) => match data {
            fields::TransactionsFieldData::Id(vec) => columns.push(Series::new(*field, vec)),
            fields::TransactionsFieldData::TransactionIndex(vec) => {
                columns.push(Series::new(*field, vec))
            }
            fields::TransactionsFieldData::From(vec) => columns.push(Series::new(*field, vec)),
            fields::TransactionsFieldData::To(vec) => columns.push(Series::new(*field, vec)),
            fields::TransactionsFieldData::Hash(vec) => columns.push(Series::new(*field, vec)),
            fields::TransactionsFieldData::Gas(vec) => columns.push(Series::new(*field, vec)),
            fields::TransactionsFieldData::GasPrice(vec) => columns.push(Series::new(*field, vec)),
            fields::TransactionsFieldData::MaxFeePerGas(vec) => {
                columns.push(Series::new(*field, vec))
            }
            fields::TransactionsFieldData::MaxPriorityFeePerGas(vec) => {
                columns.push(Series::new(*field, vec))
            }
            fields::TransactionsFieldData::Input(vec) => columns.push(Series::new(*field, vec)),
            fields::TransactionsFieldData::Nonce(vec) => columns.push(Series::new(*field, vec)),
            fields::TransactionsFieldData::Value(vec) => columns.push(Series::new(*field, vec)),
            fields::TransactionsFieldData::V(vec) => columns.push(Series::new(*field, vec)),
            fields::TransactionsFieldData::R(vec) => columns.push(Series::new(*field, vec)),
            fields::TransactionsFieldData::S(vec) => columns.push(Series::new(*field, vec)),
            fields::TransactionsFieldData::YParity(vec) => columns.push(Series::new(*field, vec)),
            fields::TransactionsFieldData::ChainId(vec) => columns.push(Series::new(*field, vec)),
            fields::TransactionsFieldData::GasUsed(vec) => columns.push(Series::new(*field, vec)),
            fields::TransactionsFieldData::CumulativeGasUsed(vec) => {
                columns.push(Series::new(*field, vec))
            }
            fields::TransactionsFieldData::EffectiveGasPrice(vec) => {
                columns.push(Series::new(*field, vec))
            }
            fields::TransactionsFieldData::ContractAddress(vec) => {
                columns.push(Series::new(*field, vec))
            }
            fields::TransactionsFieldData::Type(vec) => columns.push(Series::new(*&field, vec)),
            fields::TransactionsFieldData::Status(vec) => columns.push(Series::new(*field, vec)),
            fields::TransactionsFieldData::Sighash(vec) => columns.push(Series::new(*field, vec)),
        },

        _ => panic!("{} not found", field),
    });
    columns
}

fn process_json_object(
    json_obj: &Value,
    field_map: &mut HashMap<String, FieldData>,
    fields: &[&str],
    dataset: &Dataset,
) -> Result<(), Error> {
    // Extracting logic to process a single json object
    match dataset {
        Dataset::Blocks => {
            if let Some(header) = json_obj.get("header") {
                //check types here TODO
                fields.iter().for_each(|field| {
                    if let Some(data) = field_map.get_mut(*field) {
                        if let Some(value) = header.get(*field) {
                            //println!("Value: {:?}", value);
                            data.add_value(value);
                        }
                    }
                });
                //println!("FIELD MAP: {:?}", field_map);
            }
        }
        Dataset::Transactions => {
            //println!("JSON OBJ: {:?}", json_obj);

            if let Some(header) = json_obj.get("transactions") {
                //check types here TODO

                fields.iter().for_each(|field| {
                    if let Some(data) = field_map.get_mut(*field) {
                        if let Some(value) = header.get(*field) {
                            //println!("Value: {:?}", value);
                            data.add_value(value);
                        }
                    }
                });
                //println!("FIELD MAP: {:?}", field_map);
            }
        }
        _ => panic!("Dataset not found"),
    }
    Ok(())
}
