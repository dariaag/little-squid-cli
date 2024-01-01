//use polars::prelude::*;
use crate::cli::config::Dataset;
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
        "hash" => {
            let block_hash = fields::BlockFieldData::Hash(vec![]);
            let field_data = FieldData::BlocksData(block_hash);
            field_data
        }
        "number" => {
            let block_number = fields::BlockFieldData::Number(vec![]);
            let field_data = FieldData::BlocksData(block_number);
            field_data
        }
        "parentHash" => {
            let parent_hash = fields::BlockFieldData::ParentHash(vec![]);
            let field_data = FieldData::BlocksData(parent_hash);
            field_data
        }
        "timestamp" => {
            let timestamp = fields::BlockFieldData::Timestamp(vec![]);
            let field_data = FieldData::BlocksData(timestamp);
            field_data
        }
        "miner" => {
            let miner = fields::BlockFieldData::Miner(vec![]);
            let field_data = FieldData::BlocksData(miner);
            field_data
        }
        "stateRoot" => {
            let state_root = fields::BlockFieldData::StateRoot(vec![]);
            let field_data = FieldData::BlocksData(state_root);
            field_data
        }
        "transactionsRoot" => {
            let transactions_root = fields::BlockFieldData::TransactionsRoot(vec![]);
            let field_data = FieldData::BlocksData(transactions_root);
            field_data
        }
        "receiptsRoot" => {
            let receipts_root = fields::BlockFieldData::ReceiptsRoot(vec![]);
            let field_data = FieldData::BlocksData(receipts_root);
            field_data
        }
        "gasUsed" => {
            let gas_used = fields::BlockFieldData::GasUsed(vec![]);
            let field_data = FieldData::BlocksData(gas_used);
            field_data
        }
        "extraData" => {
            let extra_data = fields::BlockFieldData::ExtraData(vec![]);
            let field_data = FieldData::BlocksData(extra_data);
            field_data
        }
        "baseFeePerGas" => {
            let base_fee_per_gas = fields::BlockFieldData::BaseFeePerGas(vec![]);
            let field_data = FieldData::BlocksData(base_fee_per_gas);
            field_data
        }
        "logsBloom" => {
            let logs_bloom = fields::BlockFieldData::LogsBloom(vec![]);
            let field_data = FieldData::BlocksData(logs_bloom);
            field_data
        }
        "totalDifficulty" => {
            let total_difficulty = fields::BlockFieldData::TotalDifficulty(vec![]);
            let field_data = FieldData::BlocksData(total_difficulty);
            field_data
        }
        "size" => {
            let size = fields::BlockFieldData::Size(vec![]);
            let field_data = FieldData::BlocksData(size);
            field_data
        }
        //tx
        "id" => {
            let id = fields::TransactionsFieldData::Id(vec![]);
            let field_data = FieldData::TransactionsData(id);
            field_data
        }
        "transactionIndex" => {
            let transaction_index = fields::TransactionsFieldData::TransactionIndex(vec![]);
            let field_data = FieldData::TransactionsData(transaction_index);
            field_data
        }
        "from" => {
            let from = fields::TransactionsFieldData::From(vec![]);
            let field_data = FieldData::TransactionsData(from);
            field_data
        }
        "to" => {
            let to = fields::TransactionsFieldData::To(vec![]);
            let field_data = FieldData::TransactionsData(to);
            field_data
        }

        // "number" => FieldData::Number(vec![]),
        // "parentHash" => FieldData::ParentHash(vec![]),
        // "timestamp" => FieldData::Timestamp(vec![]),
        // "miner" => FieldData::Miner(vec![]),
        // "stateRoot" => FieldData::StateRoot(vec![]),
        // "transactionsRoot" => FieldData::TransactionsRoot(vec![]),
        // "receiptsRoot" => FieldData::ReceiptsRoot(vec![]),
        // "gasUsed" => FieldData::GasUsed(vec![]),
        // "extraData" => FieldData::ExtraData(vec![]),
        // "baseFeePerGas" => FieldData::BaseFeePerGas(vec![]),
        // "logsBloom" => FieldData::LogsBloom(vec![]),
        // "totalDifficulty" => FieldData::TotalDifficulty(vec![]),
        // "size" => FieldData::Size(vec![]),
        _ => panic!("{} not found", field),
    }
}

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
        match dataset {
            Dataset::Blocks => {
                if let Some(header) = json_obj.get("header") {
                    //println!("HEADER: {:?}", header);
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
                        //println!("HEADER: {:?}", header);

                        //NOTE HEADERS ARE NOT ARRAYS AND TX ARE< VARIABILITY SHOULD BE ON EXPORT #TODO
                        // tx -> header[0]
                        // blocks -> header (no array)
                        //println!("FIELD: {:?}", field);
                        let v = header[0].get(*field);
                        //println!("V: {:?}", v);
                        if let Some(data) = field_map.get_mut(*field) {
                            if let Some(value) = header.get(*field) {
                                //println!("Value: {:?}", value);
                                data.add_value(value);
                            }
                            match dataset {
                                Dataset::Blocks => {
                                    if let Some(value) = header.get(*field) {
                                        //println!("Value: {:?}", value);
                                        data.add_value(value);
                                    }
                                }
                                Dataset::Transactions => {
                                    if let Some(value) = header[0].get(*field) {
                                        //println!("Value: {:?}", value);
                                        data.add_value(value);
                                    }
                                }
                                _ => panic!("Dataset not found"),
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
            Dataset::Transactions => {
                //println!("JSON OBJ: {:?}", json_obj);

                if let Some(header) = json_obj.get("transactions") {
                    //println!("HEADER: {:?}", header);
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
                        //println!("HEADER: {:?}", header);

                        //NOTE HEADERS ARE NOT ARRAYS AND TX ARE< VARIABILITY SHOULD BE ON EXPORT #TODO
                        // tx -> header[0]
                        // blocks -> header (no array)
                        //println!("FIELD: {:?}", field);
                        let v = header[0].get(*field);
                        //println!("V: {:?}", v);
                        if let Some(data) = field_map.get_mut(*field) {
                            if let Some(value) = header.get(*field) {
                                //println!("Value: {:?}", value);
                                data.add_value(value);
                            }
                            match dataset {
                                Dataset::Blocks => {
                                    if let Some(value) = header.get(*field) {
                                        //println!("Value: {:?}", value);
                                        data.add_value(value);
                                    }
                                }
                                Dataset::Transactions => {
                                    if let Some(value) = header[0].get(*field) {
                                        //println!("Value: {:?}", value);
                                        data.add_value(value);
                                    }
                                }
                                _ => panic!("Dataset not found"),
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
            _ => panic!("Dataset not found"),
        }
    }
    //create series from fields

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
                _ => panic!("{} not found", field),
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

    // Create DataFrames

    //let df = DataFrame::new(columns).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    let df = DataFrame::new(columns).unwrap();
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
    //let fields = vec!["number", "hash", "timestamp", "baseFeePerGas"];
    let json_element = json_data[0].get("header").unwrap().clone();
    //let fields = get_json_fields(&json_element);
    //let fields = vec!["from", "to"];
    let fields = fields_vec.iter().map(|s| s.as_str()).collect();

    let tx = json_data[0].get("transactions").unwrap().clone();

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
