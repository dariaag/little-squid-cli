//use polars::prelude::*;
use crate::cli::config::Dataset;

use polars::prelude::{DataFrame, ParquetCompression, ParquetWriter, Series};
use serde_json::Value;
use std::collections::HashMap;

use crate::export::fields::{create_columns_from_field_data, create_field_data, FieldData};
use std::fs::{self, File};
use std::io::Error;
use std::path::Path;

fn blocks_to_dataframe(
    dataset: Dataset,
    json_data: Vec<Value>,
    fields: Vec<&str>,
) -> Result<DataFrame, Error> {
    //let block_fields: Vec<(&str, FieldData)> = vec![(fields[0], FieldData::Hash(vec![]))];
    let block_fields: Vec<(&str, FieldData)> = fields
        .iter()
        .map(|&field| (field, create_field_data(field, dataset)))
        .collect();

    let mut field_map: HashMap<String, FieldData> = block_fields
        .into_iter()
        .map(|(name, data)| (name.to_string(), data))
        .collect();
    //put loop inside func, return mutable reference to fieldmap
    field_map = process_json_object(json_data, field_map, &fields, &dataset).unwrap();
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

fn process_json_object(
    json_data: Vec<Value>,
    mut field_map: HashMap<String, FieldData>,
    fields: &[&str],
    dataset: &Dataset,
) -> Result<HashMap<String, FieldData>, Error> {
    for json_obj in json_data {
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
                if let Some(tx_list) = json_obj.get("transactions") {
                    //check types here TODO
                    fields.iter().for_each(|field| {
                        //Same for logs
                        if let Some(data) = field_map.get_mut(*field) {
                            for tx in tx_list.as_array().unwrap() {
                                if let Some(value) = tx.get(*field) {
                                    data.add_value(value);
                                }
                            }
                        }
                    });
                    //println!("FIELD MAP: {:?}", field_map);
                }
            }
            _ => panic!("Dataset not found"),
        }
    }

    Ok(field_map)
}
