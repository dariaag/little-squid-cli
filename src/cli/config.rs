use std::collections::HashMap;

use crate::{cli::opts::Opts, datasets};
use anyhow::{anyhow, Context, Ok, Result};

#[derive(Debug, PartialEq)]
pub struct Range {
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, PartialEq)]
pub enum Dataset {
    Blocks,
    Transactions,
    Logs,
}
#[derive(Debug)]
pub struct Config {
    pub dataset: Dataset,
    pub range: Range,
    pub fields: Vec<String>,
    pub options: HashMap<String, String>,
}

impl TryFrom<Opts> for Config {
    type Error = anyhow::Error;
    fn try_from(opts: Opts) -> Result<Self> {
        let dataset = get_dataset(opts.dataset)?;
        let range = get_range(opts.range)?.try_into()?;
        let fields = get_fields(opts.fields, dataset)?;
        let options = get_options(opts.options, dataset)?;

        Ok(Config {
            dataset,
            range,
            fields,
            options,
        })
    }
}

impl TryFrom<String> for Dataset {
    type Error = anyhow::Error;
    fn try_from(value: String) -> Result<Self> {
        match value.as_str() {
            "blocks" => Ok(Dataset::Blocks),
            "transactions" => Ok(Dataset::Transactions),
            "logs" => Ok(Dataset::Logs),
            _ => Err(anyhow!("Invalid dataset")),
        }
    }
}

impl TryFrom<Vec<String>> for Range {
    type Error = anyhow::Error;
    fn try_from(value: Vec<String>) -> Result<Self> {
        let start = value[0].parse::<u64>()?;
        let end = value[1].parse::<u64>()?;
        Ok(Range { start, end })
    }
}

//TODO make default range equal for archive height
fn get_range(range: Option<String>) -> Result<Vec<String>> {
    match range {
        Some(range) => Ok(range.split(":").map(String::from).collect()),
        None => Err(anyhow!("No range specified")),
    }
}

fn get_fields(fields: Option<String>, dataset: Dataset) -> Result<Vec<String>> {
    match dataset {
        Dataset::Blocks => verify_block_ields(
            fields
                .unwrap()
                .trim()
                .split(" ")
                .map(String::from)
                .collect(),
            dataset,
        ),
        Dataset::Transactions => Ok(vec!["hash".to_owned()]),
        Dataset::Logs => Ok(vec!["hash".to_owned()]),
    }
    // match fields {
    //     Some(fields) => verify_fields(fields.trim().split(" ").map(String::from).collect()),
    //     None => Err(anyhow!("No fields specified")),
    // }
}

fn verify_transaction_fields(fields: Vec<String>, dataset: Dataset) -> Result<Vec<String>> {
    let valid_fields = vec![
        "id",
        "transactionIndex",
        "from",
        "to",
        "hash",
        "gas",
        "gasPrice",
        "maxFeePerGas",
        "maxPriorityFeePerGas",
        "input",
        "nonce",
        "value",
        "v",
        "r",
        "s",
        "yParity",
        "chainId",
        "gasUsed",
        "cumulativeGasUsed",
        "effectiveGasPrice",
        "contractAddress",
        "type",
        "status",
        "sighash",
        "blockHash",
        "blockNumber",
        "timestamp",
        "type",
    ];
}

fn verify_block_ields(fields: Vec<String>, dataset: Dataset) -> Result<Vec<String>> {
    let valid_fields = vec![
        "hash",
        "number",
        "parentHash",
        "timestamp",
        "miner",
        "stateRoot",
        "transactionsRoot",
        "receiptsRoot",
        "gasUsed",
        "extraData",
        "baseFeePerGas",
        "logsBloom",
        "totalDifficulty",
        "size",
    ];
    let mut verified_fields: Vec<String> = vec![];
    for field in fields {
        if valid_fields.contains(&field.as_str()) {
            verified_fields.push(field);
        } else {
            return Err(anyhow!("Invalid field"));
        }
    }
    Ok(verified_fields)
}

fn get_dataset(dataset: Option<String>) -> Result<Dataset> {
    match dataset {
        Some(dataset) => match dataset.as_str() {
            "blocks" => Ok(Dataset::Blocks),
            "transactions" => Ok(Dataset::Transactions),
            "logs" => Ok(Dataset::Logs),
            _ => Err(anyhow!("Invalid dataset")),
        },
        None => Err(anyhow!("No dataset specified")),
    }
}

fn get_options(
    options: Option<String>,
    dataset: Dataset,
) -> Result<HashMap<String, String>, anyhow::Error> {
    match options {
        Some(options) => {
            if dataset == Dataset::Blocks {
                return Ok(HashMap::new());
            }
            let verified_options = get_verified_options(dataset).unwrap();
            let mut options_map: HashMap<String, String> = HashMap::new();
            for option in options.split(" ") {
                let option_value = option.split(":").collect::<Vec<&str>>();
                if verified_options.contains(&option_value[0].to_string()) {
                    options_map.insert(option_value[0].to_string(), option_value[1].to_string());
                } else {
                    return Err(anyhow!("Invalid option"));
                }
            }
            Ok(options_map)
        }
        None => Ok(HashMap::new()),
    }
}

fn get_verified_options(dataset: Dataset) -> Option<Vec<String>> {
    match dataset {
        Dataset::Blocks => Some(vec!["".to_owned()]),
        Dataset::Transactions => Some(vec![
            "from".to_string(),
            "to".to_string(),
            "sighash".to_string(),
        ]),
        Dataset::Logs => Some(vec![
            "address".to_string(),
            "topic0".to_string(),
            "topic1".to_string(),
            "topic2".to_string(),
            "topic3".to_string(),
        ]),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{Config, Dataset, Range};
    use crate::cli::opts::Opts;
    use anyhow::Result;

    #[test]
    fn test_blocks() -> Result<()> {
        let opts: Config = Opts {
            dataset: Some("blocks".to_owned()),
            range: Some("1:10".to_owned()),
            fields: Some("timestamp".to_owned()),
        }
        .try_into()?;
        assert_eq!(opts.dataset, Dataset::Blocks);
        assert_eq!(opts.range, Range { start: 1, end: 10 });
        assert_eq!(opts.fields, vec!["timestamp".to_owned()]);

        return Ok(());
    }
    //TODO add fields vs datasets
    #[test]
    fn test_block_fields() -> Result<()> {
        let opts: Config = Opts {
            dataset: Some("blocks".to_owned()),
            range: Some("1:10000".to_owned()),
            fields: Some("timestamp miner logsBloom".to_owned()),
        }
        .try_into()?;
        assert_eq!(opts.dataset, Dataset::Blocks);
        assert_eq!(
            opts.range,
            Range {
                start: 1,
                end: 10000
            }
        );
        assert_eq!(
            opts.fields,
            vec![
                "timestamp".to_owned(),
                "miner".to_owned(),
                "logsBloom".to_owned()
            ]
        );

        return Ok(());
    }

    // #[test]
    // fn test_print_key() -> Result<()> {
    //     let opts: Config = Opts {
    //         args: vec!["foo".to_string()],
    //         pwd: None,
    //         config: None,
    //     }
    //     .try_into()?;
    //     assert_eq!(opts.operation, Operation::Print(Some("foo".to_string())));

    //     return Ok(());
    // }
    // #[test]
    // fn test_add_key_value() -> Result<()> {
    //     let opts: Config = Opts {
    //         args: vec![
    //             String::from("add"),
    //             String::from("foo"),
    //             String::from("bar"),
    //         ],
    //         pwd: None,
    //         config: None,
    //     }
    //     .try_into()?;
    //     assert_eq!(
    //         opts.operation,
    //         Operation::Add(String::from("foo"), String::from("bar"))
    //     );

    //     return Ok(());
    // }
    // #[test]
    // fn test_remove_value() -> Result<()> {
    //     let opts: Config = Opts {
    //         args: vec![String::from("rm"), String::from("foo")],
    //         pwd: None,
    //         config: None,
    //     }
    //     .try_into()?;
    //     assert_eq!(opts.operation, Operation::Remove(String::from("foo")));

    //     return Ok(());
    // }
}