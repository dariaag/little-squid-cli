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

pub fn create_field_data(field: &str) -> FieldData {
    match field {
        "hash" => {
            let block_hash = BlockFieldData::Hash(vec![]);
            let field_data = FieldData::BlocksData(block_hash);
            field_data
        }
        "number" => {
            let block_number = BlockFieldData::Number(vec![]);
            let field_data = FieldData::BlocksData(block_number);
            field_data
        }
        "parentHash" => {
            let parent_hash = BlockFieldData::ParentHash(vec![]);
            let field_data = FieldData::BlocksData(parent_hash);
            field_data
        }
        "timestamp" => {
            let timestamp = BlockFieldData::Timestamp(vec![]);
            let field_data = FieldData::BlocksData(timestamp);
            field_data
        }
        "miner" => {
            let miner = BlockFieldData::Miner(vec![]);
            let field_data = FieldData::BlocksData(miner);
            field_data
        }
        "stateRoot" => {
            let state_root = BlockFieldData::StateRoot(vec![]);
            let field_data = FieldData::BlocksData(state_root);
            field_data
        }
        "transactionsRoot" => {
            let transactions_root = BlockFieldData::TransactionsRoot(vec![]);
            let field_data = FieldData::BlocksData(transactions_root);
            field_data
        }
        "receiptsRoot" => {
            let receipts_root = BlockFieldData::ReceiptsRoot(vec![]);
            let field_data = FieldData::BlocksData(receipts_root);
            field_data
        }
        "gasUsed" => {
            let gas_used = BlockFieldData::GasUsed(vec![]);
            let field_data = FieldData::BlocksData(gas_used);
            field_data
        }
        "extraData" => {
            let extra_data = BlockFieldData::ExtraData(vec![]);
            let field_data = FieldData::BlocksData(extra_data);
            field_data
        }
        "baseFeePerGas" => {
            let base_fee_per_gas = BlockFieldData::BaseFeePerGas(vec![]);
            let field_data = FieldData::BlocksData(base_fee_per_gas);
            field_data
        }
        "logsBloom" => {
            let logs_bloom = BlockFieldData::LogsBloom(vec![]);
            let field_data = FieldData::BlocksData(logs_bloom);
            field_data
        }
        "totalDifficulty" => {
            let total_difficulty = BlockFieldData::TotalDifficulty(vec![]);
            let field_data = FieldData::BlocksData(total_difficulty);
            field_data
        }
        "size" => {
            let size = BlockFieldData::Size(vec![]);
            let field_data = FieldData::BlocksData(size);
            field_data
        }
        //tx
        "id" => {
            let id = TransactionsFieldData::Id(vec![]);
            let field_data = FieldData::TransactionsData(id);
            field_data
        }
        "transactionIndex" => {
            let transaction_index = TransactionsFieldData::TransactionIndex(vec![]);
            let field_data = FieldData::TransactionsData(transaction_index);
            field_data
        }
        "from" => {
            let from = TransactionsFieldData::From(vec![]);
            let field_data = FieldData::TransactionsData(from);
            field_data
        }
        "to" => {
            let to = TransactionsFieldData::To(vec![]);
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
