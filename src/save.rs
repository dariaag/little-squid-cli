use crate::cli::config::Dataset;
use crate::export::export::save_to_file;
use crossbeam::channel::Receiver;

use serde_json::Value;

use std::io::Result;
pub fn write_loop(
    dataset: Dataset,
    fields: Vec<String>,
    write_rx: Receiver<Vec<Value>>,
) -> Result<()> {
    let mut counter = 0;
    loop {
        //receive the bytes from stats
        let buffer = write_rx.recv().unwrap();
        if buffer.is_empty() {
            break;
        }

        save_to_file(dataset, &fields, buffer, counter)?;
        counter += 1;
    }
    Ok(())
}
