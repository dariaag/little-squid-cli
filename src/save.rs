use crate::export::export::save_to_file;
use crossbeam::channel::Receiver;
use json_writer::JSONObjectWriter;
use serde_json::Value;
use std::fs::File;
use std::io::{self, BufWriter, ErrorKind, Result, Write};
pub fn write_loop(write_rx: Receiver<Vec<Value>>) -> Result<()> {
    let mut counter = 0;
    loop {
        //receive the bytes from stats
        let buffer = write_rx.recv().unwrap();
        if buffer.is_empty() {
            break;
        }
        //let serialized = serde_json::to_string(&buffer)
        //.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        //

        save_to_file(buffer, counter)?;
        counter += 1;

        // Write the serialized string to the file
        // writer.write_all(serialized.as_bytes())?;

        // let mut object_writer = JSONObjectWriter::new(&mut object_str);
        // if let Err(e) = writer.write_all(&buffer) {
        //     if e.kind() == ErrorKind::BrokenPipe {
        //         return Ok(());
        //     }
        //     return Err(e);
        // };
    }
    Ok(())
}

fn write_vec_to_file(vec: Vec<Value>, file_path: &str) -> Result<()> {
    // Serialize the Vec<Value> to a JSON string
    let serialized = serde_json::to_string(&vec)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    // Create a file and use a buffered writer for efficiency
    let file = File::create(file_path)?;
    let mut writer = BufWriter::new(file);

    // Write the serialized string to the file
    writer.write_all(serialized.as_bytes())?;

    Ok(())
}
