//use anyhow::Result as AnyhowResult;
use clap::Parser;
use crossbeam::channel::unbounded;
use little_squid_cli::cli::config::Config;
use little_squid_cli::cli::opts::Opts;
use little_squid_cli::datasets::blocks;
use little_squid_cli::progress::stats;
use little_squid_cli::save;
use std::io::Result;
use std::thread;
use tokio;
#[tokio::main]
async fn main() -> Result<()> {
    let config: Config = Opts::parse().try_into().unwrap();
    println!("CONFIG: {:?}", config);
    let fields = config.fields.clone();
    let start_time = std::time::Instant::now();
    //let (stat_tx, stat_rx) = unbounded();

    let (write_tx, write_rx) = unbounded();

    let (stat_tx, stat_rx) = unbounded();
    //let read_handle = thread::spawn(move || block_on(blocks::block_loop(1, 1000, write_tx)));
    let read_handle = tokio::spawn(blocks::block_loop(
        config.dataset,
        config.range.start,
        config.range.end,
        config.fields,
        config.options,
        write_tx,
        stat_tx,
    ));
    let stats_handle = thread::spawn(move || stats::stats_loop(stat_rx));

    //let stats_handle = thread::spawn(move || stats::stats_loop(silent, stat_rx));
    let write_handle = thread::spawn(move || save::write_loop(config.dataset, fields, write_rx));

    //crash if anythread have crashed
    //.join returns io result wrapping our resul
    //let read_io_result = read_handle.join().unwrap();
    let read_io_result = read_handle.await?;
    let stats_io_result = stats_handle.join().unwrap();
    let write_io_result = write_handle.join().unwrap();
    //return error if any thread returned error
    read_io_result?;
    stats_io_result?;
    write_io_result?;
    let elapsed_time = start_time.elapsed();

    println!("Elapsed time: {:?}", elapsed_time);
    Ok(())
}
