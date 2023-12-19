use indicatif::{ProgressBar, ProgressStyle};
use std::sync::{Arc, Mutex};
use std::thread;

use crossbeam::channel::Receiver;
use crossterm::{
    cursor, execute,
    style::{style, Color, PrintStyledContent, Stylize},
    terminal::{Clear, ClearType},
};
use std::io::{self, Result, Stderr, Write};
use std::time::Instant;

pub fn stats_loop(stats_rx: Receiver<u64>) -> Result<()> {
    let progress = Arc::new(Mutex::new((0, false))); // (progress, completed)
                                                     //let total_blocks = 10000;

    //let progress_clone = Arc::clone(&progress);
    // let processing_thread = thread::spawn(move || {
    //     for _ in 0..total_blocks {
    //         // Extract and save data here
    //         let mut progress = progress_clone.lock().unwrap();
    //         progress.0 += 1;
    //     }
    //     progress_clone.lock().unwrap().1 = true; // Mark as completed
    // });

    //let progress_bar = ProgressBar::new(total_blocks);
    let progress_bar = ProgressBar::new(100);

    let progress_bar_style = ProgressStyle::default_bar()
        .template(
            "[{elapsed_precise}] {spinner:.green} [{wide_bar:.cyan/blue}] {pos}/{len} ({eta})",
        )
        .unwrap();
    progress_bar.set_style(progress_bar_style);

    loop {
        let normalized_progress = stats_rx.recv().unwrap();

        //let (current_progress, completed) = *progress.lock().unwrap();
        progress_bar.set_position(normalized_progress as u64);
        if normalized_progress >= 100 {
            break;
        }
        thread::sleep(std::time::Duration::from_millis(100));
    }

    progress_bar.finish_with_message("Processing complete");
    Ok(())

    // processing_thread.join().unwrap();
    // display_thread.join().unwrap();
}
