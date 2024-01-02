use crossbeam::channel::Receiver;
use indicatif::{ProgressBar, ProgressStyle};
use std::io::Result;
use std::thread;

pub fn stats_loop(stats_rx: Receiver<u64>) -> Result<()> {
    //let progress = Arc::new(Mutex::new((0, false))); // (progress, completed)

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
