pub fn normalize_progess(start_block: u64, end_block: u64, current_block: u64) -> u64 {
    let total_blocks = end_block - start_block;
    let current_progress = current_block - start_block;
    let normalized_progress = (current_progress * 100) / total_blocks;
    normalized_progress
}
