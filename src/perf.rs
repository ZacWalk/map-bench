use std::{sync::{Arc, Mutex}, time::Duration};

#[derive(Debug, Clone, Copy)] // Add these derives if needed for convenience
pub struct Measurement<'a> {
    pub name : &'a str,
    pub latency: f64,
    pub thread_count: u64, 
}

pub fn calc_av_nanos(results: Arc<Mutex<Vec<Duration>>>, total_ops: u64) -> f64 {
    let results = results.lock().unwrap();
    let total_latency: u128 = results
        .iter()
        .map(|m| m.as_nanos())
        .sum();
    let average_duration = total_latency as f64 / total_ops as f64;
    average_duration
}
