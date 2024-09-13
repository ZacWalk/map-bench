#[derive(Debug, Clone, Copy)] // Add these derives if needed for convenience
pub struct Measurement<'a> {
    pub name : &'a str,
    pub latency: u64,
    pub thread_count: u64, 
}