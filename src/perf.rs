#[derive(Debug, Clone, Copy)] // Add these derives if needed for convenience
pub struct Measurement {
    pub name : &'static str,
    pub total: u64,
    pub thread_count: u64, 
}