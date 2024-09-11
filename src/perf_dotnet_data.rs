use std::sync::LazyLock;
use crate::perf::Measurement;

// Test data from last .net test run
// 
pub static PERF_DATA_DOT_NET_95: LazyLock<Vec<Measurement>> = LazyLock::new(|| {
    vec![
        Measurement { name: "c#", thread_count: 1, total: 95 },
        Measurement { name: "c#", thread_count: 2, total: 110 },
        Measurement { name: "c#", thread_count: 3, total: 124 },
        Measurement { name: "c#", thread_count: 4, total: 140 },
        Measurement { name: "c#", thread_count: 5, total: 163 },
        Measurement { name: "c#", thread_count: 6, total: 175 },
        Measurement { name: "c#", thread_count: 7, total: 189 },
        Measurement { name: "c#", thread_count: 8, total: 209 },
        Measurement { name: "c#", thread_count: 9, total: 224 },
        Measurement { name: "c#", thread_count: 10, total: 237 },
        Measurement { name: "c#", thread_count: 11, total: 258 },
        Measurement { name: "c#", thread_count: 12, total: 279 },
        Measurement { name: "c#", thread_count: 13, total: 287 },
        Measurement { name: "c#", thread_count: 14, total: 299 },
        Measurement { name: "c#", thread_count: 15, total: 319 },
        Measurement { name: "c#", thread_count: 16, total: 333 },
    ]
});