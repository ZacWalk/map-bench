use std::sync::LazyLock;
use crate::perf::Measurement;

pub static PERF_DATA_DOT_NET_99: LazyLock<Vec<Measurement>> = LazyLock::new(|| {  vec![
Measurement { name: "c#", thread_count: 1, latency: 82 },
Measurement { name: "c#", thread_count: 2, latency: 91 },
Measurement { name: "c#", thread_count: 3, latency: 92 },
Measurement { name: "c#", thread_count: 4, latency: 97 },
Measurement { name: "c#", thread_count: 5, latency: 102 },
Measurement { name: "c#", thread_count: 6, latency: 107 },
Measurement { name: "c#", thread_count: 7, latency: 113 },
Measurement { name: "c#", thread_count: 8, latency: 118 },
Measurement { name: "c#", thread_count: 9, latency: 117 },
Measurement { name: "c#", thread_count: 10, latency: 127 },
Measurement { name: "c#", thread_count: 11, latency: 131 },
Measurement { name: "c#", thread_count: 12, latency: 139 },
Measurement { name: "c#", thread_count: 13, latency: 149 },
Measurement { name: "c#", thread_count: 14, latency: 151 },
Measurement { name: "c#", thread_count: 15, latency: 167 },
Measurement { name: "c#", thread_count: 16, latency: 172 },
] });
pub static PERF_DATA_DOT_NET_100: LazyLock<Vec<Measurement>> = LazyLock::new(|| {  vec![
Measurement { name: "c#", thread_count: 1, latency: 62 },
Measurement { name: "c#", thread_count: 2, latency: 65 },
Measurement { name: "c#", thread_count: 3, latency: 69 },
Measurement { name: "c#", thread_count: 4, latency: 70 },
Measurement { name: "c#", thread_count: 5, latency: 73 },
Measurement { name: "c#", thread_count: 6, latency: 77 },
Measurement { name: "c#", thread_count: 7, latency: 77 },
Measurement { name: "c#", thread_count: 8, latency: 83 },
Measurement { name: "c#", thread_count: 9, latency: 86 },
Measurement { name: "c#", thread_count: 10, latency: 86 },
Measurement { name: "c#", thread_count: 11, latency: 93 },
Measurement { name: "c#", thread_count: 12, latency: 94 },
Measurement { name: "c#", thread_count: 13, latency: 100 },
Measurement { name: "c#", thread_count: 14, latency: 102 },
Measurement { name: "c#", thread_count: 15, latency: 116 },
Measurement { name: "c#", thread_count: 16, latency: 113 },
] });
