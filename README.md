# map-bench-rust
This repository contains a benchmarking framework to compare the performance of various maps. This was useful for research into how NUMA architectures affect performance.

![Alt text for the SVG](latency99-numa.svg)

This chart shows results for a workload with 99% reads. The chart shows average latency. Lower is better.

- [bfix](https://github.com/ZacWalk/bfix-map) = my sharded simd probing hash map  
- c# = ConcurrentDictionary
- scc = shared map optimistic locking
- nop = no-operation to measure test framework overhead

The test has the following parameters:

- Initial items: 500,000 
- Operation count: 55,000,000 split over specified threads
- Operation types: 99% read, 1% upsert
- Hash function: ahash
- Hardware: HB120-64rs Azure VM with 64 vCPUs, 4 numa nodes, 456 GiB of RAM. The VM was running Windows.

There is also an equivalent testing framework [for dot-net]( https://github.com/ZacWalk/map-bench-dot-net).