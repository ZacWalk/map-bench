use perf::Measurement;
use perf_dotnet_data::PERF_DATA_DOT_NET_99_10k;
use perf_dotnet_data::PERF_DATA_DOT_NET_100_10K;
use perf_dotnet_data::PERF_DATA_DOT_NET_100_1M;
use perf_dotnet_data::PERF_DATA_DOT_NET_99_1M;
use perf_map::MapAdapter;
use perf_map::{Keys, Mix, SharedMapTestConfig};
use perf_mem::get_core_info;
use perf_mem::AffinityType;
use plotters::prelude::SVGBackend;
use plotters::prelude::*;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Instant;
use thousands::Separable;
use rand::Rng;

mod map_adapters;
mod perf;
mod perf_dotnet_data;
mod perf_info;
mod perf_map;
mod perf_mem;
mod sfix;

use crate::map_adapters::*;

// Enable this to use mimalloc
//#[global_allocator]
//static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    perf_info::write_cpu_info();

    run_map_op_test(Mix::read_100(), 1_000_000, &PERF_DATA_DOT_NET_100_1M);
    run_map_op_test(Mix::read_100(), 10_000, &PERF_DATA_DOT_NET_100_10K);
    run_map_op_test(Mix::read_99(), 1_000_000, &PERF_DATA_DOT_NET_99_1M);
    run_map_op_test(Mix::read_99(), 10_000, &PERF_DATA_DOT_NET_99_10k);
    run_map_key_test(Mix::read_99(), 1_000_000);
    run_map_key_test(Mix::read_99(), 10_000);
    run_map_key_test(Mix::read_100(), 100_000);
    run_map_test();
    run_mem_indirect_test();
    run_fetch_add_test();
    run_heapalloc_tests();
    run_memory_read_write_test();
}

fn run_memory_read_write_test() {
    let mut measurements1 = Vec::new();
    let mut measurements2 = Vec::new();
    let mut measurements3 = Vec::new();
    let mut measurements4 = Vec::new();
    let mut measurements5 = Vec::new();
    let num_cpus = perf_mem::get_num_cpus();

    const BIG_BLOCK_SIZE: usize = 8 * 1024 * 1024;
    const SMALL_BLOCK_SIZE: usize = 64 * 1024;

    for i in 1..=num_cpus {
        measurements1.push(perf_mem::run_independent_memory_access_test("normal", i, AffinityType::NoAffinity, false, BIG_BLOCK_SIZE));    
        measurements1.push(perf_mem::run_independent_memory_access_test("numa match", i, AffinityType::NumaNodeAffinity, false, BIG_BLOCK_SIZE));
        measurements1.push(perf_mem::run_independent_memory_access_test("numa miss", i, AffinityType::NumaMismatch, false, BIG_BLOCK_SIZE));

        measurements2.push(perf_mem::run_independent_memory_access_test("normal", i, AffinityType::NoAffinity, true, BIG_BLOCK_SIZE));    
        measurements2.push(perf_mem::run_independent_memory_access_test("numa match", i, AffinityType::NumaNodeAffinity, true, BIG_BLOCK_SIZE));
        measurements2.push(perf_mem::run_independent_memory_access_test("numa miss", i, AffinityType::NumaMismatch, true, BIG_BLOCK_SIZE));

        measurements3.push(perf_mem::run_independent_memory_access_test("normal", i, AffinityType::NoAffinity, false, SMALL_BLOCK_SIZE));    
        measurements3.push(perf_mem::run_independent_memory_access_test("numa match", i, AffinityType::NumaNodeAffinity, false, SMALL_BLOCK_SIZE));
        measurements3.push(perf_mem::run_independent_memory_access_test("numa miss", i, AffinityType::NumaMismatch, false, SMALL_BLOCK_SIZE));

        measurements4.push(perf_mem::run_independent_memory_access_test("normal", i, AffinityType::NoAffinity, true, SMALL_BLOCK_SIZE));    
        measurements4.push(perf_mem::run_independent_memory_access_test("numa match", i, AffinityType::NumaNodeAffinity, true, SMALL_BLOCK_SIZE));
        measurements4.push(perf_mem::run_independent_memory_access_test("numa miss", i, AffinityType::NumaMismatch, true, SMALL_BLOCK_SIZE));

        measurements5.push(perf_mem::run_independent_memory_access_test("64k", i, AffinityType::NoAffinity, false, SMALL_BLOCK_SIZE));    
        measurements5.push(perf_mem::run_independent_memory_access_test("8mb", i, AffinityType::NoAffinity, false, BIG_BLOCK_SIZE)); 
    }

    write_plot(
        &measurements1,
        "Independent Memory Reads and Writes (8MB blocks)",
        "Average", "Threads",
        "memory-8mb-read-write.svg",
    )
    .expect("failed to plot");

    write_plot(
        &measurements2,
        "Independent Memory Reads (8MB blocks)",
        "Average", "Threads",
        "memory-8mb-read.svg",
    )
    .expect("failed to plot");

    write_plot(
        &measurements3,
        "Independent Memory Reads and Writes (64k blocks)",
        "Average", "Threads",
        "memory-64k-read-write.svg",
    )
    .expect("failed to plot");

    write_plot(
        &measurements4,
        "Independent Memory Reads (64k blocks)",
        "Average", "Threads",
        "memory-64k-read.svg",
    )
    .expect("failed to plot");

    write_plot(
        &measurements5,
        "Independent Memory Reads and Writes (64k vs 8mb)",
        "Average", "Threads",
        "memory-64k-8mb.svg",
    )
    .expect("failed to plot");
}

fn run_heapalloc_tests()
{
    let mut measurements = Vec::new();
    let num_cpus = perf_mem::get_num_cpus();

    for i in 1..=num_cpus {
        measurements.push(perf_mem::run_heapalloc_test("1 heap", i, 1));
        measurements.push(perf_mem::run_heapalloc_test("4 heaps", i, 4));
        measurements.push(perf_mem::run_heapalloc_test("16 heaps", i, 16));
    }

    write_plot(
        &measurements,
        "heap contention",
        "Average", "Threads",
        "memory-allocators.svg",
    )
    .expect("failed to plot");
}

fn run_fetch_add_test() {
    let mut measurements1 = Vec::new();
    let mut measurements2 = Vec::new();
    let num_cpus = perf_mem::get_num_cpus();
    let core_info = get_core_info().expect("Failed to get core IDs");

    for i in 1..=num_cpus {
        measurements2.push(perf_mem::run_fetch_add_test("atomic", i, 1));
        measurements2.push(perf_mem::run_mutex_test("mutex", i, 1));

        measurements1.push(perf_mem::run_fetch_add_test("std alloc", i, core_info.num_numa_nodes));
        measurements1.push(perf_mem::run_numa_fetch_add_test("numa aff", i, true));
        measurements1.push(perf_mem::run_numa_fetch_add_test("core aff", i, false));
    }

    write_plot(
        &measurements1,
        "Counter per Numa node (counter per numa node)",
        "Average", "Threads",
        "memory-counter-atomic.svg",
    )
    .expect("failed to plot");

    write_plot(
        &measurements2,
        "Global Counter (Mutex vs Atomic)",
        "Average", "Threads",
        "memory-counter-mutex.svg",
    )
    .expect("failed to plot");
}


fn run_map_op_test(spec: Mix, num_start_items : usize, dot_net : &Vec<Measurement>) {
    let operations = spec.to_ops();    
    let total_ops = 40_000_000;
    let prefill = num_start_items;
    let expected_inserts = total_ops * spec.insert / 100;
    let capacity = num_start_items + expected_inserts;
    let total_keys = prefill + expected_inserts + 1000; // 1000 needed for some rounding error?

    let mut measurements = dot_net.clone();

    let keys = Arc::new(Keys::new(total_keys));

    for i in 0..perf_mem::get_num_cpus() {
        let thread_count = i + 1;
        let keys_needed_per_thread = expected_inserts / thread_count;

        // Get the number of logical processors
        let config = SharedMapTestConfig {
            thread_count: i + 1,
            total_ops,
            operations: &operations,
            keys_needed_per_thread,
            prefill,
        };

        let m = Arc::new(SccCollection::<u64, u64, ahash::RandomState>::with_capacity(capacity));
        measurements.push(perf_map::run_shared_map_test(&"scc", m, &config, &keys));

        let m =
            Arc::new(BFixCollection::<u64, u64, ahash::RandomState>::with_capacity(capacity));
        measurements.push(perf_map::run_shared_map_test(&"bfix", m, &config, &keys));

        // let m =
        //     Arc::new(StdHashMapCollection::<u64, u64, ahash::RandomState>::with_capacity(capacity));
        // measurements.push(perf_map::run_workload(&"std", m, &config, &keys));

        let m =
            Arc::new(NopCollection::<u64, u64, ahash::RandomState>::with_capacity(capacity));
        measurements.push(perf_map::run_shared_map_test(&"nop", m, &config, &keys));
    }

    write_plot(
        &measurements,
        &format!("Average latency (read = {}%   items = {}+{})", spec.read, prefill.separate_with_commas(), expected_inserts.separate_with_commas()),
        "Latency", "Threads",
        &format!("latency{}-{}.svg", spec.read, num_start_items),
    )
    .expect("failed to plot");
}

type DefaultHashBuilder = core::hash::BuildHasherDefault<ahash::AHasher>;

impl<K, V> MapAdapter<K, V> for std::collections::HashMap<K, V, DefaultHashBuilder>
where
    K: Eq + std::hash::Hash,
    V : Clone,
{
    fn insert(&mut self, key: K, value: V) {
        std::collections::HashMap::insert(self, key, value);
    }

    fn get(&self, key: &K) -> Option<V> {
        std::collections::HashMap::get(self, key).cloned()
    }
}

impl<K, V> MapAdapter<K, V> for hashbrown::HashMap<K, V, DefaultHashBuilder>
where
    K: Eq + std::hash::Hash,
    V : Clone,
{
    fn insert(&mut self, key: K, value: V) {
        hashbrown::HashMap::insert(self, key, value);
    }

    fn get(&self, key: &K) -> Option<V> {
        hashbrown::HashMap::get(self, key).cloned()
    }
}

impl<K, V> MapAdapter<K, V> for bfixmap::BFixMap<K, V, DefaultHashBuilder>
where
    K: std::hash::Hash + Eq + Default,
    V: Default + Copy,
{
    fn insert(&mut self, key: K, value: V) {
        bfixmap::BFixMap::insert(self, key, value);
    }

    fn get(&self, key: &K) -> Option<V> {
        self.get(&key, |v| *v)
    }
}


impl<K, V> MapAdapter<K, V> for sfix::SFixMap<K, V, DefaultHashBuilder>
where
    K: std::hash::Hash + Eq + Default,
    V: Default + Copy,
{
    fn insert(&mut self, key: K, value: V) {
        sfix::SFixMap::insert(self, key, value);
    }

    fn get(&self, key: &K) -> Option<V> {
        sfix::SFixMap::get(self, &key).cloned()
    }
}

fn run_map_test() {

    let mut measurements = Vec::new();

    for i in 0..20 {

        let prefill = (i + 1) * 5000;

        let mut std_map: HashMap<u64, u64, DefaultHashBuilder> = HashMap::with_capacity_and_hasher(prefill, DefaultHashBuilder::default());
        measurements.push(perf_map::run_map_test("std", 10_000_000, prefill, &mut std_map));

        let mut hashbrown_map: hashbrown::HashMap<u64, u64, DefaultHashBuilder> = hashbrown::HashMap::with_capacity_and_hasher(prefill, DefaultHashBuilder::default());        
        measurements.push(perf_map::run_map_test("hb", 10_000_000, prefill, &mut hashbrown_map));

        let mut bfix_map: bfixmap::BFixMap<u64, u64, DefaultHashBuilder> = bfixmap::BFixMap::with_capacity_and_hasher(prefill, DefaultHashBuilder::default());        
        measurements.push(perf_map::run_map_test("bfix", 10_000_000, prefill, &mut bfix_map));

        let mut sfix_map: sfix::SFixMap<u64, u64, DefaultHashBuilder> = sfix::SFixMap::with_capacity_and_hasher(prefill, DefaultHashBuilder::default());        
        measurements.push(perf_map::run_map_test("sfix", 10_000_000, prefill, &mut sfix_map));
    }

    write_plot(
        &measurements,
        &"Non-shared maps (Average latency)",
        &"Latency", "K items",
        &format!("maps.svg"),
    )
    .expect("failed to plot");
}

fn run_mem_indirect_test() {

    let mut measurements = Vec::new();
    let mut rng = rand::thread_rng();

    for i in 1..100 {
        let size = (i * 1024 * 1024) / 4;

        let mut vec1 = Vec::with_capacity(size);
        let mut vec2 = Vec::with_capacity(size);
        let mut vec3 = Vec::with_capacity(size);
        let mut vec4 = Vec::with_capacity(size);

        // Initialize vectors with random elements
        for _ in 0..size {
            vec1.push(rng.gen_range(0..size));
            vec2.push(rng.gen_range(0..size));
            vec3.push(rng.gen_range(0..size));
            vec4.push(rng.gen_range(0..size));
        }

        const OP_COUNT: usize = 1_000_000;
        let start_time = Instant::now();

        for i in 0..OP_COUNT {
            let j = vec1[i % size];
            let j = vec2[j];
            let j = vec3[j];
            let j = vec4[j];
            std::hint::black_box(j);
        }

        let elapsed = start_time.elapsed();
        let average_duration = elapsed.as_nanos() as f64 / OP_COUNT as f64;

        let m = Measurement {
            name: "4",
            latency: average_duration,
            thread_count: i as u64,
        };

        measurements.push(m);

        let start_time = Instant::now();

        for i in 0..OP_COUNT {
            let j = vec1[i % size];
            let j = vec2[j];
            let j = vec3[j];
            std::hint::black_box(j);
        }

        let elapsed = start_time.elapsed();
        let average_duration = elapsed.as_nanos() as f64 / OP_COUNT as f64;

        let m = Measurement {
            name: "3",
            latency: average_duration,
            thread_count: i as u64,
        };

        measurements.push(m);

        let start_time = Instant::now();

        for i in 0..OP_COUNT {
            let j = vec1[i % size];
            let j = vec2[j];
            std::hint::black_box(j);
        }

        let elapsed = start_time.elapsed();
        let average_duration = elapsed.as_nanos() as f64 / OP_COUNT as f64;

        let m = Measurement {
            name: "2",
            latency: average_duration,
            thread_count: i as u64,
        };

        measurements.push(m);

        println!(
            "Size: {}MB, Average latency: {:.2} ns",
            i, average_duration
        );
    }

    write_plot(
        &measurements,
        &"Indirect memory access (MB blocks)",
        &"Latency", "MB block size",
        &format!("mem-indirect.svg"),
    )
    .expect("failed to plot");
}

fn run_map_key_test(spec: Mix, num_start_items : usize) {
    let operations = spec.to_ops();
    let total_ops = 40_000_000;
    let prefill = num_start_items;
    let expected_inserts = total_ops * spec.insert / 100;
    let capacity = num_start_items + expected_inserts;
    let total_keys = prefill + expected_inserts + 1000; // 1000 needed for some rounding error?

    let mut measurements = Vec::new();

    let keys1 = Arc::new(Keys::new(total_keys));
    let keys2 = Arc::new(Keys::new(total_keys));

    for i in 0..perf_mem::get_num_cpus() {
        let thread_count = i + 1;
        let keys_needed_per_thread = expected_inserts / thread_count;

        let config = SharedMapTestConfig {
            thread_count: i + 1,
            total_ops,
            operations: &operations,
            keys_needed_per_thread,
            prefill,
        };

        let scc1 = Arc::new(SccCollection::<u64, u64, ahash::RandomState>::with_capacity(capacity));
        measurements.push(perf_map::run_shared_map_test(&"scc u64", scc1, &config, &keys1));

        let scc2 = Arc::new(
            SccCollection::<String, u64, ahash::RandomState>::with_capacity(capacity),
        );
        measurements.push(perf_map::run_shared_map_test(&"scc str", scc2, &config, &keys2));

        let bfix1 =
            Arc::new(BFixCollection::<u64, u64, ahash::RandomState>::with_capacity(capacity));
        measurements.push(perf_map::run_shared_map_test(&"bfix u64", bfix1, &config, &keys1));

        let bfix2 = Arc::new(
            BFixCollection::<String, u64, ahash::RandomState>::with_capacity(capacity),
        );
        measurements.push(perf_map::run_shared_map_test(&"bfix str", bfix2, &config, &keys2));
    }

    write_plot(
        &measurements,
        &format!("String vs u64 keys latency (read = {}%   items = {}+{})", spec.read, prefill.separate_with_commas(), expected_inserts.separate_with_commas()),
        "Latency", "Threads",
        &format!("keys{}-{}.svg", spec.read, num_start_items),
    )
    .expect("failed to plot");
}

const FONT: &str = "Fira Code";
const PLOT_WIDTH: u32 = 800;
const PLOT_HEIGHT: u32 = 400;

pub fn write_plot(
    records: &Vec<perf::Measurement>,
    caption: &str,
    y_label: &str,
    x_label: &str,
    path: &str,
) -> Result<(), Box<dyn Error>> {
    let mut groups: BTreeMap<&str, Vec<&perf::Measurement>> = BTreeMap::new();

    let mut color_map = HashMap::new();
    color_map.insert("bfix", GREEN);
    color_map.insert("c#", RED);
    color_map.insert("ev", GREEN);
    color_map.insert("scc", BLUE);
    color_map.insert("nop", CYAN);
    color_map.insert("std", MAGENTA);
    color_map.insert("hb", BLUE);
    color_map.insert("sfix", RED);
    color_map.insert("scc u64", RGBColor(10, 10, 240));
    color_map.insert("scc str", RGBColor(10, 10, 180));
    color_map.insert("bfix u64", RGBColor(10, 240, 10));
    color_map.insert("bfix str", RGBColor(10, 180, 10));
    color_map.insert("normal", RED);
    color_map.insert("numa match", GREEN);
    color_map.insert("numa miss", BLUE);
    color_map.insert("std alloc", RED);
    color_map.insert("atomic", RED);
    color_map.insert("numa aff", BLUE);
    color_map.insert("mutex", BLUE);
    color_map.insert("core aff", MAGENTA);
    color_map.insert("64k", RED);
    color_map.insert("8mb", BLUE);
    color_map.insert("1", BLUE);
    color_map.insert("2", GREEN);
    color_map.insert("3", RED);
    color_map.insert("4", MAGENTA);
    color_map.insert("1 heap", BLUE);
    color_map.insert("4 heaps", GREEN);
    color_map.insert("16 heaps", RED);

    for record in records.iter() {
        let group = groups.entry(record.name).or_insert_with(Vec::new);
        group.push(&record);
    }

    let resolution = (PLOT_WIDTH, PLOT_HEIGHT);
    let root = SVGBackend::new(&path, resolution).into_drawing_area();

    root.fill(&WHITE)?;

    
    let y_min = records.iter().map(|m| m.latency).fold(f64::INFINITY, f64::min);
    let y_max = records.iter().map(|m| m.latency).fold(f64::NEG_INFINITY, f64::max);
    let y_diff = y_max - y_min;
    let y_padding = (y_diff / 10.0).min(y_min);

    let x_min = records.iter().map(|m| m.thread_count).min().unwrap();
    let x_max = records.iter().map(|m| m.thread_count).max().unwrap();

    
    let mut chart = ChartBuilder::on(&root)
        .margin(10)
        .caption(caption, (FONT, 20))
        .set_label_area_size(LabelAreaPosition::Left, 70)
        .set_label_area_size(LabelAreaPosition::Right, 70)
        .set_label_area_size(LabelAreaPosition::Bottom, 40)
        .build_cartesian_2d(1..x_max, y_min - y_padding..y_max + y_padding)?;

    chart
        .configure_mesh()
        .disable_y_mesh()
        .x_label_formatter(&|v| format!("{}", v))
        .y_label_formatter(&|v| format!("{:.0} ns", v))
        .x_labels(20)
        .y_labels(20)
        .y_desc(y_label)
        .x_desc(x_label)
        .draw()?;

    for records in groups.values() {
        let color = color_map.get(records[0].name).unwrap();
        chart
            .draw_series(LineSeries::new(
                records
                    .iter()
                    .map(|record| (record.thread_count, record.latency)),
                color,
            ))?
            .label(records[0].name)
            .legend(move |(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], color));
    }

    chart
        .configure_series_labels()
        .position(SeriesLabelPosition::UpperLeft)
        .label_font((FONT, 13))
        .background_style(WHITE.mix(0.8))
        .border_style(BLACK)
        .draw()?;

    Ok(())
}

