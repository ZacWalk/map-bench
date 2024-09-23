use perf::Measurement;
use perf_dotnet_data::PERF_DATA_DOT_NET_99_10k;
use perf_dotnet_data::PERF_DATA_DOT_NET_100_10K;
use perf_dotnet_data::PERF_DATA_DOT_NET_100_1M;
use perf_dotnet_data::PERF_DATA_DOT_NET_99_1M;
use perf_map::{Keys, Mix, RunConfig};
use perf_mem::get_core_info;
use perf_mem::AffinityType;
use plotters::prelude::SVGBackend;
use plotters::prelude::*;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use thousands::Separable;

mod map_adapters;
mod perf;
mod perf_dotnet_data;
mod perf_info;
mod perf_map;
mod perf_mem;

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
    run_fetch_add_test();
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

    for i in 1..num_cpus + 1 {
        measurements1.push(perf_mem::run_memory_access_test("normal", i, AffinityType::NoAffinity, false, BIG_BLOCK_SIZE));    
        measurements1.push(perf_mem::run_memory_access_test("numa match", i, AffinityType::NumaNodeAffinity, false, BIG_BLOCK_SIZE));
        measurements1.push(perf_mem::run_memory_access_test("numa miss", i, AffinityType::NumaMismatch, false, BIG_BLOCK_SIZE));

        measurements2.push(perf_mem::run_memory_access_test("normal", i, AffinityType::NoAffinity, true, BIG_BLOCK_SIZE));    
        measurements2.push(perf_mem::run_memory_access_test("numa match", i, AffinityType::NumaNodeAffinity, true, BIG_BLOCK_SIZE));
        measurements2.push(perf_mem::run_memory_access_test("numa miss", i, AffinityType::NumaMismatch, true, BIG_BLOCK_SIZE));

        measurements3.push(perf_mem::run_memory_access_test("normal", i, AffinityType::NoAffinity, false, SMALL_BLOCK_SIZE));    
        measurements3.push(perf_mem::run_memory_access_test("numa match", i, AffinityType::NumaNodeAffinity, false, SMALL_BLOCK_SIZE));
        measurements3.push(perf_mem::run_memory_access_test("numa miss", i, AffinityType::NumaMismatch, false, SMALL_BLOCK_SIZE));

        measurements4.push(perf_mem::run_memory_access_test("normal", i, AffinityType::NoAffinity, true, SMALL_BLOCK_SIZE));    
        measurements4.push(perf_mem::run_memory_access_test("numa match", i, AffinityType::NumaNodeAffinity, true, SMALL_BLOCK_SIZE));
        measurements4.push(perf_mem::run_memory_access_test("numa miss", i, AffinityType::NumaMismatch, true, SMALL_BLOCK_SIZE));

        measurements5.push(perf_mem::run_memory_access_test("64k", i, AffinityType::NoAffinity, false, SMALL_BLOCK_SIZE));    
        measurements5.push(perf_mem::run_memory_access_test("8mb", i, AffinityType::NoAffinity, false, BIG_BLOCK_SIZE)); 
    }

    write_plot(
        &measurements1,
        "Memory Reads and Writes (8MB blocks)",
        "Average",
        "memory-8mb-read-write.svg",
    )
    .expect("failed to plot");

    write_plot(
        &measurements2,
        "Memory Reads (8MB blocks)",
        "Average",
        "memory-8mb-read.svg",
    )
    .expect("failed to plot");

    write_plot(
        &measurements3,
        "Memory Reads and Writes (64k blocks)",
        "Average",
        "memory-64k-read-write.svg",
    )
    .expect("failed to plot");

    write_plot(
        &measurements4,
        "Memory Reads (64k blocks)",
        "Average",
        "memory-64k-read.svg",
    )
    .expect("failed to plot");

    write_plot(
        &measurements5,
        "Memory Reads and Writes (64k vs 8mb)",
        "Average",
        "memory-64k-8mb.svg",
    )
    .expect("failed to plot");
}

fn run_fetch_add_test() {
    let mut measurements1 = Vec::new();
    let mut measurements2 = Vec::new();
    let num_cpus = perf_mem::get_num_cpus();
    let core_info = get_core_info().expect("Failed to get core IDs");

    for i in 1..num_cpus + 1 {
        measurements2.push(perf_mem::run_fetch_add_test("atomic", i, 1));
        measurements2.push(perf_mem::run_mutex_test("mutex", i, 1));

        measurements1.push(perf_mem::run_fetch_add_test("std alloc", i, core_info.num_numa_nodes));
        measurements1.push(perf_mem::run_numa_fetch_add_test("numa aff", i, true));
        measurements1.push(perf_mem::run_numa_fetch_add_test("core aff", i, false));
    }

    write_plot(
        &measurements1,
        "Counter per Numa node (counter per numa node)",
        "Average",
        "memory-counter-atomic.svg",
    )
    .expect("failed to plot");

    write_plot(
        &measurements2,
        "Global Counter (Mutex vs Atomic)",
        "Average",
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
        let config = RunConfig {
            thread_count: i + 1,
            total_ops,
            operations: &operations,
            keys_needed_per_thread,
            prefill,
        };

        let m = Arc::new(SccCollection::<u64, u64, ahash::RandomState>::with_capacity(capacity));
        measurements.push(perf_map::run_workload(&"scc", m, &config, &keys));

        let m =
            Arc::new(BFixCollection::<u64, u64, ahash::RandomState>::with_capacity(capacity));
        measurements.push(perf_map::run_workload(&"bfix", m, &config, &keys));

        // let m =
        //     Arc::new(StdHashMapCollection::<u64, u64, ahash::RandomState>::with_capacity(capacity));
        // measurements.push(perf_map::run_workload(&"std", m, &config, &keys));

        let m =
            Arc::new(NopCollection::<u64, u64, ahash::RandomState>::with_capacity(capacity));
        measurements.push(perf_map::run_workload(&"nop", m, &config, &keys));
    }

    write_plot(
        &measurements,
        &format!("Average latency (read = {}%   items = {}+{})", spec.read, prefill.separate_with_commas(), expected_inserts.separate_with_commas()),
        "Latency",
        &format!("latency{}-{}.svg", spec.read, num_start_items),
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

        let config = RunConfig {
            thread_count: i + 1,
            total_ops,
            operations: &operations,
            keys_needed_per_thread,
            prefill,
        };

        let scc1 = Arc::new(SccCollection::<u64, u64, ahash::RandomState>::with_capacity(capacity));
        measurements.push(perf_map::run_workload(&"scc u64", scc1, &config, &keys1));

        let scc2 = Arc::new(
            SccCollection::<String, u64, ahash::RandomState>::with_capacity(capacity),
        );
        measurements.push(perf_map::run_workload(&"scc str", scc2, &config, &keys2));

        let bfix1 =
            Arc::new(BFixCollection::<u64, u64, ahash::RandomState>::with_capacity(capacity));
        measurements.push(perf_map::run_workload(&"bfix u64", bfix1, &config, &keys1));

        let bfix2 = Arc::new(
            BFixCollection::<String, u64, ahash::RandomState>::with_capacity(capacity),
        );
        measurements.push(perf_map::run_workload(&"bfix str", bfix2, &config, &keys2));
    }

    write_plot(
        &measurements,
        &format!("String vs u64 keys latency (read = {}%   items = {}+{})", spec.read, prefill.separate_with_commas(), expected_inserts.separate_with_commas()),
        "Latency",
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

    for record in records.iter() {
        let group = groups.entry(record.name).or_insert_with(Vec::new);
        group.push(&record);
    }

    let resolution = (PLOT_WIDTH, PLOT_HEIGHT);
    let root = SVGBackend::new(&path, resolution).into_drawing_area();

    root.fill(&WHITE)?;

    
    let y_min = records.iter().map(|m| m.latency).min().unwrap();
    let y_max = records.iter().map(|m| m.latency).max().unwrap();
    let y_diff = y_max - y_min;
    let y_padding = (y_diff / 10).min(y_min);

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
        .x_desc("Threads")
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

