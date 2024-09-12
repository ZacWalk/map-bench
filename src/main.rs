use perf_map::{Keys, Mix, RunConfig};
use plotters::prelude::SVGBackend;
use plotters::prelude::*;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;

mod map_adapters;
mod perf_map;
mod keys;
mod perf_mem;
mod perf_info;
mod perf_dotnet_data;
mod perf;

use crate::map_adapters::*;

// Enable this to use mimalloc
//#[global_allocator]
//static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {

    perf_info::print_cpu_info();

    // run_map_op_test(95);
    // run_map_op_test(100);

    // run_map_key_test();

    run_memory_read_write_test();
}

fn run_memory_read_write_test() 
{
    let mut measurements = Vec::new();

    for i in 1..perf_mem::get_num_cpus() + 1 {
        let mesurment = perf_mem::run_memory_access_test("normal", i, false, false);        
        println!(
            "Threads {}\tReads+Writes {}",
            mesurment.thread_count, mesurment.total
        );
        measurements.push(mesurment);
    }
    for i in 1..perf_mem::get_num_cpus() + 1 {
        let mesurment = perf_mem::run_memory_access_test("affinity", i, true, false);        
        println!(
            "Threads {}\tReads+Writes {}",
            mesurment.thread_count, mesurment.total
        );
        measurements.push(mesurment);
    }
    for i in 1..perf_mem::get_num_cpus() + 1 {
        let mesurment = perf_mem::run_memory_access_test("numa", i, true, true);        
        println!(
            "Threads {}\tReads+Writes {}",
            mesurment.thread_count, mesurment.total
        );
        measurements.push(mesurment);
    }

    write_plot(
        &measurements,
        "Memory Reads and Writes",
        "Average",
        "read-write.svg",
    ).expect("failed to plot");
}

// Uncomment the appropriate type alias for the key type
//type Key<'a> = keys::StrKey<'a>;
//type Key = keys::StringKey;
type Key = u64;

fn run_map_op_test(read_perc: i32) {
    let spec = if read_perc == 100 {
        Mix::read_only()
    } else {
        Mix::read_heavy()
    };

    let mix = spec.to_ops();
    let capacity = 1_000_000;
    let total_ops = capacity * 55;
    let prefill = capacity / 2;
    let keys_needed_for_inserts = (total_ops * spec.insert / 100) + 1;
    let total_keys = prefill + keys_needed_for_inserts + 1000; // 1000 needed for some rounding error?

    let mut measurements = if read_perc == 100 {
        Vec::new()
    } else {
        // values from C# test run
        perf_dotnet_data::PERF_DATA_DOT_NET_95.clone()
    };

    let keys = Arc::new(Keys::new(total_keys));

    for i in 0..perf_mem::get_num_cpus() {
        // Get the number of logical processors
        let config = RunConfig {
            threads: i + 1,
            total_ops,
            prefill,
        };

        let keys_needed_per_thread = keys_needed_for_inserts / config.threads;

        let scc = Arc::new(SccCollection::<Key, u64, ahash::RandomState>::with_capacity(capacity));
        measurements.push(perf_map::run_workload(
            &"scc",
            scc,
            mix.clone(),
            config,
            keys.clone(),
            keys_needed_per_thread,
        ));

        let bfix =
            Arc::new(BFixCollection::<Key, u64, ahash::RandomState>::with_capacity(capacity));
        measurements.push(perf_map::run_workload(
            &"bfix",
            bfix,
            mix.clone(),
            config,
            keys.clone(),
            keys_needed_per_thread,
        ));

        let ev = Arc::new(EvMapCollection::<Key, u64, ahash::RandomState>::with_capacity(capacity));
        measurements.push(perf_map::run_workload(
            &"ev",
            ev,
            mix.clone(),
            config,
            keys.clone(),
            keys_needed_per_thread,
        ));

        let ev = Arc::new(NopCollection::<Key, u64, ahash::RandomState>::with_capacity(capacity));
        measurements.push(perf_map::run_workload(
            &"nop",
            ev,
            mix.clone(),
            config,
            keys.clone(),
            keys_needed_per_thread,
        ));

        // Uncomment to run the std hashmap - super slow
        //let std = Arc::new(StdHashMapCollection::<u64, u64, ahash::RandomState>::with_capacity(capacity));
        //measurements.push(bench::run_workload2(&"std", std,mix.clone(), config, keys.clone()));
    }

    let csv_file_path = format!("latency{}.csv", read_perc);
    write_csv(&csv_file_path, &measurements);

    println!("Plotting");
    write_plot(
        &measurements,
        &format!("Latency (read = {}%)", read_perc),
        "Latency",
        &format!("latency{}.svg", read_perc),
    )
    .expect("failed to plot");
}

fn run_map_key_test() {
    let spec = Mix::read_heavy();
    let mix = spec.to_ops();
    let capacity = 1_000_000;
    let total_ops = capacity * 55;
    let prefill = capacity / 2;
    let keys_needed_for_inserts = (total_ops * spec.insert / 100) + 1;
    let total_keys = prefill + keys_needed_for_inserts + 1000; // 1000 needed for some rounding error?

    let mut measurements = Vec::new();

    let keys1 = Arc::new(Keys::new(total_keys));
    let keys2 = Arc::new(Keys::new(total_keys));

    for i in 0..perf_mem::get_num_cpus() {
        // Get the number of logical processors
        let config = RunConfig {
            threads: i + 1,
            total_ops,
            prefill,
        };

        let keys_needed_per_thread = keys_needed_for_inserts / config.threads;

        let scc1 = Arc::new(SccCollection::<u64, u64, ahash::RandomState>::with_capacity(capacity));
        measurements.push(perf_map::run_workload(
            &"scc u64",
            scc1,
            mix.clone(),
            config,
            keys1.clone(),
            keys_needed_per_thread,
        ));

        let scc2 = Arc::new(
            SccCollection::<keys::StrKey, u64, ahash::RandomState>::with_capacity(capacity),
        );
        measurements.push(perf_map::run_workload(
            &"scc str",
            scc2,
            mix.clone(),
            config,
            keys2.clone(),
            keys_needed_per_thread,
        ));

        let bfix1 =
            Arc::new(BFixCollection::<u64, u64, ahash::RandomState>::with_capacity(capacity));
        measurements.push(perf_map::run_workload(
            &"bfix u64",
            bfix1,
            mix.clone(),
            config,
            keys1.clone(),
            keys_needed_per_thread,
        ));

        let bfix2 = Arc::new(
            BFixCollection::<keys::StrKey, u64, ahash::RandomState>::with_capacity(capacity),
        );
        measurements.push(perf_map::run_workload(
            &"bfix str",
            bfix2,
            mix.clone(),
            config,
            keys2.clone(),
            keys_needed_per_thread,
        ));
    }

    write_csv("latency-keys.csv", &measurements);

    println!("Plotting");
    write_plot(&measurements, "Keys Latency", "Latency", "latency-keys.svg").expect("failed to plot");
}

fn write_csv(csv_file_path: &str, measurements: &Vec<perf::Measurement>) {
    let mut file = File::create(csv_file_path).expect("Failed to create CSV file");
    writeln!(file, "name,total_ops,threads,latency").expect("Failed to write CSV header");

    for m in measurements {
        let row = format!(
            "{},{},{}",
            m.name, m.thread_count, m.total
        );
        writeln!(file, "{}", row).expect("Failed to write CSV row");
    }
}

type Groups = BTreeMap<&'static str, Vec<perf::Measurement>>;

const FONT: &str = "Fira Code";
const PLOT_WIDTH: u32 = 800;
const PLOT_HEIGHT: u32 = 400;

pub fn write_plot(
    records: &Vec<perf::Measurement>,
    caption: &str,
    y_label: &str,
    path: &str,
) -> Result<(), Box<dyn Error>> {
    let mut groups = Groups::new();

    let mut color_map = HashMap::new();
    color_map.insert("bfix", BLUE);
    color_map.insert("c#", RED);
    color_map.insert("ev", GREEN);
    color_map.insert("scc", MAGENTA);
    color_map.insert("nop", CYAN);
    color_map.insert("scc u64", BLUE);
    color_map.insert("scc str", GREEN);
    color_map.insert("bfix u64", RED);
    color_map.insert("bfix str", MAGENTA);
    color_map.insert("normal", RED);
    color_map.insert("affinity", GREEN);
    color_map.insert("numa", BLUE);

    for record in records.iter() {
        let group = groups.entry(record.name).or_insert_with(Vec::new);
        group.push(record.clone());
    }

    let resolution = (PLOT_WIDTH, PLOT_HEIGHT);
    let root = SVGBackend::new(&path, resolution).into_drawing_area();

    root.fill(&WHITE)?;

    let (x_max, y_min, y_max) = groups
        .values()
        .flatten()
        .map(|record| (record.thread_count, record.total))
        .fold((0, u64::MAX, 0), |res, cur| (res.0.max(cur.0), res.1.min(cur.1), res.1.max(cur.1)));

    let mut chart = ChartBuilder::on(&root)
        .margin(10)
        .caption(caption, (FONT, 20))
        .set_label_area_size(LabelAreaPosition::Left, 70)
        .set_label_area_size(LabelAreaPosition::Right, 70)
        .set_label_area_size(LabelAreaPosition::Bottom, 40)
        .build_cartesian_2d(1..x_max, y_min - 10..y_max + 10)?;

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
                    .map(|record| (record.thread_count, record.total)),
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
