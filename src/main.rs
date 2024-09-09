use bench::Measurement;
use bench::{Keys, Mix, RunConfig};
use plotters::prelude::SVGBackend;
use plotters::prelude::*;
use plotters::style::RGBColor;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use std::collections::BTreeMap;

mod adapters;
mod bench;

use crate::adapters::*;

fn main() {
    run_perf_test(95);
    run_perf_test(100);
}

fn run_perf_test(read_perc: i32) {
    let spec = if read_perc == 100 {
        Mix::read_only()
    } else {
        Mix::read_heavy()
    };

    let mix = spec.to_ops();
    let capacity = 1_000_000;
    let total_ops = capacity * 55;
    let prefill = capacity / 2;
    let total_keys = prefill + (total_ops * spec.insert / 100) + 1000;

    let mut measurements = if read_perc == 100 {
        Vec::new()
    } else {
        vec![
            Measurement { name: "c#", total_ops: 55000000, thread_count: 1, latency: 110 },
            Measurement { name: "c#", total_ops: 55000000, thread_count: 2, latency: 123 },
            Measurement { name: "c#", total_ops: 54999999, thread_count: 3, latency: 143 },
            Measurement { name: "c#", total_ops: 55000000, thread_count: 4, latency: 158 },
            Measurement { name: "c#", total_ops: 55000000, thread_count: 5, latency: 167 },
            Measurement { name: "c#", total_ops: 54999996, thread_count: 6, latency: 191 },
            Measurement { name: "c#", total_ops: 54999994, thread_count: 7, latency: 210 },
            Measurement { name: "c#", total_ops: 55000000, thread_count: 8, latency: 238 },
            Measurement { name: "c#", total_ops: 54999999, thread_count: 9, latency: 250 },
            Measurement { name: "c#", total_ops: 55000000, thread_count: 10, latency: 288 },
            Measurement { name: "c#", total_ops: 55000000, thread_count: 11, latency: 279 },
            Measurement { name: "c#", total_ops: 54999996, thread_count: 12, latency: 307 },
            Measurement { name: "c#", total_ops: 54999997, thread_count: 13, latency: 306 },
            Measurement { name: "c#", total_ops: 54999994, thread_count: 14, latency: 324 },
            Measurement { name: "c#", total_ops: 54999990, thread_count: 15, latency: 353 },
            Measurement { name: "c#", total_ops: 55000000, thread_count: 16, latency: 367 },
        ]
    };

    let keys = Arc::new(Keys::new(total_keys));

    for i in 0..num_cpus::get() {
        // Get the number of logical processors
        let config = RunConfig {
            threads: i + 1,
            total_ops,
            prefill,
        };

        let scc = Arc::new(SccCollection::<u64, u64, ahash::RandomState>::with_capacity(capacity));
        measurements.push(bench::run_workload2(
            &"scc",
            scc,
            mix.clone(),
            config,
            keys.clone(),
        ));

        let bfix =
            Arc::new(BFixCollection::<u64, u64, ahash::RandomState>::with_capacity(capacity));
        measurements.push(bench::run_workload2(
            &"bfix",
            bfix,
            mix.clone(),
            config,
            keys.clone(),
        ));

        let ev = Arc::new(EvMapCollection::<u64, u64, ahash::RandomState>::with_capacity(capacity));
        measurements.push(bench::run_workload2(
            &"ev",
            ev,
            mix.clone(),
            config,
            keys.clone(),
        ));

        //let std = Arc::new(StdHashMapCollection::<u64, u64, ahash::RandomState>::with_capacity(capacity));
        //measurements.push(bench::run_workload2(&"std", std,mix.clone(), config, keys.clone()));
    }

    let csv_file_path = format!("latency{}.csv", read_perc);
    let mut file = File::create(csv_file_path).expect("Failed to create CSV file");
    writeln!(file, "name,total_ops,threads,latency").expect("Failed to write CSV header");

    for m in &measurements {
        let row = format!(
            "{},{},{},{}",
            m.name, m.total_ops, m.thread_count, m.latency
        );
        writeln!(file, "{}", row).expect("Failed to write CSV row");
    }

    println!("Plotting");
    plot(
        &measurements,
        &format!("Latency (read = {}%)", read_perc),
        &format!("latency{}.svg", read_perc),
    )
    .expect("failed to plot");
}

type Groups = BTreeMap<&'static str, Vec<bench::Measurement>>;

static COLORS: &[RGBColor] = &[BLUE, RED, GREEN, MAGENTA, CYAN, BLACK, YELLOW];
const FONT: &str = "Fira Code";
const PLOT_WIDTH: u32 = 800;
const PLOT_HEIGHT: u32 = 400;

pub fn plot(
    records: &Vec<bench::Measurement>,
    caption: &str,
    path: &str,
) -> Result<(), Box<dyn Error>> {
    let mut groups = Groups::new();

    let mut color_map = HashMap::new();
    color_map.insert("bfix", BLUE);
    color_map.insert("c#", RED);
    color_map.insert("ev", GREEN);
    color_map.insert("scc", MAGENTA);

    for record in records.iter() {
        let group = groups.entry(record.name).or_insert_with(Vec::new);
        group.push(record.clone());
    }

    let resolution = (PLOT_WIDTH, PLOT_HEIGHT);
    let root = SVGBackend::new(&path, resolution).into_drawing_area();

    root.fill(&WHITE)?;

    let (x_max, y_max) = groups
        .values()
        .flatten()
        .map(|record| (record.thread_count, record.latency))
        .fold((0, 0), |res, cur| (res.0.max(cur.0), res.1.max(cur.1)));

    let mut chart = ChartBuilder::on(&root)
        .margin(10)
        .caption(caption, (FONT, 20))
        .set_label_area_size(LabelAreaPosition::Left, 70)
        .set_label_area_size(LabelAreaPosition::Right, 70)
        .set_label_area_size(LabelAreaPosition::Bottom, 40)
        .build_cartesian_2d(1..x_max, 0..y_max)?;

    chart
        .configure_mesh()
        .disable_y_mesh()
        .x_label_formatter(&|v| format!("{}", v))
        .y_label_formatter(&|v| format!("{:.0} ns", v))
        .x_labels(20)
        .y_labels(20)
        .y_desc("Latency")
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
