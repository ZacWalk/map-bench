use bench::Measurement;
use bench::{Keys, Mix, RunConfig};
use plotters::prelude::SVGBackend;
use plotters::prelude::*;
use plotters::style::RGBColor;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::sync::Arc;

mod adapters;
mod bench;

use crate::adapters::*;

//#[global_allocator]
//static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    run_perf_test(95);
    run_perf_test(100);
}

#[derive(Clone, Default)]
struct StringKey(String);

impl From<u64> for StringKey {
    fn from(num: u64) -> Self {
        // Your conversion logic here
        StringKey(num.to_string())
    }
}

impl PartialEq for StringKey {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for StringKey {}

impl Hash for StringKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl PartialOrd for StringKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0) // Compare the underlying strings
    }
}

impl Ord for StringKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0) // Compare the underlying strings
    }

    fn max(self, other: Self) -> Self
    where
        Self: Sized,
    {
        std::cmp::max_by(self, other, Ord::cmp)
    }

    fn min(self, other: Self) -> Self
    where
        Self: Sized,
    {
        std::cmp::min_by(self, other, Ord::cmp)
    }

    fn clamp(self, min: Self, max: Self) -> Self
    where
        Self: Sized,
    {
        assert!(min <= max);
        if self < min {
            min
        } else if self > max {
            max
        } else {
            self
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct StrKey<'a>(&'a str); 

impl<'a> From<u64> for StrKey<'a> {
    fn from(num: u64) -> Self {
        // Convert u64 to string and leak it to get a static str
        let s: &'static str = Box::leak(format!("{}", num).into_boxed_str());
        StrKey(s)
    }
}

impl<'a> Default for StrKey<'a> {
    fn default() -> Self {
        // You need a 'static str for the default value
        static DEFAULT_STR: &str = "";
        StrKey(DEFAULT_STR)
    }
}

type Key<'a> = StrKey<'a>;
//type Key = StringKey;
//type Key = u64;

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
    let keys_needed_for_inserts = (total_ops * spec.insert / 100) + 1;
    let total_keys = prefill + keys_needed_for_inserts + 1000; // 1000 needed for some rounding error?

    let mut measurements = if read_perc == 100 {
        Vec::new()
    } else {
        vec![
            Measurement { name: "c#", total_ops: 55000000, thread_count: 1, latency: 173 },
            Measurement { name: "c#", total_ops: 55000000, thread_count: 2, latency: 197 },
            Measurement { name: "c#", total_ops: 54999999, thread_count: 3, latency: 214 },
            Measurement { name: "c#", total_ops: 55000000, thread_count: 4, latency: 253 },
            Measurement { name: "c#", total_ops: 55000000, thread_count: 5, latency: 269 },
            Measurement { name: "c#", total_ops: 54999996, thread_count: 6, latency: 301 },
            Measurement { name: "c#", total_ops: 54999994, thread_count: 7, latency: 328 },
            Measurement { name: "c#", total_ops: 55000000, thread_count: 8, latency: 349 },
            Measurement { name: "c#", total_ops: 54999999, thread_count: 9, latency: 361 },
            Measurement { name: "c#", total_ops: 55000000, thread_count: 10, latency: 398 },
            Measurement { name: "c#", total_ops: 55000000, thread_count: 11, latency: 374 },
            Measurement { name: "c#", total_ops: 54999996, thread_count: 12, latency: 416 },
            Measurement { name: "c#", total_ops: 54999997, thread_count: 13, latency: 446 },
            Measurement { name: "c#", total_ops: 54999994, thread_count: 14, latency: 398 },
            Measurement { name: "c#", total_ops: 54999990, thread_count: 15, latency: 435 },
            Measurement { name: "c#", total_ops: 55000000, thread_count: 16, latency: 477 },
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

        let keys_needed_per_thread = keys_needed_for_inserts / config.threads;

        let scc = Arc::new(SccCollection::<Key, u64, ahash::RandomState>::with_capacity(capacity));
        measurements.push(bench::run_workload(
            &"scc",
            scc,
            mix.clone(),
            config,
            keys.clone(),
            keys_needed_per_thread,
        ));

        let bfix =
            Arc::new(BFixCollection::<Key, u64, ahash::RandomState>::with_capacity(capacity));
        measurements.push(bench::run_workload(
            &"bfix",
            bfix,
            mix.clone(),
            config,
            keys.clone(),
            keys_needed_per_thread,
        ));

        let ev = Arc::new(
            EvMapCollection::<Key, u64, ahash::RandomState>::with_capacity(capacity),
        );
        measurements.push(bench::run_workload(
            &"ev",
            ev,
            mix.clone(),
            config,
            keys.clone(),
            keys_needed_per_thread,
        ));

        let ev =
            Arc::new(NopCollection::<Key, u64, ahash::RandomState>::with_capacity(capacity));
        measurements.push(bench::run_workload(
            &"nop",
            ev,
            mix.clone(),
            config,
            keys.clone(),
            keys_needed_per_thread,
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
    color_map.insert("nop", CYAN);

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
