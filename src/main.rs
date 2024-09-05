use plotters::prelude::SVGBackend;
use plotters::prelude::*;
use plotters::style::RGBColor;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::hash::RandomState;
use std::time::Duration;

mod adapters;
mod bench;

use crate::adapters::*;

type Groups = HashMap<String, Vec<Record>>;

fn main() {
    // Open the file for writing

    fun_name(bench::Mix::read_only(), 100);
    fun_name(bench::Mix::read_heavy(), 95);
}

fn fun_name(mix: bench::Mix, read_perc : i32) {
    let mut records = Vec::<Record>::new();
    let mut records_aff = Vec::<Record>::new();
    let run_checks = false;

    for num_threads in 1..=16 {
        let workload = bench::Workload::new(num_threads);

        // let std = bench::run_workload::<StdHashMapCollection<u64, u64, RandomState>>(
        //         "std", mix, workload, run_checks, false,
        //     );

        let ev = bench::run_workload::<EvMapCollection<u64, u64, RandomState>>(
            "ev", mix, workload, run_checks, false,
        );

        let ev_aff = bench::run_workload::<EvMapCollection<u64, u64, RandomState>>(
            "ev aff", mix, workload, run_checks, true,
        );

        let scc = bench::run_workload::<SccCollection<u64, u64, RandomState>>(
            "scc", mix, workload, run_checks, false,
        );

        let scc_aff = bench::run_workload::<SccCollection<u64, u64, RandomState>>(
            "scc aff", mix, workload, run_checks, true,
        );

        let bfix = bench::run_workload::<BFixCollection<u64, u64, RandomState>>(
            "bfix", mix, workload, run_checks, false,
        );
    
        let bfix_aff = bench::run_workload::<BFixCollection<u64, u64, RandomState>>(
            "bfix aff", mix, workload, run_checks, true,
        );        

        //record(&mut records, "std", num_threads, std);
        record(&mut records, "bfix", num_threads, bfix.clone());
        record(&mut records, "scc", num_threads, scc.clone());
        record(&mut records, "ev", num_threads, ev.clone());

        record(&mut records_aff, "scc aff", num_threads, scc_aff);
        record(&mut records_aff, "scc", num_threads, scc);

        record(&mut records_aff, "ev aff", num_threads, ev_aff);
        record(&mut records_aff, "ev", num_threads, ev);

        record(&mut records_aff, "bfix aff", num_threads, bfix_aff);
        record(&mut records_aff, "bfix", num_threads, bfix);        
    }


    write_csv(&records, &format!("latency{}.csv", read_perc)).expect("failed to write csv");
    write_csv(&records_aff, &format!("affinity{}.csv", read_perc)).expect("failed to write csv");

    println!("Plotting");
    plot(&records,&format!("Latency (read = {}%)", read_perc),  &format!("latency{}.svg", read_perc)).expect("failed to plot");
    plot(&records_aff,&format!("Affinity (read = {}%)", read_perc),  &format!("affinity{}.svg", read_perc)).expect("failed to plot");
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct Record {
    pub name: String,
    pub total_ops: u64,
    pub threads: usize,
    #[serde(with = "timestamp")]
    pub spent: Duration,
    pub throughput: f64,
    #[serde(with = "timestamp")]
    pub latency: Duration,
}

mod timestamp {
    use super::*;

    use serde::{de::Deserializer, ser::Serializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        u64::deserialize(deserializer).map(Duration::from_nanos)
    }

    pub fn serialize<S>(value: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        (value.as_nanos() as u64).serialize(serializer)
    }
}

static COLORS: &[RGBColor] = &[BLUE, RED, GREEN, MAGENTA, CYAN, BLACK, YELLOW];
const FONT: &str = "Fira Code";
const PLOT_WIDTH: u32 = 800;
const PLOT_HEIGHT: u32 = 400;

fn record(records: &mut Vec<Record>, name: &str, num_threads: usize, m: bench::Measurement) {
    let record = Record {
        name: name.into(),
        total_ops: m.total_ops,
        threads: num_threads,
        spent: m.spent,
        latency: m.latency,
        throughput: m.throughput,
    };

    records.push(record);
}

fn write_csv(records: &Vec<Record>, name : &str) -> Result<(), Box<dyn Error>> {
    let file = File::create(name).expect("Failed to create file");
    let mut wr = csv::WriterBuilder::new().from_writer(file);

    for record in records.iter() {
        wr.serialize(record).expect("cannot serialize");
    }

    Ok(())
}

pub fn plot(records: &Vec<Record>, caption : &str, path : &str) -> Result<(), Box<dyn Error>> {
    let mut groups = Groups::new();

    for record in records.iter() {
        let group = groups.entry(record.name.clone()).or_insert_with(Vec::new);
        group.push(record.clone());
    }

    let resolution = (PLOT_WIDTH, PLOT_HEIGHT);
    let root = SVGBackend::new(&path, resolution).into_drawing_area();

    root.fill(&WHITE)?;

    let (x_max, y_max) = groups
        .values()
        .flatten()
        .map(|record| (record.threads, record.latency))
        .fold((0, Duration::from_secs(0)), |res, cur| {
            (res.0.max(cur.0), res.1.max(cur.1))
        });

    let y_max = y_max.as_nanos() as u64;

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

    let colors = COLORS.iter().cycle();

    for (records, color) in groups.values().zip(colors) {
        chart
            .draw_series(LineSeries::new(
                records
                    .iter()
                    .map(|record| (record.threads, record.latency.as_nanos() as u64)),
                color,
            ))?
            .label(&records[0].name)
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
