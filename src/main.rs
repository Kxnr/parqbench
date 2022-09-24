#![warn(clippy::all, rust_2018_idioms)]
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows in release

pub mod components;
pub mod data;
pub mod layout;

use crate::data::{DataFilters, ParquetData, TableName};
use std::env;
use std::process::exit;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt()]
struct Args {
    #[structopt()]
    filename: Option<String>,

    #[structopt(short, long, requires("filename"))]
    query: Option<String>,

    #[structopt(short, long, requires_all(&["filename", "query"]))]
    table_name: Option<TableName>,
}

#[cfg(not(target_arch = "wasm32"))]
fn main() {
    // Log to stdout (if you run with `RUST_LOG=debug`).
    tracing_subscriber::fmt::init();

    let args = Args::from_args();

    let options = eframe::NativeOptions {
        drag_and_drop_support: true,
        ..Default::default()
    };

    eframe::run_native(
        "ParqBench",
        options,
        Box::new(
            move |cc| {
                // layout::ParqBenchApp::new_with_future(ParquetData::load_with_query())

                Box::new(match args.filename {
                    None => layout::ParqBenchApp::new(cc),
                    Some(filename) => {
                        match args.query {
                            Some(_) => {
                                let filters = DataFilters {
                                    query: args.query,
                                    // FIXME: this doesn't grab struct default
                                    table_name: args.table_name.unwrap_or_default(),
                                    ..Default::default()
                                };
                                dbg!(filters.clone());
                                let future = ParquetData::load_with_query(filename, filters);
                                layout::ParqBenchApp::new_with_future(cc, future)
                            }
                            None => {
                                let future = ParquetData::load(filename);
                                layout::ParqBenchApp::new_with_future(cc, future)
                            }
                        }
                    }
                })
            },
        ),
    );
}
