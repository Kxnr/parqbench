#![warn(clippy::all)]
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows in release

pub mod components;
pub mod data;
pub mod layout;

use crate::data::{DataFilters, ParquetData, TableName};
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

/**
    clear && cargo test -- --nocapture
    clear && cargo run -- -h
    cargo b -r && cargo install --path=.
*/

#[cfg(not(target_arch = "wasm32"))]
fn main() -> eframe::Result<()> {
    // Log to stdout (if you run with `RUST_LOG=debug`).
    tracing_subscriber::fmt::init();

    let args = Args::from_args();

    let options = eframe::NativeOptions {
        // drag_and_drop_support: true,
        centered: true,
        persist_window: true,
        ..Default::default()
    };

    eframe::run_native(
        "ParqBench",
        options,
        Box::new(move |cc| {
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
                            layout::ParqBenchApp::new_with_future(cc, Box::new(Box::pin(future)))
                        }
                        None => {
                            let future = ParquetData::load(filename);
                            layout::ParqBenchApp::new_with_future(cc, Box::new(Box::pin(future)))
                        }
                    }
                }
            })
        }),
    )
}
