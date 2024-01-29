#![warn(clippy::all)]
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows in release

use parqbench::{Args, DataFilters, ParqBenchApp, ParquetData};

/**
    clear && cargo test -- --nocapture
    clear && cargo run -- -h
    cargo b -r && cargo install --path=.
*/

#[cfg(not(target_arch = "wasm32"))]
fn main() -> eframe::Result<()> {
    // Log to stdout (if you run with `RUST_LOG=debug`).

    use parqbench::TableName;
    tracing_subscriber::fmt::init();

    let args = Args::build();

    let options = eframe::NativeOptions {
        centered: true,
        persist_window: true,
        ..Default::default()
    };

    eframe::run_native(
        "ParqBench",
        options,
        Box::new(move |cc| {
            Box::new(match args.filename {
                Some(filename) => {
                    /*
                    match args.query {
                        Some(query) => {
                            let filters = DataFilters {
                                query: Some(query),
                                // FIXME: this doesn't grab struct default
                                table_name: args.table_name.unwrap_or_default(),
                                ..Default::default()
                            };
                            dbg!(filters.clone());
                            let future = ParquetData::load_with_query(filename, filters);
                            ParqBenchApp::new_with_future(cc, Box::new(Box::pin(future)))
                        }
                        None => {
                            let future = ParquetData::load(filename);
                            ParqBenchApp::new_with_future(cc, Box::new(Box::pin(future)))
                        }
                    }
                    */

                    let data_filters = match (args.query, args.table_name) {
                        (Some(query), Some(table_name)) => DataFilters {
                            query: Some(query),
                            table_name,
                            ..Default::default()
                        },
                        (Some(query), None) => DataFilters {
                            query: Some(query),
                            table_name: TableName::default(),
                            ..Default::default()
                        },
                        _ => DataFilters::default(),
                    };
                    dbg!(data_filters);

                    let future = ParquetData::load(filename);
                    ParqBenchApp::new_with_future(cc, Box::new(Box::pin(future)))
                }
                None => ParqBenchApp::new(cc),
            })
        }),
    )
}
