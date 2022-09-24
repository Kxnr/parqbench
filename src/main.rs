#![warn(clippy::all, rust_2018_idioms)]
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows in release

pub mod components;
pub mod data;
pub mod layout;

use std::env;
use structopt::StructOpt;
use crate::data::{DataFilters, TableName};

#[derive(StructOpt)]
#[structopt()]
struct Args {
    #[structopt()]
    filename: Option<String>,

    #[structopt(short, long, requires("filename"))]
    query: String,

    #[structopt(default_value, short, long, requires("filename"))]
    table_name: TableName
}

#[cfg(not(target_arch = "wasm32"))]
fn main() {
    // Log to stdout (if you run with `RUST_LOG=debug`).
    tracing_subscriber::fmt::init();

    let args = Args::from_args();
    if let (
        
    )

    let options = eframe::NativeOptions {
        drag_and_drop_support: true,
        ..Default::default()
    };

    eframe::run_native(
        "ParqBench",
        options,
        Box::new(|cc| Box::new(layout::ParqBenchApp::new(cc))),
    );
}
