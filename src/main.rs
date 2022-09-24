#![warn(clippy::all, rust_2018_idioms)]
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows in release

pub mod components;
pub mod data;
pub mod layout;

use std::env;

#[cfg(not(target_arch = "wasm32"))]
fn main() {
    // Log to stdout (if you run with `RUST_LOG=debug`).
    tracing_subscriber::fmt::init();

    let _args: Vec<String> = env::args().collect();

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
