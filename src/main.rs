#![warn(clippy::all, rust_2018_idioms)]
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows in release

pub mod components;
pub mod data;
pub mod layout;

use crate::components::Action;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt()]
struct Args {
    #[structopt()]
    filename: Option<String>,
    // #[structopt(short, long, requires("filename"))]
    // query: Option<String>,

    // #[structopt(short, long, requires_all(&["filename", "query"]))]
    // table_name: Option<TableName>,
}

#[cfg(not(target_arch = "wasm32"))]
fn main() {
    // Log to stdout (if you run with `RUST_LOG=debug`).

    use data::TableDescriptor;
    use eframe::icon_data::from_png_bytes;
    tracing_subscriber::fmt::init();

    let args = Args::from_args();
    let icon =
        from_png_bytes(include_bytes!("../assets/icon-circle.png")).expect("Failed to load icon");

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([320.0, 240.0])
            .with_drag_and_drop(true)
            .with_icon(icon),
        ..Default::default()
    };

    eframe::run_native(
        "ParqBench",
        options,
        Box::new(move |cc| {
            let mut app = layout::ParqBenchApp::new(cc);
            if let Some(filename) = args.filename {
                let table =
                    TableDescriptor::new(&filename).expect("Could not build table from filename");
                app.handle_action(Action::LoadSource(table));
            }
            Box::new(app)
        }),
    )
    .expect("Could not create app");
}
