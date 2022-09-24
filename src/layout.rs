use datafusion::arrow::util::display::array_value_to_string;
use eframe;
use egui::{Ui, WidgetText};
use std::future::Future;
use std::marker::Send;

use core::default::Default;
use f32;
use std::sync::Arc;
use tokio::sync::oneshot::error::TryRecvError;
use crate::components::{SelectionDepth, file_dialog};

use crate::data::{DataFilters, ParquetData, SortState};

pub struct ParqBenchApp {
    pub table: Arc<Option<ParquetData>>,
    // error: Err(String) TODO
    runtime: tokio::runtime::Runtime,
    pipe: Option<tokio::sync::oneshot::Receiver<Result<ParquetData, String>>>,
}

impl Default for ParqBenchApp {
    fn default() -> Self {
        Self {
            table: Arc::new(None),
            runtime: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap(),
            pipe: None,
        }
    }
}

impl ParqBenchApp {
    // TODO: re-export load, query, and sort.
    pub fn new(_cc: &eframe::CreationContext<'_>) -> Self {
        Default::default()
    }

    pub fn data_pending(&mut self) -> bool {
        // hide implementation details of waiting for data to load
        // FIXME: should do some error handling/notification
        match &mut self.pipe {
            Some(output) => match output.try_recv() {
                Ok(data) => match data {
                    Ok(data) => {
                        self.table = Arc::new(Some(data));
                        self.pipe = None;
                        false
                    }
                    _ => {
                        self.pipe = None;
                        false
                    }
                },
                Err(e) => match e {
                    TryRecvError::Empty => true,
                    TryRecvError::Closed => false,
                },
            },
            _ => false,
        }
    }

    pub fn run_data_future<F>(&mut self, future: F, ctx: &egui::Context)
    where
        F: Future<Output = Result<ParquetData, String>> + Send + 'static,
    {
        if self.data_pending() {
            // FIXME, use vec of tasks?
            panic!("Cannot schedule future when future already running");
        }
        let (tx, rx) = tokio::sync::oneshot::channel::<Result<ParquetData, String>>();
        self.pipe = Some(rx);

        async fn inner<F>(
            future: F,
            ctx: egui::Context,
            tx: tokio::sync::oneshot::Sender<F::Output>,
        ) where
            F: Future<Output = Result<ParquetData, String>> + Send,
        {
            let data = future.await;
            let _result = tx.send(data);
            ctx.request_repaint();
        }

        self.runtime.spawn(inner::<F>(future, ctx.clone(), tx));
    }
}

impl eframe::App for ParqBenchApp {
    fn update(&mut self, ctx: &egui::Context, frame: &mut eframe::Frame) {
        //////////
        // Frame setup. Check if various interactions are in progress and resolve them
        //////////
        ctx.set_visuals(egui::style::Visuals::dark());

        if !ctx.input().raw.dropped_files.is_empty() {
            // FIXME: unsafe unwraps
            let file: egui::DroppedFile = ctx.input().raw.dropped_files.last().unwrap().clone();
            let filename = file.path.unwrap().to_str().unwrap().to_string();
            self.run_data_future(ParquetData::load(filename), ctx);
        }

        //////////
        // Main UI layout.
        //////////

        //////////
        //   Using static layout until I put together a TabTree that can make this dynamic
        //
        //   | menu_bar            |
        //   -----------------------
        //   |       |             |
        //   | query |     main    |
        //   | info  |     table   |
        //   |       |             |
        //   -----------------------
        //   | notification footer |
        //
        //////////

        egui::TopBottomPanel::top("top_panel").show(ctx, |ui| {
            egui::menu::bar(ui, |ui| {
                ui.menu_button("File", |ui| {
                    ui.menu_button("About", |ui| {
                        ui.label("Built with egui");
                    });

                    if ui.button("Open...").clicked() {
                        let filename = self.runtime.block_on(file_dialog());
                        self.run_data_future(ParquetData::load(filename), ctx);
                        ui.close_menu();
                    }

                    if ui.button("Quit").clicked() {
                        frame.quit();
                    }
                });
            });
        });

        // egui::SidePanel::left("side_panel").min_width(0f32).max_width(400f32).resizable(true).show(ctx, |ui| {
        //     // TODO: collapsing headers
        //     egui::ScrollArea::vertical().show(ui, |ui| {
        //         ui.label("Filter");
        //         ui.label("Not Yet Implemented");
        //         ui.set_enabled(false);
        //         ui.label("Table Name");
        //         // ui.text_edit_singleline(&mut self.filters.table_name);
        //         ui.label("SQL Query");
        //         // ui.text_edit_singleline(&mut self.filters.query);
        //         // TODO: input, update data, output for errors
        //         // button
        //         // self.data.query(self.filters)
        //     });
        // });

        egui::TopBottomPanel::bottom("bottom_panel").show(ctx, |ui| {
            ui.horizontal(|ui| {
                match &*self.table {
                    Some(table) => {
                        ui.label(format!("{:#?}", table.filename));
                    }
                    None => {
                        ui.label("no file set");
                    }
                }
                egui::warn_if_debug_build(ui);
            });
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            egui::ScrollArea::horizontal().show(ui, |ui| {
                let filters = match *self.table {
                    Some(_) => self.table.as_ref().clone().unwrap().render_table(ui),
                    _ => None,
                };

                if let Some(filters) = filters {
                    self.run_data_future(
                        self.table.as_ref().clone().unwrap().sort(Some(filters)),
                        ui.ctx(),
                    )
                }
            });

            if self.data_pending() {
                ui.set_enabled(false);
                if self.table.is_none() {
                    ui.centered_and_justified(|ui| {
                        ui.spinner();
                    });
                }
            } else if self.table.is_none() {
                ui.centered_and_justified(|ui| {
                    ui.label("Drag and drop parquet file here.");
                });
            }
        });
    }
}
