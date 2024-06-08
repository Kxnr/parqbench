use eframe;

use crate::{
    components::{file_dialog, Popover, Settings},
    data::{Data, DataFuture, DataResult, DataSource, Query},
};
use core::default::Default;
use std::sync::Arc;
use tokio::sync::oneshot::error::TryRecvError;

pub struct ParqBenchApp {
    data_source: DataSource,
    current_data: Option<Data>,
    query: Query,
    pub popover: Option<Box<dyn Popover>>,

    runtime: tokio::runtime::Runtime,
    pipe: Option<tokio::sync::oneshot::Receiver<DataResult>>,
}

impl Default for ParqBenchApp {
    fn default() -> Self {
        Self {
            data_source: DataSource::default(),
            query: Query::Sql("".to_owned()),
            current_data: None,
            runtime: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap(),
            pipe: None,
            popover: None,
        }
    }
}

impl ParqBenchApp {
    // TODO: re-export load, query, and sort.
    pub fn new(cc: &eframe::CreationContext<'_>) -> Self {
        cc.egui_ctx.set_visuals(egui::style::Visuals::dark());
        Default::default()
    }

    pub fn new_with_future(cc: &eframe::CreationContext<'_>, future: DataFuture) -> Self {
        let mut app: Self = Default::default();
        cc.egui_ctx.set_visuals(egui::style::Visuals::dark());
        app.run_data_future(future, &cc.egui_ctx);
        app
    }

    pub fn check_popover(&mut self, ctx: &egui::Context) {
        if let Some(popover) = &mut self.popover {
            if !popover.popover(ctx) {
                self.popover = None;
            }
        }
    }

    pub fn check_data_pending(&mut self) -> bool {
        // hide implementation details of waiting for data to load
        // FIXME: should do some error handling/notification
        match &mut self.pipe {
            Some(output) => match output.try_recv() {
                Ok(data) => match data {
                    Ok(data) => {
                        self.current_data = Some(data);
                        self.pipe = None;
                        false
                    }
                    Err(msg) => {
                        self.pipe = None;
                        self.popover = Some(Box::new(Error { message: msg }));
                        false
                    }
                },
                Err(e) => match e {
                    TryRecvError::Empty => true,
                    TryRecvError::Closed => {
                        self.pipe = None;
                        self.popover = Some(anyhow::anyhow!(
                            "Data operation terminated without response."
                        ));
                        false
                    }
                },
            },
            _ => false,
        }
    }

    pub fn run_data_future(&mut self, future: DataFuture, ctx: &egui::Context) {
        if self.check_data_pending() {
            // FIXME, use vec of tasks?
            panic!("Cannot schedule future when future already running");
        }
        let (tx, rx) = tokio::sync::oneshot::channel::<Result<ParquetData, String>>();
        self.pipe = Some(rx);

        async fn inner(
            future: DataFuture,
            ctx: egui::Context,
            tx: tokio::sync::oneshot::Sender<DataResult>,
        ) {
            let data = future.await;
            let _result = tx.send(data);
            ctx.request_repaint();
        }

        self.runtime.spawn(inner(future, ctx.clone(), tx));
    }
}

impl eframe::App for ParqBenchApp {
    fn update(&mut self, ctx: &egui::Context, _: &mut eframe::Frame) {
        //////////
        // Frame setup. Check if various interactions are in progress and resolve them
        //////////

        self.check_popover(ctx);

        ctx.input(|i| {
            if let Some(file) = i.raw.dropped_files.last().clone() {
                let filename = file.path.as_ref().unwrap().to_str().unwrap().to_string();
                self.run_data_future(Box::new(Box::pin(ParquetData::load(filename))), ctx);
            }
        });

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
                        if let Ok(filename) = self.runtime.block_on(file_dialog()) {
                            self.run_data_future(
                                Box::new(Box::pin(ParquetData::load(filename))),
                                ctx,
                            );
                        }
                        ui.close_menu();
                    }

                    if ui.button("Settings...").clicked() {
                        // FIXME: need to manage potential for multiple popovers
                        self.popover = Some(Box::new(Settings {}));
                        ui.close_menu();
                    }

                    if ui.button("Quit").clicked() {
                        ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                    }
                });
            });
        });

        egui::SidePanel::left("side_panel")
            .resizable(true)
            .show(ctx, |ui| {
                // TODO: collapsing headers
                egui::ScrollArea::vertical().show(ui, |ui| {
                    ui.collapsing("Query", |ui| {
                        let filters = self.query_pane.render(ui);
                        if let Some((filename, filters)) = filters {
                            self.run_data_future(
                                Box::new(Box::pin(ParquetData::load_with_query(filename, filters))),
                                ctx,
                            );
                        }
                    });
                    if let Some(metadata) = &self.metadata {
                        ui.collapsing("Metadata", |ui| {
                            metadata.render_metadata(ui);
                        });
                    }
                    if let Some(metadata) = &self.metadata {
                        ui.collapsing("Schema", |ui| {
                            metadata.render_schema(ui);
                        });
                    }
                });
            });

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
                    let future = self.table.as_ref().clone().unwrap().sort(Some(filters));
                    self.run_data_future(Box::new(Box::pin(future)), ctx);
                }
            });

            if self.check_data_pending() {
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
