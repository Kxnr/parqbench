use eframe;

use crate::{
    components::{Action, Popover, QueryBuilder, Settings, Show, ShowMut},
    data::{Data, DataResult, DataSource, Query, TableDescriptor},
};
use async_compat::Compat;
use core::default::Default;
use smol::lock::RwLock;
use smol::Task;
use std::sync::Arc;

pub struct ParqBenchApp {
    data_source: Arc<RwLock<DataSource>>,
    current_data: Option<Data>,
    query: QueryBuilder,
    popover: Option<Box<dyn Popover>>,
    data_future: Option<Task<DataResult>>,
}

impl Default for ParqBenchApp {
    fn default() -> Self {
        Self {
            data_source: Arc::new(RwLock::new(DataSource::default())),
            query: QueryBuilder::new(),
            current_data: None,
            popover: None,
            data_future: None,
        }
    }
}

impl ParqBenchApp {
    pub fn new(cc: &eframe::CreationContext<'_>) -> Self {
        cc.egui_ctx.set_visuals(egui::style::Visuals::dark());
        Default::default()
    }

    pub fn handle_action(&mut self, action: Action) {
        match action {
            Action::AddSource(table) => {
                let data_source = self.data_source.clone();
                smol::spawn(Compat::new(async move {
                    let maybe_err = data_source
                        .clone()
                        .write()
                        .await
                        .add_data_source(table)
                        .await;
                    dbg!(maybe_err);
                }))
                .detach();
            }
            Action::QuerySource(query) => {
                let data_source = self.data_source.clone();
                self.data_future = Some(smol::spawn(Compat::new(async move {
                    data_source.read().await.query(query).await
                })));
            }
            Action::LoadSource(table) => {
                let data_source = self.data_source.clone();
                self.data_future = Some(smol::spawn(Compat::new(async move {
                    let table_name = data_source
                        .clone()
                        .write()
                        .await
                        .add_data_source(table)
                        .await?;
                    dbg!(&table_name);
                    data_source
                        .clone()
                        .read()
                        .await
                        .query(Query::TableName(table_name))
                        .await
                })));
            }
            Action::SortData((col, sort_state)) => {
                self.current_data = self
                    .current_data
                    .take()
                    .and_then(|data| smol::block_on(data.sort(col, sort_state)).ok());
            }
            Action::ShowPopover(popover) => {
                self.popover = Some(popover);
            }
            _ => {}
        };
    }

    fn check_popover(&mut self, ctx: &egui::Context) {
        if let Some(popover) = &mut self.popover {
            let (open, action) = popover.popover(ctx);
            if !open {
                self.popover = None;
            }
            if let Some(action) = action {
                self.handle_action(action);
            }
        }
    }

    fn check_data_future(&mut self) -> bool {
        // hide implementation details of waiting for data to load
        // FIXME: should do some error handling/notification
        if let Some(future) = self.data_future.as_mut() {
            if future.is_finished() {
                match smol::block_on(future) {
                    Ok(data) => {
                        self.current_data = Some(data);
                    }
                    Err(msg) => self.popover = Some(Box::new(msg)),
                };
                self.data_future = None;
            };
            // This will switch from true to false one frame late, but makes this logic simple
            true
        } else {
            false
        }
    }
}

impl eframe::App for ParqBenchApp {
    fn update(&mut self, ctx: &egui::Context, _: &mut eframe::Frame) {
        //////////
        // Frame setup. Check if various interactions are in progress and resolve them
        //////////

        self.check_popover(ctx);
        let loading = self.check_data_future();

        ctx.input(|i| {
            if let Some(file) = i.raw.dropped_files.last().clone() {
                let filename = file.path.as_ref().unwrap().to_str().unwrap().to_string();

                if let Ok(table) = TableDescriptor::new(&filename) {
                    self.handle_action(Action::LoadSource(table));
                }
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

                    if ui.button("Settings...").clicked() {
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
                    ui.collapsing("Data", |ui| {
                        let action =
                            smol::block_on(self.data_source.write_blocking().list_tables())
                                .show(ui);
                        if let Some(action) = action {
                            self.handle_action(action)
                        }
                        if let Some(query) = self.query.show(ui) {
                            self.handle_action(query);
                        }
                    });
                });
            });

        egui::TopBottomPanel::bottom("bottom_panel").show(ctx, |ui| {
            ui.horizontal(|ui| {
                egui::warn_if_debug_build(ui);
                if loading {
                    ui.spinner();
                }
            });
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            egui::ScrollArea::horizontal().show(ui, |ui| {
                if let Some(data) = self.current_data.as_mut() {
                    if let Some(action) = data.show(ui) {
                        self.handle_action(action);
                    }
                } else {
                    ui.centered_and_justified(|ui| {
                        ui.label("Drag and drop data source here, or use Add Source menu");
                    });
                }
            });
        });
    }
}
