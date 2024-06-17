use eframe;
use egui::Layout;

use crate::{
    components::{Action, ErrorLog, Popover, QueryBuilder, Show, ShowMut},
    data::{Data, DataResult, DataSource, Query, TableDescriptor},
};
use async_compat::Compat;
use core::default::Default;
use smol::lock::RwLock;
use smol::Task;
use std::{
    mem,
    sync::{
        mpsc::{channel, Receiver, Sender},
        Arc,
    },
};

enum DataContainer {
    Some(Data),
    Pending(Task<DataResult>),
    None,
}

impl DataContainer {
    fn apply(&mut self, apply: impl FnOnce(Data) -> Task<DataResult>) {
        let old = mem::replace(self, DataContainer::None);
        *self = match old {
            DataContainer::Some(data) => Self::Pending(apply(data)),
            _ => old,
        };
    }

    fn try_resolve(&mut self) -> Option<DataResult> {
        match self {
            Self::Pending(task) => {
                if task.is_finished() {
                    Some(smol::block_on(task))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn pending(&self) -> bool {
        matches!(self, Self::Pending(_))
    }
}

#[derive(Default)]
struct DisplayStates {
    popover: bool,
    error: bool,
    settings: bool,
}

pub struct ParqBenchApp {
    data_source: Arc<RwLock<DataSource>>,
    current_data: DataContainer,
    query: QueryBuilder,
    // TODO: separate error popover and modal dialog
    popover: Option<Box<dyn Popover>>,
    error_log_channel: (Sender<anyhow::Error>, Receiver<anyhow::Error>),
    errors: ErrorLog,
    display_states: DisplayStates,
}

impl Default for ParqBenchApp {
    fn default() -> Self {
        Self {
            data_source: Arc::new(RwLock::new(DataSource::default())),
            query: QueryBuilder::default(),
            current_data: DataContainer::None,
            popover: None,
            error_log_channel: channel(),
            errors: vec![],
            display_states: DisplayStates::default(),
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
                let channel = self.error_log_channel.0.clone();
                smol::spawn(Compat::new(async move {
                    if let Err(err) = data_source.write().await.add_data_source(table).await {
                        // if the channel is closed, not much we can do
                        let _ = channel.send(err);
                    }
                }))
                .detach();
            }
            Action::QuerySource(query) => {
                // TODO: use apply
                let data_source = self.data_source.clone();
                self.current_data = DataContainer::Pending(smol::spawn(Compat::new(async move {
                    data_source.read().await.query(query).await
                })));
            }
            Action::LoadSource(table) => {
                // TODO: use apply
                let data_source = self.data_source.clone();
                self.current_data = DataContainer::Pending(smol::spawn(Compat::new(async move {
                    let table_name = data_source.write().await.add_data_source(table).await?;
                    dbg!(&table_name);
                    data_source
                        .read()
                        .await
                        .query(Query::TableName(table_name))
                        .await
                })));
            }
            Action::SortData((col, sort_state)) => {
                self.current_data
                    .apply(|data| smol::spawn(async move { data.sort(col, sort_state).await }));
            }
            // if let DataContainer::Some(data) = self.current_data {
            //     self.current_data =
            //         DataContainer::Pending(smol::spawn(Compat::new));
            // }
            Action::ShowPopover(popover) => {
                self.popover = Some(popover);
            }
            Action::LogError(err) => {
                self.errors.push(err);
            }
            Action::DeleteSource(table) => {
                if let Err(err) = self
                    .data_source
                    .clone()
                    .write_blocking()
                    .delete_data_source(&table)
                {
                    self.errors.push(err);
                };
            }
            Action::RenameSource((from_name, to_name)) => {
                if let Err(err) = self
                    .data_source
                    .clone()
                    .write_blocking()
                    .rename_data_source(&from_name, &to_name)
                {
                    self.errors.push(err);
                };
            }
        };
    }

    fn check_error_channel(&mut self) {
        if let Ok(err) = self.error_log_channel.1.try_recv() {
            self.handle_action(Action::LogError(err));
        }
    }

    fn check_floating_displays(&mut self, ctx: &egui::Context) {
        egui::Window::new("Settings")
            .collapsible(false)
            .open(&mut self.display_states.settings)
            .show(ctx, |ui| {
                egui::ScrollArea::vertical()
                    .auto_shrink(false)
                    .show(ui, |ui| {
                        ctx.style_ui(ui);
                    });
            });

        if self.display_states.error {
            let (open, _) = self.errors.popover(ctx);
            self.display_states.error = open;
        }

        if self.display_states.popover {
            if let Some(popover) = &mut self.popover {
                let (open, action) = popover.popover(ctx);
                self.display_states.popover = open;
                if let Some(action) = action {
                    self.handle_action(action);
                }
            }
        }
    }

    fn check_data_future(&mut self) -> bool {
        if let Some(result) = self.current_data.try_resolve() {
            match result {
                Ok(data) => {
                    self.current_data = DataContainer::Some(data);
                }
                Err(err) => {
                    self.handle_action(Action::LogError(err));
                    self.current_data = DataContainer::None;
                }
            };
        };
        self.current_data.pending()
    }
}

impl eframe::App for ParqBenchApp {
    fn update(&mut self, ctx: &egui::Context, _: &mut eframe::Frame) {
        //////////
        // Frame setup. Check if various interactions are in progress and resolve them
        //////////

        self.check_error_channel();
        self.check_floating_displays(ctx);
        let loading = self.check_data_future();

        ctx.input(|i| {
            if let Some(file) = i.raw.dropped_files.last() {
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
        //   | header              |
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
            ui.horizontal(|ui| {
                egui::warn_if_debug_build(ui);
                ui.with_layout(Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui.button("⚙").clicked() {
                        self.display_states.settings = true;
                    }
                });
            });
        });

        egui::TopBottomPanel::bottom("bottom_panel").show(ctx, |ui| {
            ui.horizontal(|ui| {
                if let Some(err) = self.errors.last() {
                    let text = egui::RichText::new(format!("⚠ {}: {}", self.errors.len(), err))
                        .color(ui.style().visuals.error_fg_color);
                    if ui
                        .add(
                            egui::Label::new(text)
                                .truncate(true)
                                .sense(egui::Sense::click()),
                        )
                        .clicked()
                    {
                        // FIXME: use an action for self-referential popovers
                        self.display_states.error = true;
                    }
                };
            });
        });

        egui::SidePanel::left("side_panel")
            .resizable(true)
            .show(ctx, |ui| {
                egui::ScrollArea::vertical().show(ui, |ui| {
                    egui::Grid::new("side_panel").num_columns(1).show(ui, |ui| {
                        ui.heading("Data Sources");
                        ui.end_row();
                        ui.vertical(|ui| {
                            let action =
                                smol::block_on(self.data_source.write_blocking().list_tables())
                                    .show(ui);
                            if let Some(action) = action {
                                self.handle_action(action)
                            }
                        });
                        ui.end_row();
                        ui.end_row();
                        ui.heading("Query");
                        ui.end_row();
                        ui.vertical(|ui| {
                            if let Some(query) = self.query.show(ui) {
                                self.handle_action(query);
                            }
                        });
                        ui.end_row();
                    });
                });
            });

        egui::CentralPanel::default().show(ctx, |ui| {
            egui::ScrollArea::horizontal().show(ui, |ui| {
                if let DataContainer::Some(ref data) = self.current_data {
                    if let Some(action) = data.show(ui) {
                        self.handle_action(action);
                    }
                } else if loading {
                    ui.spinner();
                } else {
                    ui.centered_and_justified(|ui| {
                        ui.label("Drag and drop file or directory here");
                    });
                }
            });
        });
    }
}
