use std::sync::{Arc, Mutex};

use crate::data::{Data, DataSourceListing, Query, SortState, TableDescriptor};
use datafusion::arrow::{
    datatypes::{DataType, Schema},
    util::display::array_value_to_string,
};
use egui::{Context, Id, Response, Ui};
use egui_extras::{Column, TableBuilder};
use egui_file_dialog::FileDialog;
use egui_json_tree::JsonTree;
use itertools::Itertools;
use serde_json::Value;

pub type ErrorLog = Vec<anyhow::Error>;
type FromName = String;
type ToName = String;

pub enum Action {
    AddSource(TableDescriptor),
    QuerySource(Query),
    LoadSource(TableDescriptor),
    DeleteSource(String),
    RenameSource((FromName, ToName)),
    SortData((String, SortState)),
    ShowPopover(Box<dyn Popover>),
    LogError(anyhow::Error),
}

pub trait Popover {
    // TODO: make Popover a property of the app, not a component
    fn popover(&mut self, ctx: &Context) -> (bool, Option<Action>);
}

pub trait ShowMut {
    fn show(&mut self, ui: &mut Ui) -> Option<Action>;
}

pub trait Show {
    fn show(&self, ui: &mut Ui) -> Option<Action>;
}

#[derive(Default)]
pub struct QueryBuilder {
    query: String,
}

#[derive(Clone, Copy, PartialEq)]
enum SourceType {
    Azure,
    Local,
}

pub struct AddDataSource {
    // controls what configuration menu to show
    source_type: SourceType,
    file_dialog: Option<FileDialog>,
    account: String,
    container: String,
    path: String,
    extension: String,
    table_name: String,
    read_metadata: bool,
}

impl Popover for ErrorLog {
    fn popover(&mut self, ctx: &Context) -> (bool, Option<Action>) {
        let mut open = true;

        egui::Window::new("Error Log")
            .collapsible(false)
            .open(&mut open)
            .show(ctx, |ui| {
                egui::ScrollArea::vertical()
                    .auto_shrink(false)
                    .show(ui, |ui| {
                        egui::Grid::new("error log").show(ui, |ui| {
                            for err in self.iter() {
                                ui.label(format!("{}", err));
                                ui.end_row();
                            }
                        });
                    });
            });
        (open, None)
    }
}

impl Default for AddDataSource {
    fn default() -> Self {
        AddDataSource {
            source_type: SourceType::Local,
            file_dialog: None,
            account: "".to_owned(),
            container: "".to_owned(),
            path: "".to_owned(),
            extension: "".to_owned(),
            table_name: "".to_owned(),
            read_metadata: true,
        }
    }
}

impl AddDataSource {
    fn build(&self) -> anyhow::Result<TableDescriptor> {
        let mut table = match self.source_type {
            SourceType::Azure => {
                TableDescriptor::new(&format!("az://{}/{}", self.container, self.path))?
                    .with_account(&self.account)
            }
            SourceType::Local => TableDescriptor::new(&self.path)?,
        };
        if !self.extension.is_empty() {
            table = table.with_extension(&self.extension);
        }
        table = table.with_load_metadata(self.read_metadata);
        if !self.table_name.is_empty() {
            table = table.with_table_name(&self.table_name);
        }
        Ok(table)
    }
}

impl Popover for AddDataSource {
    fn popover(&mut self, ctx: &Context) -> (bool, Option<Action>) {
        let mut open = true;
        let mut action: Option<Action> = None;
        egui::Window::new("Configure Data Source")
            .collapsible(false)
            .open(&mut open)
            .show(ctx, |ui| {
                egui::Grid::new("Add Data Source")
                    .num_columns(2)
                    .show(ui, |ui| {
                        ui.scope(|ui| {
                            ui.selectable_value(&mut self.source_type, SourceType::Local, "Local");
                            ui.selectable_value(&mut self.source_type, SourceType::Azure, "Azure");
                        });

                        ui.checkbox(&mut self.read_metadata, "Read Metadata");
                        ui.end_row();

                        ui.label("Table Name");
                        ui.text_edit_singleline(&mut self.table_name);
                        ui.end_row();

                        ui.label("Extension");
                        ui.text_edit_singleline(&mut self.extension);
                        ui.end_row();
                        match self.source_type {
                            SourceType::Local => {
                                ui.label("Path");
                                ui.text_edit_singleline(&mut self.path);
                                if ui.button("Browse...").clicked() {
                                    let dialog = self.file_dialog.get_or_insert(
                                        FileDialog::new().show_path_edit_button(true),
                                    );
                                    dialog.select_file();
                                };
                                ui.end_row();

                                if let Some(dialog) = self.file_dialog.as_mut() {
                                    dialog.update(ctx);
                                    if let Some(path) = dialog.take_selected() {
                                        self.path = path.to_string_lossy().into_owned();
                                    };
                                }
                            }
                            SourceType::Azure => {
                                // TODO: support https:// url that includes account, container, and path
                                ui.label("Account");
                                ui.text_edit_singleline(&mut self.account);
                                ui.end_row();

                                ui.label("Container");
                                ui.text_edit_singleline(&mut self.container);
                                ui.end_row();

                                ui.label("Path");
                                ui.text_edit_singleline(&mut self.path);
                                ui.end_row();
                            }
                        }
                        ui.end_row();
                    });
                if let SourceType::Azure = self.source_type {
                    ui.label("Requires the azure cli to be installed and available on PATH");
                }

                ui.add_space(ui.style().spacing.interact_size.y);
                ui.vertical_centered_justified(|ui| {
                    if ui.button("add").clicked() {
                        if let Ok(table) = self.build() {
                            action = Some(Action::AddSource(table));
                        }
                    }
                    if ui.button("load").clicked() {
                        if let Ok(table) = self.build() {
                            action = Some(Action::LoadSource(table));
                        }
                    }
                });
            });

        (open, action)
    }
}

impl ShowMut for QueryBuilder {
    fn show(&mut self, ui: &mut Ui) -> Option<Action> {
        egui::TextEdit::multiline(&mut self.query)
            .clip_text(true)
            .show(ui);
        let submit = ui.button("Query");
        if submit.clicked() {
            Some(Action::QuerySource(Query::Sql(self.query.to_owned())))
        } else {
            None
        }
    }
}

impl Show for Data {
    fn show(&self, ui: &mut Ui) -> Option<Action> {
        let style = &ui.style().clone();

        fn get_sort_state(sort_state: &Option<(String, SortState)>, col: &str) -> SortState {
            match sort_state {
                Some((sorted_col, sort_state)) => {
                    if *sorted_col == col {
                        sort_state.to_owned()
                    } else {
                        SortState::NotSorted
                    }
                }
                _ => SortState::NotSorted,
            }
        }

        let text_height = egui::TextStyle::Body.resolve(style).size;
        // stop columns from getting too small to be usable
        let min_col_width = style.spacing.interact_size.x;

        // we put buttons in the header, so make sure that the vertical size of the header includes
        // the button size and the normal padding around buttons
        let header_height = style.spacing.interact_size.y + (2.0f32 * style.spacing.item_spacing.y);
        let mut action: Option<Action> = None;

        // FIXME: this will certainly break if there are no columns
        TableBuilder::new(ui)
            .striped(true)
            .stick_to_bottom(true)
            .auto_shrink(false)
            .columns(
                Column::remainder().at_least(min_col_width).clip(true),
                self.data.num_columns(),
            )
            .resizable(true)
            .header(header_height, |mut header| {
                for field in self.data.schema().fields() {
                    header.col(|ui| {
                        let column_name = field.name().to_string();
                        let mut sort_state = get_sort_state(&self.sort_state, &column_name);
                        ui.horizontal_centered(|ui| {
                            let response = ui.multi_state_button(&mut sort_state, &column_name);
                            if response.clicked() {
                                action = Some(Action::SortData((column_name.clone(), sort_state)));
                            }
                        });
                    });
                }
            })
            .body(|body| {
                body.rows(text_height, self.data.num_rows(), |mut row| {
                    for data_col in self.data.columns() {
                        let index = row.index();
                        row.col(|ui| {
                            // while not efficient (as noted in docs) we need to display
                            // at most a few dozen records at a time (barring pathological
                            // tables with absurd numbers of columns) and should still
                            // have conversion times on the order of ns.
                            // TODO: have separate value layout function
                            ui.with_layout(
                                if is_integer(data_col.data_type()) {
                                    egui::Layout::centered_and_justified(
                                        egui::Direction::LeftToRight,
                                    )
                                } else if is_float(data_col.data_type()) {
                                    egui::Layout::right_to_left(egui::Align::Center)
                                } else {
                                    egui::Layout::left_to_right(egui::Align::Center)
                                }
                                .with_main_wrap(false),
                                |ui| {
                                    let value = array_value_to_string(data_col, index).unwrap();
                                    ui.label(value);
                                },
                            );
                        });
                    }
                });
            });
        action
    }
}

// FIXME: parquet metadata is not loaded by either the Schema or DataSourceListing displays

impl Show for Schema {
    fn show(&self, ui: &mut Ui) -> Option<Action> {
        ui.collapsing("Schema", |ui| {
            for field in self.fields.iter() {
                ui.label(format!("{}: {}", field.name(), field.data_type()));
            }
        });
        ui.collapsing("Metadata", |ui| {
            for (key, value) in self.metadata.iter() {
                if let Ok(json) = serde_json::from_str::<Value>(value) {
                    JsonTree::new(key, &json).show(ui);
                } else {
                    ui.label(format!("{}: {}", key, value));
                }
            }
        });
        None
    }
}

trait EditableLabel {
    fn editable_label(&mut self, id: Id, label: &str) -> Option<String>;
}

impl EditableLabel for Ui {
    fn editable_label(&mut self, id: Id, label: &str) -> Option<String> {
        type State = Arc<Mutex<String>>;

        let current_label: Option<State> = self.memory_mut(|mem| mem.data.get_temp(id).clone());
        match current_label.as_ref() {
            Some(label) => {
                let mut label = label
                    .lock()
                    .expect("Failed to retrieve label from persisted State");
                let response = self.text_edit_singleline(&mut *label);
                if response.lost_focus() {
                    self.memory_mut(|mem| mem.data.remove::<State>(id));
                    Some(label.to_string())
                } else {
                    None
                }
            }
            None => {
                if self.selectable_label(false, label).clicked() {
                    dbg!("selected");
                    self.memory_mut(|mem| {
                        mem.data
                            .insert_temp(id, Arc::new(Mutex::new(label.to_owned())));
                    });
                }
                None
            }
        }
    }
}

impl Show for DataSourceListing {
    fn show(&self, ui: &mut Ui) -> Option<Action> {
        // TODO: rename table
        let mut action = None;
        for (table_name, table_definition) in self.iter().sorted_by_key(|x| x.0) {
            egui::collapsing_header::CollapsingState::load_with_default_open(
                ui.ctx(),
                format!("{} data source listing", table_name).into(),
                false,
            )
            .show_header(ui, |ui| {
                if let Some(rename) = ui.editable_label(table_name.to_owned().into(), table_name) {
                    action = Some(Action::RenameSource((table_name.to_owned(), rename)));
                }
                if ui.small_button("✖").clicked() {
                    action = Some(Action::DeleteSource(table_name.to_owned()));
                }
            })
            .body(|ui| {
                table_definition.schema().show(ui);
                if ui.button("Load").clicked() {
                    action = Some(Action::QuerySource(Query::TableName(table_name.to_owned())));
                }
            });
        }
        if ui.button("Add Source").clicked() {
            action = Some(Action::ShowPopover(Box::<AddDataSource>::default()));
        }
        action
    }
}

fn is_integer(t: &DataType) -> bool {
    use DataType::*;
    matches!(
        t,
        UInt8 | UInt16 | UInt32 | UInt64 | Int8 | Int16 | Int32 | Int64
    )
}

fn is_float(t: &DataType) -> bool {
    use DataType::*;
    matches!(t, Float32 | Float64)
}

pub trait SelectionDepth {
    // TODO: https://stackoverflow.com/questions/25867875/how-do-i-toggle-through-enum-variants
    fn inc(&self) -> Self;

    fn reset(&self) -> Self;

    fn format(&self) -> String;
}

impl SelectionDepth for SortState {
    fn inc(&self) -> Self {
        match self {
            SortState::Ascending => SortState::Descending,
            SortState::Descending => SortState::Ascending,
            SortState::NotSorted => SortState::Ascending,
        }
    }

    fn reset(&self) -> Self {
        // one day, I'll be proficient enough with macros that they'll be worth the time...
        SortState::NotSorted
    }

    fn format(&self) -> String {
        match self {
            SortState::Ascending => "\u{23f7}",
            SortState::Descending => "\u{23f6}",
            SortState::NotSorted => "\u{2195}",
        }
        .to_owned()
    }
}

pub trait ExtraInteractions {
    fn multi_state_button(&mut self, state: &mut impl SelectionDepth, label: &str) -> Response;
}

impl ExtraInteractions for Ui {
    fn multi_state_button(&mut self, state: &mut impl SelectionDepth, label: &str) -> Response {
        // TODO: this implementation doesn't implement column selection or mutual exclusivity,
        // TODO: but is very simple, the three/multistate toggle idea is worth revisiting at some
        // TODO: point
        let mut response = self.button(format!("{} {}", state.format(), label));
        if response.clicked() {
            *state = state.inc();
            response.mark_changed();
        };
        response
    }
}
