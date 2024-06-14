use std::sync::Arc;

use crate::data::{Data, DataSourceListing, Query, SortState};
use datafusion::arrow::{
    datatypes::{DataType, Schema},
    util::display::array_value_to_string,
};
use egui::{Context, Response, Ui};
use egui_extras::{Column, TableBuilder};
use egui_json_tree::JsonTree;
use itertools::Itertools;
use rfd::AsyncFileDialog;
use serde_json::Value;

#[derive(Debug)]
pub enum Action {
    AddSource(String),
    QuerySource(Query),
    LoadSource(String),
    DeleteSource,
    SortData((String, SortState)),
}

pub trait Popover {
    fn popover(&mut self, ctx: &Context) -> bool;
}

pub trait ShowMut {
    fn show(&mut self, ui: &mut Ui) -> Option<Action>;
}

pub trait Show {
    fn show(&self, ui: &mut Ui) -> Option<Action>;
}

pub struct Settings {}

impl Popover for Settings {
    fn popover(&mut self, ctx: &Context) -> bool {
        let mut open = true;

        egui::Window::new("Settings")
            .collapsible(false)
            .open(&mut open)
            .show(ctx, |ui| {
                ctx.style_ui(ui);
                ui.set_enabled(false);
            });

        open
    }
}

impl Popover for anyhow::Error {
    fn popover(&mut self, ctx: &Context) -> bool {
        let mut open = true;

        egui::Window::new("Error")
            .collapsible(false)
            .open(&mut open)
            .show(ctx, |ui| {
                // eprintln!("{:?}", self);
                ui.label(format!("Error: {}", self));
                ui.set_enabled(false);
            });

        open
    }
}

impl ShowMut for Query {
    fn show(&mut self, ui: &mut Ui) -> Option<Action> {
        ui.collapsing("Query", |ui| match self {
            Query::TableName(_) => None,
            Query::Sql(query) => {
                ui.label("Query:".to_string());
                egui::TextEdit::singleline(query).clip_text(true).show(ui);
                let submit = ui.button("Apply");
                if submit.clicked() && !query.is_empty() {
                    Some(Action::QuerySource(self.clone()))
                } else {
                    None
                }
            }
        })
        .body_returned
        .unwrap_or(None)
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
                                action = Some(Action::SortData((
                                    column_name.clone(),
                                    sort_state.clone(),
                                )));
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

impl Show for DataSourceListing {
    fn show(&self, ui: &mut Ui) -> Option<Action> {
        // TODO: rename table
        ui.collapsing("Sources", |ui| {
            for (table_name, table_definition) in self.iter().sorted_by_key(|x| x.0) {
                ui.collapsing(table_name, |ui| {
                    // TODO: show shouldn't need an `&mut` for read only views
                    Arc::make_mut(&mut table_definition.schema()).show(ui);
                });
            }
        });
        None
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

pub async fn file_dialog() -> Result<String, String> {
    let file = AsyncFileDialog::new().pick_file().await;

    if let Some(file) = file {
        file.inner()
            .to_str()
            .ok_or_else(|| "Could not parse path.".to_string())
            .map(|s| s.to_string())
    } else {
        Err("No file loaded.".to_string())
    }
}
