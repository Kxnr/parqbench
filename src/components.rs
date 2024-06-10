use crate::data::{Data, Query, SortState};
use datafusion::arrow::{
    datatypes::{DataType, Schema},
    util::display::array_value_to_string,
};
use egui::{Context, Response, Ui};
use egui_extras::{Column, TableBuilder};
use egui_json_tree::JsonTree;
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

pub trait Show {
    fn show(&mut self, ui: &mut Ui) -> Option<Action>;
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
                ui.label(format!("Error: {}", self));
                ui.set_enabled(false);
            });

        open
    }
}

impl Show for Query {
    fn show(&mut self, ui: &mut Ui) -> Option<Action> {
        match self {
            Query::TableName(_) => None,
            Query::Sql(query) => {
                ui.label("Query:".to_string());
                ui.text_edit_singleline(query);
                let submit = ui.button("Apply");
                if submit.clicked() && !query.is_empty() {
                    Some(Action::QuerySource(self.clone()))
                } else {
                    None
                }
            }
        }
    }
}

impl Show for Data {
    fn show(&mut self, ui: &mut Ui) -> Option<Action> {
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
        // ensure that our column layout fills the view
        let initial_col_width = ui.available_width() / self.data.num_columns() as f32;

        // stop columns from resizing to smaller than the window
        let min_col_width = if style.spacing.interact_size.x > initial_col_width {
            style.spacing.interact_size.x
        } else {
            initial_col_width
        };

        // we put buttons in the header, so make sure that the vertical size of the header includes
        // the button size and the normal padding around buttons
        let header_height = style.spacing.interact_size.y + (2.0f32 * style.spacing.item_spacing.y);
        let mut action: Option<Action> = None;

        // FIXME: this will certainly break if there are no columns
        TableBuilder::new(ui)
            .striped(true)
            .stick_to_bottom(true)
            .columns(
                Column::initial(initial_col_width)
                    .at_least(min_col_width)
                    .clip(true),
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

impl Show for Schema {
    fn show(&mut self, ui: &mut Ui) -> Option<Action> {
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

// TODO: Show data sources/data source metadata, including parquet data if available

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
    fn inc(&self) -> Self;

    fn reset(&self) -> Self;

    fn format(&self) -> String;
}

impl SelectionDepth for SortState {
    fn inc(&self) -> Self {
        match self {
            SortState::Ascending => SortState::Descending,
            SortState::Descending => SortState::Ascending,
            SortState::NotSorted => SortState::Descending,
        }
    }

    fn reset(&self) -> Self {
        // one day, I'll be proficient enough with macros that they'll be worth the time...
        SortState::NotSorted
    }

    fn format(&self) -> String {
        match self {
            SortState::Descending => "\u{23f7}",
            SortState::Ascending => "\u{23f6}",
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
