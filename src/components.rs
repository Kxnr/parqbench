use crate::data::{DataFilters, ParquetData, SortState};
use datafusion::arrow::util::display::array_value_to_string;
use egui::{Response, Ui, WidgetText};
use egui_extras::{Size, TableBuilder};
use rfd::AsyncFileDialog;
use tracing_subscriber::fmt::init;

impl DataFilters {
    fn render(&self, ui: &mut Ui) {
        // how to cache fields?
        // table name
        // column
        // sort direction
        // query
        // load/refresh button
    }
}

impl ParquetData {
    pub fn render_schema(&self, ui: &mut Ui) {
        todo!();
        // includes df.schema() and df.metadata()
    }

    pub fn render_table(&self, ui: &mut Ui) -> Option<DataFilters> {
        let style = &ui.style().clone();

        fn is_sorted_column(sorted_col: &Option<SortState>, col: String) -> bool {
            match sorted_col {
                Some(sort) => match sort {
                    SortState::Ascending(sorted_col) => *sorted_col == col,
                    SortState::Descending(sorted_col) => *sorted_col == col,
                    _ => false,
                },
                None => false,
            }
        }

        let mut filters: Option<DataFilters> = None;
        let mut sorted_column = self.filters.sort.clone();

        let text_height = egui::TextStyle::Body.resolve(style).size;

        let initial_col_width = (ui.available_width() - style.spacing.scroll_bar_width)
            / self.data.num_columns() as f32;

        // stop columns from resizing to smaller than the window--remainder stops the last column
        // growing, which we explicitly want to allow for the case of large datatypes.
        let min_col_width = if style.spacing.interact_size.x > initial_col_width {
            style.spacing.interact_size.x
        } else {
            initial_col_width
        };

        let header_height = style.spacing.interact_size.y + (2.0f32 * style.spacing.item_spacing.y);

        // FIXME: this will certainly break if there are no columns
        TableBuilder::new(ui)
            .striped(true)
            .stick_to_bottom(true)
            .clip(true)
            .columns(
                Size::initial(initial_col_width).at_least(min_col_width),
                self.data.num_columns(),
            )
            .resizable(true)
            .header(header_height, |mut header| {
                for field in self.data.schema().fields() {
                    header.col(|ui| {
                        let column_label =
                            if is_sorted_column(&sorted_column, field.name().to_string()) {
                                sorted_column.clone().unwrap()
                            } else {
                                SortState::NotSorted(field.name().to_string())
                            };
                        ui.horizontal_centered(|ui| {
                            let response = ui.sort_button(&mut sorted_column, column_label.clone());
                            if response.clicked() {
                                filters = Some(DataFilters {
                                    sort: sorted_column.clone(),
                                    ..self.filters.clone()
                                });
                            }
                        });
                    });
                }
            })
            .body(|body| {
                body.rows(
                    text_height,
                    self.data.num_rows() as usize,
                    |row_index, mut row| {
                        for data_col in self.data.columns() {
                            row.col(|ui| {
                                // while not efficient (as noted in docs) we need to display
                                // at most a few dozen records at a time (barring pathological
                                // tables with absurd numbers of columns) and should still
                                // have conversion times on the order of ns.
                                ui.with_layout(
                                    egui::Layout::left_to_right(egui::Align::Center)
                                        .with_main_wrap(false),
                                    |ui| {
                                        let value =
                                            array_value_to_string(data_col, row_index).unwrap();
                                        ui.label(value);
                                    },
                                );
                            });
                        }
                    },
                );
            });
        filters
    }
}

impl SelectionDepth<String> for SortState {
    fn inc(&self) -> Self {
        match self {
            SortState::NotSorted(col) => SortState::Descending(col.to_owned()),
            SortState::Ascending(col) => SortState::Descending(col.to_owned()),
            SortState::Descending(col) => SortState::Ascending(col.to_owned()),
        }
    }

    fn reset(&self) -> Self {
        // one day, I'll be proficient enough with macros that they'll be worth the time...
        match self {
            SortState::NotSorted(col) => SortState::NotSorted(col.to_owned()),
            SortState::Ascending(col) => SortState::NotSorted(col.to_owned()),
            SortState::Descending(col) => SortState::NotSorted(col.to_owned()),
        }
    }

    fn format(&self) -> String {
        match self {
            SortState::Descending(col) => format!("\u{23f7} {}", col),
            SortState::Ascending(col) => format!("\u{23f6} {}", col),
            SortState::NotSorted(col) => format!("\u{2195} {}", col),
        }
    }
}

pub trait SelectionDepth<Icon> {
    fn inc(&self) -> Self;

    fn reset(&self) -> Self;

    fn format(&self) -> Icon
    where
        Icon: Into<WidgetText>;
}

pub trait ExtraInteractions {
    fn sort_button<Value: PartialEq + SelectionDepth<Icon>, Icon: Into<WidgetText>>(
        &mut self,
        current_value: &mut Option<Value>,
        selected_value: Value,
    ) -> Response;
}

impl ExtraInteractions for Ui {
    fn sort_button<Value: PartialEq + SelectionDepth<Icon>, Icon: Into<WidgetText>>(
        &mut self,
        current_value: &mut Option<Value>,
        selected_value: Value,
    ) -> Response {
        let selected = match current_value {
            Some(value) => *value == selected_value,
            None => false,
        };
        let mut response = self.selectable_label(selected, selected_value.format());
        if response.clicked() {
            if selected {
                *current_value = Some(selected_value.inc());
            } else {
                if let Some(value) = current_value {
                    value.reset();
                }
                *current_value = Some(selected_value.inc());
            };
            response.mark_changed();
        }
        response
    }
}

pub async fn file_dialog() -> String {
    let file = AsyncFileDialog::new().pick_file().await;

    // FIXME: unsafe unwraps
    file.unwrap().inner().to_str().unwrap().to_string()
}
