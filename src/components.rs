use crate::data::{DataFilters, ParquetData, SortState};
use crate::TableName;
use datafusion::arrow::util::display::array_value_to_string;
use egui::{Context, Response, Ui, WidgetText};
use egui_extras::{Size, TableBuilder};
use parquet::basic::ColumnOrder;
use parquet::file::metadata::{KeyValue, ParquetMetaData};
use parquet::file::reader::{FileReader, SerializedFileReader};
use rfd::AsyncFileDialog;
use std::fs::File;
use std::path::Path;

pub trait Popover {
    fn show(&mut self, ctx: &Context) -> bool;
}

pub struct Settings {}

impl Popover for Settings {
    fn show(&mut self, ctx: &Context) -> bool {
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

pub struct Error {
    pub message: String,
}

impl Popover for Error {
    fn show(&mut self, ctx: &Context) -> bool {
        let mut open = true;

        egui::Window::new("Error")
            .collapsible(false)
            .open(&mut open)
            .show(ctx, |ui| {
                ui.label(format!("Error: {}", self.message));
                ui.set_enabled(false);
            });

        open
    }
}

pub struct QueryPane {
    filename: String,
    table_name: TableName,
    query: String,
}

impl QueryPane {
    pub fn new(filename: Option<String>, filters: DataFilters) -> Self {
        Self {
            filename: filename.unwrap_or_default(),
            query: filters.query.unwrap_or_default(),
            table_name: filters.table_name,
        }
    }

    pub fn render(&mut self, ui: &mut Ui) -> Option<(String, DataFilters)> {
        ui.label("Filename:".to_string());
        ui.text_edit_singleline(&mut self.filename);

        ui.label("Table Name:".to_string());
        ui.text_edit_singleline(&mut self.table_name.to_string());

        ui.label("Query:".to_string());
        ui.text_edit_singleline(&mut self.query);

        let submit = ui.button("Apply");
        if submit.clicked() && !self.query.is_empty() {
            let filename = shellexpand::full(&self.filename);
            match filename {
                Ok(filename) => Some((
                    filename.to_string(),
                    DataFilters {
                        query: Some(self.query.clone()),
                        table_name: self.table_name.clone(),
                        ..Default::default()
                    },
                )),
                Err(_) => None,
            }
        } else {
            None
        }
    }
}

pub struct FileMetadata {
    info: ParquetMetaData,
}

impl FileMetadata {
    pub fn from_filename(filename: &str) -> Result<Self, String> {
        // while possible, digging this out of datafusion is immensely painful.
        // reading with parquet is mildly less so.
        let path = Path::new(filename);
        if let Ok(file) = File::open(&path) {
            let reader = SerializedFileReader::new(file).unwrap();
            Ok(Self {
                info: reader.metadata().to_owned(),
            })
        } else {
            Err("Could not read metadata from file.".to_string())
        }
    }

    // Lots of metadata available here, if anything else is of interest.

    pub fn render_metadata(&self, ui: &mut Ui) {
        let file_metadata = self.info.file_metadata();
        ui.label(format!("version: {}", file_metadata.version()));
        ui.label(format!(
            "created by: {}",
            file_metadata.created_by().unwrap_or("unknown")
        ));
        ui.label(format!("row groups: {}", self.info.num_row_groups()));
        ui.label(format!("rows: {}", file_metadata.num_rows()));
        ui.label(format!(
            "columns: {}",
            file_metadata.schema_descr().num_columns()
        ));
        if let Some(key_value) = file_metadata.key_value_metadata() {
            for KeyValue { key, value } in key_value {
                ui.label(format!(
                    "{}: {}",
                    key,
                    value.to_owned().unwrap_or_else(|| "-".to_string())
                ));
            }
        }
    }

    pub fn render_schema(&self, ui: &mut Ui) {
        let file_metadata = self.info.file_metadata();
        for (idx, field) in file_metadata.schema_descr().columns().iter().enumerate() {
            ui.collapsing(field.name(), |ui| {
                let field_type = field.self_type();
                let field_type = if field_type.is_primitive() {
                    format!("{}", field_type.get_physical_type())
                } else {
                    format!("{}", field.converted_type())
                };
                ui.label(format!("type: {}", field_type));
                ui.label(format!(
                    "sort_order: {}",
                    match file_metadata.column_order(idx) {
                        ColumnOrder::TYPE_DEFINED_ORDER(sort_order) => format!("{}", sort_order),
                        _ => "undefined".to_string(),
                    }
                ));
            });
        }
    }
}

impl ParquetData {
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

pub async fn file_dialog() -> Result<String, String> {
    let file = AsyncFileDialog::new().pick_file().await;

    // FIXME: unsafe unwraps
    if let Some(file) = file {
        file.path()
            .to_str()
            .map(|s| s.to_string())
            .ok_or_else(|| "Invalid characters in Path.".to_string())
    } else {
        Err("No file loaded.".to_string())
    }
}
