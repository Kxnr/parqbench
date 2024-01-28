use datafusion::arrow::{datatypes::DataType, util::display::array_value_to_string};
use egui::{Context, Layout, Response, TextStyle, Ui, WidgetText};
use egui_extras::{Column, TableBuilder, TableRow};
use parquet::{
    basic::ColumnOrder,
    file::{
        metadata::{KeyValue, ParquetMetaData},
        reader::{FileReader, SerializedFileReader},
    },
};
use rfd::AsyncFileDialog;
use std::{fs::File, path::Path};

use crate::data::{DataFilters, ParquetData, SortState};
use crate::TableName;

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
    table_name: String,
    query: String,
}

impl QueryPane {
    pub fn new(filename: Option<String>, filters: DataFilters) -> Self {
        Self {
            filename: filename.unwrap_or_default(),
            query: filters.query.unwrap_or_default(),
            table_name: filters.table_name.to_string(),
        }
    }

    pub fn render(&mut self, ui: &mut Ui) -> Option<(String, DataFilters)> {
        ui.label("Filename:".to_string());
        ui.text_edit_singleline(&mut self.filename);

        ui.label("Table Name:".to_string());
        ui.text_edit_singleline(&mut self.table_name);

        ui.label("Query:".to_string());
        ui.text_edit_singleline(&mut self.query);

        let submit = ui.button("Apply");
        if submit.clicked() && !self.query.is_empty() {
            Some((
                self.filename.clone(),
                DataFilters {
                    query: Some(self.query.clone()),
                    table_name: TableName {
                        name: self.table_name.clone(),
                    },
                    ..Default::default()
                },
            ))
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
        if let Ok(file) = File::open(path) {
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
        //ui.set_width(ui.available_width());
        //ui.set_height(ui.available_height());
        //ui.set_max_size(ui.available_size());

        let style = ui.style().as_ref();

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

        let text_height = TextStyle::Body.resolve(style).size;

        let initial_col_width = (ui.available_width() - style.spacing.scroll.bar_width)
            / (self.data.num_columns() + 1) as f32;

        // stop columns from resizing to smaller than the window--remainder stops the last column
        // growing, which we explicitly want to allow for the case of large datatypes.
        let min_col_width = if style.spacing.interact_size.x > initial_col_width {
            style.spacing.interact_size.x
        } else {
            initial_col_width / 4.0
        };

        let header_height = style.spacing.interact_size.y + 2.0f32 * style.spacing.item_spacing.y;

        // https://github.com/emilk/egui/issues/3680
        let column = Column::initial(initial_col_width)
            .at_least(min_col_width)
            .resizable(true)
            .clip(true);

        let analyze_header = |mut table_row: TableRow<'_, '_>| {
            for field in self.data.schema().fields() {
                table_row.col(|ui| {
                    let column_label = if is_sorted_column(&sorted_column, field.name().to_string())
                    {
                        sorted_column.clone().unwrap()
                    } else {
                        SortState::NotSorted(field.name().to_string())
                    };
                    //ui.centered_and_justified(|ui| {
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
        };

        let analyze_rows = |mut table_row: TableRow<'_, '_>| {
            for data_col in self.data.columns() {
                let row_index = table_row.index();
                table_row.col(|ui| {
                    // while not efficient (as noted in docs) we need to display
                    // at most a few dozen records at a time (barring pathological
                    // tables with absurd numbers of columns) and should still
                    // have conversion times on the order of ns.
                    ui.with_layout(
                        if is_integer(data_col.data_type()) {
                            Layout::centered_and_justified(egui::Direction::LeftToRight)
                        } else if is_float(data_col.data_type()) {
                            Layout::right_to_left(egui::Align::Center)
                        } else {
                            Layout::left_to_right(egui::Align::Center)
                        }
                        .with_main_wrap(false),
                        |ui| {
                            let value: String =
                                array_value_to_string(data_col, row_index).unwrap_or_default();
                            /*
                            if is_float(data_col.data_type()) {
                                // convert string to floating point number
                                value = match value.parse::<f64>() {
                                    Ok(float) => format!("{float:0.4}"),
                                    Err(_) => value,
                                }
                            }
                            */
                            ui.label(value);
                        },
                    );
                });
            }
        };

        TableBuilder::new(ui)
            .striped(true) // false: takes all available height
            .stick_to_bottom(true)
            .columns(column, self.data.num_columns())
            .column(Column::remainder())
            .auto_shrink([false, false])
            .min_scrolled_height(300.0)
            .header(header_height, analyze_header)
            .body(|body| {
                let num_rows = self.data.num_rows();
                body.rows(text_height, num_rows, analyze_rows);
            });

        filters
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
    let opt_file_handle = AsyncFileDialog::new().pick_file().await;

    if let Some(file_handle) = opt_file_handle {
        Ok(file_handle.file_name())
    } else {
        Err("No file loaded.".to_string())
    }
}
