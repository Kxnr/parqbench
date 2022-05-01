use eframe;
use datafusion::arrow;
use datafusion::arrow::util::display::{array_value_to_string};
use egui::{Response, WidgetText, Ui};
use egui_extras::{TableBuilder, StripBuilder, Size};
use datafusion::execution::context::ExecutionContext;
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
use datafusion::parquet::file::metadata::ParquetMetaData;

use native_dialog::FileDialog;
use std::path::PathBuf;
use std::fs::File;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use tokio::runtime::Runtime;


#[derive(PartialEq)]
enum MenuPanels { Schema, Info, Filter}

trait ExtraInteractions {
    fn toggleable_value<Value: PartialEq>(
        &mut self,
        current_value: &mut Option<Value>,
        selected_value: Value,
        text: impl Into<WidgetText>,
    ) -> Response;
}

impl ExtraInteractions for Ui {
    fn toggleable_value<Value: PartialEq>(
        &mut self,
        current_value: &mut Option<Value>,
        selected_value: Value,
        text: impl Into<WidgetText>,
    ) -> Response {
        let selected = match current_value {
            Some(value) => *value == selected_value,
            None => false,
        };
        let mut response = self.selectable_label(selected, text);
        if response.clicked() {
            *current_value = if selected {None} else {Some(selected_value)};
            response.mark_changed();
        }
        response
    }
}


/// We derive Deserialize/Serialize so we can persist app state on shutdown.
pub struct ParqBenchApp {
    table: Option<PathBuf>,
    menu_panel: Option<MenuPanels>,
    metadata: Option<ParquetMetaData>,
    data: Option<Vec<arrow::record_batch::RecordBatch>>,
}

impl Default for ParqBenchApp {
    fn default() -> Self {
        Self {
            table: None,
            menu_panel: None,
            metadata: None,
            data: None
        }
    }
}

impl ParqBenchApp {
    pub fn new(cc: &eframe::CreationContext<'_>) -> Self {
        // TODO: set font size constants
        Default::default()
    }
}

async fn print_parq(filename: &str) -> Result<Vec<RecordBatch>, DataFusionError> {
    let mut ctx = ExecutionContext::new();
    let df = ctx.read_parquet(filename).await?;
    let result: Vec<RecordBatch> = df.limit(100)?.collect().await?;
    let pretty_result = arrow::util::pretty::pretty_format_batches(&result)?;
    println!("{}", pretty_result);

    return Ok(result);
}

impl eframe::App for ParqBenchApp {
    // TODO: format rows in df to rows in gui
    // TODO: deal with paging/scrolling
    // TODO: move data loading to separate thread and add wait spinner
    // TODO: fill in side panels
    // TODO: extend format compatibility

    fn update(&mut self, ctx: &egui::Context, frame: &mut eframe::Frame) {
        let Self {table, menu_panel, metadata, data} = self;

        egui::TopBottomPanel::top("top_panel").show(ctx, |ui| {
            egui::menu::bar(ui, |ui| {
                ui.menu_button("File", |ui| {
                    ui.menu_button("About", |ui| {
                        ui.label("Built with egui");
                    });

                    if ui.button("Open...").clicked() {
                        *table = FileDialog::new()
                            .set_location("~")
                            .show_open_single_file()
                            .unwrap();

                        if let Ok(file) = File::open(table.as_ref().unwrap()) {
                            // use parquet to get metadata, reading is done by datafusion
                            let reader = SerializedFileReader::new(file).unwrap();
                            *metadata = Some(reader.metadata().clone());
                            let rt = Runtime::new().unwrap();
                            let loaded = rt.block_on(print_parq(table.as_ref().unwrap().to_str().unwrap()));
                            // TODO: load in separate thread, allow passing in query to reload
                            *data = loaded.ok();
                            println!("{}", data.as_ref().unwrap()[0].schema().to_json().to_string());
                        }

                        ui.close_menu();
                    }

                    if ui.button("Quit").clicked() {
                        frame.quit();
                    }
                });
            });
        });


        egui::SidePanel::left("side_panel").min_width(0f32).max_width(400f32).resizable(true).show(ctx, |ui| {

            ui.horizontal_top(|ui| {
                ui.vertical(|ui| {
                    // TODO: tooltips
                    let _ = ui.toggleable_value(menu_panel, MenuPanels::Schema, "\u{FF5B}");
                    let _ = ui.toggleable_value(menu_panel, MenuPanels::Info, "\u{2139}");
                    let _ = ui.toggleable_value(menu_panel, MenuPanels::Filter, "\u{1F50E}");
                });

                match menu_panel {
                    // TODO: looks like CollapsingState can accomplish this nicely
                    Some(panel) => {
                        match panel {
                            MenuPanels::Schema => {
                                egui::ScrollArea::vertical().show(ui, |ui| {
                                    ui.label("schema menu");
                                });
                            },
                            MenuPanels::Info => {
                                egui::ScrollArea::vertical().show(ui, |ui| {
                                    ui.label("info menu");
                                    match metadata {
                                        Some(data) => {
                                            // set ui to vertical, maybe better with heading
                                            ui.label(format!("Total rows: {}", data.file_metadata().num_rows()));
                                            ui.label(format!("Parquet version: {}", data.file_metadata().version()));
                                        },
                                        _ => {}
                                    }

                                    // rows
                                    // columns
                                    // data size
                                    // compressed size
                                    // compression method
                                });
                            },
                            MenuPanels::Filter => {
                                egui::ScrollArea::vertical().show(ui, |ui| {
                                    ui.label("filter menu");
                                    ui.input();
                                });
                            }
                        }
                    },
                    _ => {},
                }
            });
        });


        egui::TopBottomPanel::bottom("bottom_panel").show(ctx, |ui| {
            ui.horizontal(|ui| {
                match table {
                    Some(table) => { ui.label(&format!("{:#?}", table)); },
                    None => { ui.label("no file set"); },
                }
                egui::warn_if_debug_build(ui);
            });
        });


        // TODO: table
        egui::CentralPanel::default().show(ctx, |ui| {
            // The central panel the region left after adding TopPanel's and SidePanel's
            egui::ScrollArea::horizontal().show(ui, |ui| {
                if let None = data {
                    return;
                }

                TableBuilder::new(ui)
                    .resizable(true)
                    .striped(true)
                    // TODO: set sizes in font units
                    // TODO: set widths by type, max(type_width, total_space/columns)
                    .columns(Size::initial(20.0).at_least(20.0), data.as_ref().unwrap()[0].num_columns())
                    .header(20.0, |mut header| {
                        for field in data.as_ref().unwrap()[0].schema().fields() {
                            header.col(|ui| {
                                ui.label(field.name());
                            });
                        }
                    })
                    .body(|mut body| {
                        body.rows(20.0, 30, |row_index, mut row| {
                            let mut offset = 0;
                            let _data = data.as_ref().unwrap();
                            for batch in _data {
                                if row_index <= offset + batch.num_rows() {
                                    for data_col in batch.columns() {
                                        row.col(|ui| {
                                            // while not efficient (as noted in docs) we need to display
                                            // at most a few dozen records at a time (barring pathological
                                            // tables with absurd numbers of columns) and should still
                                            // have conversion times on the order of ns.
                                            let value = array_value_to_string(data_col, row_index - offset).unwrap();
                                            ui.label( value );
                                        });
                                    }
                                } else {
                                    offset += batch.num_rows();
                                }
                            }
                        });
                    });
            });
        });
    }
}
