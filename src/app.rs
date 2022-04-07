use eframe::{egui, epi};
use datafusion::arrow;
use egui::{Response, WidgetText, Ui};
use datafusion::execution::context::ExecutionContext;
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
use datafusion::parquet::file::metadata::{ParquetMetaData};

use native_dialog::FileDialog;
use std::path::PathBuf;
use std::fs::File;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use tokio::runtime::Runtime;

pub const LOREM_IPSUM_LONG: &str = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Curabitur pretium tincidunt lacus. Nulla gravida orci a odio. Nullam varius, turpis et commodo pharetra, est eros bibendum elit, nec luctus magna felis sollicitudin mauris. Integer in mauris eu nibh euismod gravida. Duis ac tellus et risus vulputate vehicula. Donec lobortis risus a elit. Etiam tempor. Ut ullamcorper, ligula eu tempor congue, eros est euismod turpis, id tincidunt sapien risus a quam. Maecenas fermentum consequat mi. Donec fermentum. Pellentesque malesuada nulla a mi. Duis sapien sem, aliquet nec, commodo eget, consequat quis, neque. Aliquam faucibus, elit ut dictum aliquet, felis nisl adipiscing sapien, sed malesuada diam lacus eget erat. Cras mollis scelerisque nunc. Nullam arcu. Aliquam consequat. Curabitur augue lorem, dapibus quis, laoreet et, pretium ac, nisi. Aenean magna nisl, mollis quis, molestie eu, feugiat in, orci. In hac habitasse platea dictumst.";

#[derive(PartialEq)]
#[cfg_attr(feature = "persistence", derive(serde::Deserialize, serde::Serialize))]
enum MenuPanels { Schema, Info, Filter}

fn lorem_ipsum(ui: &mut egui::Ui) {
    ui.with_layout(
        egui::Layout::top_down(egui::Align::LEFT).with_cross_justify(true),
        |ui| {
            ui.label(egui::RichText::new(LOREM_IPSUM_LONG).small().weak());
        },
    );
}

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
// #[cfg_attr(feature = "persistence", derive(serde::Deserialize, serde::Serialize))]
// #[cfg_attr(feature = "persistence", serde(default))] // if we add new fields, give them default values when deserializing old state
pub struct TemplateApp {
    // #[cfg_attr(feature = "persistence", serde(skip))]
    table: Option<PathBuf>,
    menu_panel: Option<MenuPanels>,
    metadata: Option<ParquetMetaData>,
    data: Option<Vec<arrow::record_batch::RecordBatch>>,
}

impl Default for TemplateApp {
    fn default() -> Self {
        Self {
            // Example stuff:
            table: None,
            menu_panel: None,
            metadata: None,
            data: None
        }
    }
}

async fn print_parq(filename: &str) -> Result<bool, DataFusionError> {
    let mut ctx = ExecutionContext::new();
    let df = ctx.read_parquet(filename).await?;
    let result: Vec<RecordBatch> = df.limit(100)?.collect().await?;
    let pretty_result = arrow::util::pretty::pretty_format_batches(&result)?;
    println!("{}", pretty_result);

    return Ok(true);
}

impl epi::App for TemplateApp {
    fn name(&self) -> &str {
        "parqbench"
    }

    fn setup(
        &mut self,
        _ctx: &egui::Context,
        _frame: &epi::Frame,
        _storage: Option<&dyn epi::Storage>,
    ) {
        // #[cfg(feature = "persistence")]
        // if let Some(storage) = _storage {
        //     *self = epi::get_value(storage, epi::APP_KEY).unwrap_or_default()
        // }
    }

    // #[cfg(feature = "persistence")]
    // fn save(&mut self, storage: &mut dyn epi::Storage) {
    //     epi::set_value(storage, epi::APP_KEY, self);
    // }

    // TODO: format rows in df to rows in gui
    // TODO: deal with paging/scrolling
    // TODO: move data loading to separate thread and add wait spinner
    // TODO: fill in side panels
    // TODO: extend format compatibility

    fn update(&mut self, ctx: &egui::Context, frame: &epi::Frame) {
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
                            match loaded {
                                Ok(_) => {},
                                _ => {ui.label("failure loading");}
                            }
                        }
                    }

                    // TODO: load/initialize data
                    // TODO: check that this doesn't fail!?

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
                                            ui.label(format!("{}", data.file_metadata().num_rows()));
                                            ui.label(format!("{}", data.file_metadata().version()));
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
            egui::ScrollArea::vertical().show(ui, |ui| {
                lorem_ipsum(ui);
            });
        });

    }
}
