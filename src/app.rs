use eframe;
use datafusion::arrow;
use datafusion::arrow::util::display::{array_value_to_string};
use egui::{Response, WidgetText, Ui};
use egui_extras::{TableBuilder, Size};
use std::future::{Future, Ready};
use std::marker::Send;

// TODO: replace with rfd
use native_dialog::FileDialog;
use std::fs::File;
use datafusion::prelude::*;
use tokio::runtime::Runtime;
use tracing_subscriber::registry::Data;
use arrow::record_batch::RecordBatch;


#[derive(PartialEq)]
enum MenuPanels { Schema, Info, Filter}

trait SelectionDepth {
    fn inc(
        &mut self
    ) -> Self;

    fn depth<Depth: PartialEq>(
        &mut self
    ) -> Depth;

    fn reset(
        &mut self
    ) -> Self;

    fn icon<Icon: Into<WidgetText>>(
        &mut self
    ) -> Icon;
}

trait ExtraInteractions {
    fn toggleable_value<Value: PartialEq>(
        &mut self,
        current_value: &mut Option<Value>,
        selected_value: Value,
        text: impl Into<WidgetText>,
    ) -> Response;

    // fn sort_button<Value: PartialEq + SelectionDepth> (
    //     &mut self,
    //     current_value: &mut Option<Value>,
    //     selected_value: Value,
    // ) -> Response;
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

pub struct DataFilters {
    query: Option<String>,
    sort: Option<String>,
    ascending: bool,
}

impl Default for DataFilters {
   fn default() -> Self {
       Self {
           query: None,
           sort: None,
           ascending: true,
       }
   }
}

// TODO: does this need to be Send + Sync?
pub struct ParqBenchApp<'a> {
    menu_panel: Option<MenuPanels>,
    table_name: &'a str,
    datasource: Option<ExecutionContext>,
    data: Option<RecordBatch>,
    filters: DataFilters,

    // accessed only by methods
    runtime: tokio::runtime::Runtime,
    task: Option<Future>,
}

impl Default for ParqBenchApp {
    fn default() -> Self {
        Self {
            menu_panel: None,
            table_name: "main",
            datasource: None,
            data: None,
            filters: DataFilters::default(),
            runtime: tokio::runtime::Builder::new_multi_thread().worker_threads(1).enable_all().build().unwrap(),
            task: None,
        }
    }
}

impl ParqBenchApp {
    pub fn new(_cc: &eframe::CreationContext<'_>) -> Self {
        Default::default()
    }

    // TODO: encapsulate queing to runtime
    pub async fn load_datasource(&mut self, filename: &str, callback: fn() -> ()) {
        // TODO: handle data loading errors
        let mut ctx = ExecutionContext::new();
        ctx.register_parquet(&self.table_name, filename).await.ok();
        self.datasource = Some(ctx);
        callback();
    }

    pub async fn update_data_query(&mut self, callback: fn() -> ()) {
        // cloning the execution context is cheap by design
        if let Some(mut data) = self.datasource.clone() {
            let data = if let Some(query) = &self.filters.query {
                data.sql(query).await.ok().unwrap()
            } else {
                // FIXME: this is horrible
                data.sql(format!("SELECT * FROM {}", self.table_name).as_str()).await.ok().unwrap()
            };

            let data = if let Some(sort) = &self.filters.sort {
                // TODO: make nulls first a configurable parameter
                data.sort(vec![col(sort).sort(*self.filters.ascending, false)]).ok().unwrap()
            } else {
                data
            };

            let data = data.collect().await.ok();
            match data {
                Some(data) => {
                    self.data = RecordBatch::concat(&data[0].schema(), &data).ok();
                }
                _ => {
                    self.data = None
                }
            }

            callback();
        }
    }

    pub async fn sort_data(&mut self) -> () {
        // TODO: use kernel to sort loaded data

    }

    pub fn clear_filters(&mut self) {
        self.filters = DataFilters::default();
    }

    pub fn data_pending(&self) -> bool {
        // hide implementation details of waiting for data to load
        if let Some(task) = *self.task {
            if task.poll().is_ready() {
                *self.task = None;
                false
            }
        }
        else {
            true
        }
    }

    pub fn run_future<F>(&mut self, future: F) -> ()
    where F: Future + Send,
          F::Output: Send
    {
        if self.data_pending() {
            // FIXME
            panic!("Cannot schedule future when future already running");
        }
        self.task = Some(self.runtime.spawn(future));
    }
}

impl eframe::App for ParqBenchApp {
    // TODO: move data loading to separate thread and add wait spinner
    // TODO: ^ pt. 2, ad-hoc re-filtering of data
    // TODO: load partitioned dataset
    // TODO: fill in side panels
    // TODO: panel layout improvement

    fn update(&mut self, ctx: &egui::Context, frame: &mut eframe::Frame) {
        let Self {table, menu_panel, metadata, data} = self;

        if self.data_pending() {

        }

        if !ctx.input().raw.dropped_files.is_empty() {
            // TODO: load data
            println!("dropped: {:?}", ctx.input().raw.dropped_files)
        }

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
                            let rt = Runtime::new().unwrap();
                            let loaded = rt.block_on(load_parq(table.as_ref().unwrap().to_str().unwrap()));
                            *data = loaded.ok();
                            // println!("{}", data.as_ref().unwrap()[0].schema().to_json().to_string());
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
                                    ui.heading("Schema");
                                });
                            },
                            MenuPanels::Info => {
                                egui::ScrollArea::vertical().show(ui, |ui| {
                                    ui.vertical(|ui| {
                                        ui.heading("File Info");
                                        match metadata {
                                            Some(data) => {
                                                // set ui to vertical, maybe better with heading
                                                    ui.label(format!("Total rows: {}", data.file_metadata().num_rows()));
                                                    ui.label(format!("Parquet version: {}", data.file_metadata().version()));
                                                // TODO:
                                            },
                                            _ => {}
                                    }
                                    });

                                    // rows
                                    // columns
                                    // data size
                                    // compressed size
                                    // compression method
                                });
                            },
                            MenuPanels::Filter => {
                                egui::ScrollArea::vertical().show(ui, |ui| {
                                    ui.label("Filter");
                                    // TODO: input, update data, output for errors
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
            let text_height = egui::TextStyle::Body.resolve(ui.style()).size;

            egui::ScrollArea::horizontal().show(ui, |ui| {
                if let None = data {
                    return;
                }

                TableBuilder::new(ui)
                    .striped(true)
                    // TODO: set sizes in font units
                    // TODO: set widths by type, max(type_width, total_space/columns)
                    // TODO: show 'drag file to load' before loaded
                    .columns(Size::initial(8.0*text_height).at_least(8.0*text_height), data.as_ref().unwrap().num_columns())
                    .resizable(true)
                    .header(text_height * 4.0, |mut header| {
                        for field in data.as_ref().unwrap().schema().fields() {
                            header.col(|ui| {
                                // TODO: sort with arrow, use 3 position switch
                                ui.button("\u{2195}");
                                ui.label(format!("{}", field.name()));
                            });
                        }
                    })
                    .body(|body| {
                        body.rows(text_height, metadata.as_ref().unwrap().file_metadata().num_rows() as usize, |row_index, mut row| {
                            let _data = data.as_ref().unwrap();
                            for data_col in _data.columns() {
                                row.col(|ui| {
                                    // while not efficient (as noted in docs) we need to display
                                    // at most a few dozen records at a time (barring pathological
                                    // tables with absurd numbers of columns) and should still
                                    // have conversion times on the order of ns.
                                    let value = array_value_to_string(data_col, row_index).unwrap();
                                    ui.label( value );
                                });
                            }
                        });
                    });
            });
        });
    }
}
