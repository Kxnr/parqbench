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
    sort: Option<String>,
    ascending: bool,

    // rather than move all data, just change our indexing to sort data
    sort_map: Option<arrow::array::UInt32Array>
}

impl Default for DataFilters {
   fn default() -> Self {
       Self {
           sort: None,
           ascending: true,
           sort_map: None
       }
   }
}

// TODO: does this need to be Send + Sync?
pub struct ParqBenchApp<'a> {
    menu_panel: Option<MenuPanels>,
    table_name: &'a str,

    // Execution context gives us metadata that we can't get from RecordBatch,
    // and lets us re-query with sql.
    // TODO: maybe better to store filename, metadata, and data? we can always re-construct the
    // TODO: context, though we have to construct it to get metadata in the first place.
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
        self.filters = DataFilters::default();

        // FIXME: for the moment, we load all data. This will be updated to pop out the
        // query panel, include a default query, and prompt to query data in case dataset is
        // too large for loading directly. Data must be loaded into memory, as this project
        // prioritizes the ability to sort/filter data over the ability to handle the very
        // largest of datasets. While this could be done on disk, the hybrid of rendered data in
        // memory and source on disk is deemed a suitable compromise for the moment.
        self.data = ExecutionContext::new().read_parquet(filename).collect().await?;
        callback();
    }

    pub async fn update_data_query(&mut self, query: &str, callback: fn() -> ()) {
        // cloning the execution context is cheap by design
        if let Some(mut data) = self.datasource.clone() {
            let data=  {
                data.sql(&self.filters.query).await.ok().unwrap()
            };
            // else {
            //     // FIXME: this is horrible
            //     data.sql(format!("SELECT * FROM {}", self.table_name).as_str()).await.ok().unwrap()
            // };

            // let data = if let Some(sort) = &self.filters.sort {
            //     // TODO: make nulls first a configurable parameter
            //     // TODO: check that column still exists post-query
            //     data.sort(vec![col(sort).sort(*self.filters.ascending, false)]).ok().unwrap()
            // } else {
            //     data
            // };

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
        if let Some(filter_col) = *self.filters.sort {
            // TODO: potential panic if filters and data are out of sync
            let filter_index = *self.data.ok().schema().index_of(filter_col);

            // though it requires jumping through some hoops, we store the indices rather than
            // actually twiddling data. TODO: make data struct to encapsulate this mask.
            self.filters.sort_map = arrow::compute::sort_to_indices(*self.data.ok().column(filter_index), None, None).ok();
        }
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

    pub fn format_value(&self, col: &str, row: usize) -> &str {
        let row = if let Some(map) = *self.filters.sort_map {
            map[row]
        } else {
            row
        };

        array_value_to_string(col, row).ok().as_str()
    }
}

impl eframe::App for ParqBenchApp {
    // TODO: move data loading to separate thread and add wait spinner
    // TODO: ^ pt. 2, ad-hoc re-filtering of data
    // TODO: load partitioned dataset
    // TODO: fill in side panels
    // TODO: panel layout improvement

    fn update(&mut self, ctx: &egui::Context, frame: &mut eframe::Frame) {

        if !ctx.input().raw.dropped_files.is_empty() {
            self.run_future( self.load_datasource( ctx.input().raw.dropped_files[0], ctx.request_repaint) );
        }

        egui::TopBottomPanel::top("top_panel").show(ctx, |ui| {
            egui::menu::bar(ui, |ui| {
                ui.menu_button("File", |ui| {
                    ui.menu_button("About", |ui| {
                        ui.label("Built with egui");
                    });

                    if ui.button("Open...").clicked() {
                        let table = FileDialog::new()
                                .set_location("~")
                                .show_open_single_file()
                                .unwrap();

                        self.run_future( self.load_datasource(&table.unwrap().to_str().unwrap(), ctx.request_repaint) );

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
                    let _ = ui.toggleable_value(*self.menu_panel, MenuPanels::Schema, "\u{FF5B}");
                    let _ = ui.toggleable_value(*self.menu_panel, MenuPanels::Info, "\u{2139}");
                    let _ = ui.toggleable_value(*self.menu_panel, MenuPanels::Filter, "\u{1F50E}");
                });

                match *self.menu_panel {
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
                                    //     match metadata {
                                    //         Some(data) => {
                                    //             // set ui to vertical, maybe better with heading
                                    //             ui.label(format!("Total rows: {}", data.file_metadata().num_rows()));
                                    //             ui.label(format!("Parquet version: {}", data.file_metadata().version()));
                                    //             // TODO:
                                    //         },
                                    //         _ => {}
                                    // }
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
                // match self.datasource {
                //     Some(table) => { ui.label(&format!("{:#?}", table)); },
                //     None => { ui.label("no file set"); },
                // }
                egui::warn_if_debug_build(ui);
            });
        });


        // TODO: table
        egui::CentralPanel::default().show(ctx, |ui| {
            // The central panel the region left after adding TopPanel's and SidePanel's
            let text_height = egui::TextStyle::Body.resolve(ui.style()).size;

            if self.data_pending() {
                // TODO: prevent/handle simultaneous data loads
                ui.spinner();
                ui.set_enabled(false);

            }

            egui::ScrollArea::horizontal().show(ui, |ui| {
                if let None = self.data {
                    return;
                }

                TableBuilder::new(ui)
                    .striped(true)
                    // TODO: set sizes in font units
                    // TODO: set widths by type, max(type_width, total_space/columns)
                    // TODO: show 'drag file to load' before loaded
                    .columns(Size::initial(8.0*text_height).at_least(8.0*text_height), self.data.as_ref().unwrap().num_columns())
                    .resizable(true)
                    .header(text_height * 4.0, |mut header| {
                        for field in self.data.as_ref().unwrap().schema().fields() {
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
