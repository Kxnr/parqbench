use eframe;
use datafusion::arrow;
use datafusion::arrow::util::display::{array_value_to_string};
use egui::{Response, WidgetText, Ui};
use egui_extras::{TableBuilder, Size};
use std::future::{Future, Ready};
use std::marker::Send;

// TODO: replace with rfd
use rfd::AsyncFileDialog;
use std::sync::Arc;
use datafusion::prelude::*;
use arrow::record_batch::RecordBatch;
use tokio::task::JoinHandle;


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

fn concat_record_batches(batches: Vec<RecordBatch>) -> RecordBatch {
    RecordBatch()
}

pub struct DataFilters {
    sort: Option<String>,
    table_name: String,
    ascending: bool,
    query: Option<String>,
}

impl Default for DataFilters {
   fn default() -> Self {
       Self {
           sort: None,
           table_name: "main".to_string(),
           ascending: true,
           query: None,
       }
   }
}


pub struct ParquetData {
    pub filename: String,
    // pub metadata: ??,
    pub data: RecordBatch,
    dataframe: Arc<DataFrame>
}

impl ParquetData {
    pub async fn query(filename: &str, filters: &DataFilters) -> Option<Self> {
        let mut ctx = SessionContext::new();
        ctx.register_parquet(filters.table_name.as_str(), filename, ParquetReadOptions::default()).await.ok();
        // FIXME
        match ctx.sql(filters.query.unwrap().as_str()).await.ok() {
            Some(df) => {
                if let Some(data) = df.collect().await.ok() {
                    let data = concat_record_batches(data);
                    Some(self::ParquetData { filename: filename.to_string(), data: data, dataframe: df })
                }
            },
            None => {
                None
            }
        }
    }

    pub async fn sort(self, &filters: DataFilters) -> Self {
        let df = self.df.sort(vec![col(filters.sort_col).sort(filters.ascending, false)]);
        match df {
            Some(df) => {
                self::ParquetData { filename: self.filename, data: self.data, dataframe: df }
            },
            None => {
                self
            }
        }
    }

    pub async fn load(filename: &str) -> Option<Self> {
        let mut ctx = SessionContext::new();
        match ctx.read_parquet(filename, ParquetReadOptions::default()).await.ok() {
            Some(df) => {
                let data = df.collect().await.unwrap();
                let data = concat_record_batches(data);
                Some(self::ParquetData { filename: filename.to_string(), data: data, dataframe: df })
            },
            None => {
                None
            }
        }
    }

    // fn update_metadata() {
    //
    // }
}

pub struct ParqBenchApp {
    pub menu_panel: Option<MenuPanels>,

    // Execution context gives us metadata that we can't get from RecordBatch,
    // and lets us re-query with sql.
    pub data: Option<ParquetData>,
    pub filters: DataFilters,

    // accessed only by methods
    runtime: tokio::runtime::Runtime,
    task: Option<JoinHandle<_>>,
}

impl Default for ParqBenchApp {
    fn default() -> Self {
        Self {
            menu_panel: None,
            // table_name: "main".to_string(),
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

    pub fn clear_filters(&mut self) {
        self.filters = DataFilters::default();
    }

    pub fn future_pending(&self) -> bool {
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

    pub fn run_future<F>(&mut self, future: F, ctx: &egui::Context) -> ()
    where F: Future + Send,
          F::Output: Send
    {
        async fn inner<F>(future: F, ctx: &egui::Context) {
            future.await;
            ctx.request_repaint();
        }

        if self.data_pending() {
            // FIXME, use vec of tasks?
            panic!("Cannot schedule future when future already running");
        }
        self.task = Some(self.runtime.spawn(inner::<F>(future, ctx)));
    }

    pub fn format_value(&self, col: &str, row: usize) -> &str {
        if let Some(da) = *self.data {
            let col = da[col];
            array_value_to_string(col, row).ok().as_str()
        }
        ""
    }
}

impl eframe::App for ParqBenchApp {
    // TODO: move data loading to separate thread and add wait spinner
    // TODO: load partitioned dataset
    // TODO: fill in side panels
    // TODO: panel layout improvement
    // TODO: better file dialog
    // TODO: notification of loading failures

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
                                    ui.label("Not Yet Implemented");
                                });
                            },
                            MenuPanels::Info => {
                                egui::ScrollArea::vertical().show(ui, |ui| {
                                    ui.vertical(|ui| {
                                        ui.heading("File Info");
                                        ui.label("Not Yet Implemented");
                                    });
                                });
                            },
                            MenuPanels::Filter => {
                                egui::ScrollArea::vertical().show(ui, |ui| {
                                    ui.label("Filter");
                                    // ui.text_edit_singleline();
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
