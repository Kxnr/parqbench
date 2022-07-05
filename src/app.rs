use eframe;
use datafusion::arrow;
use datafusion::arrow::util::display::{array_value_to_string};
use egui::{Response, WidgetText, Ui};
use egui_extras::{TableBuilder, Size};
use std::future::Future;
use std::marker::Send;

use rfd::AsyncFileDialog;
use std::sync::Arc;
use datafusion::prelude::*;
use arrow::record_batch::RecordBatch;
use tokio::sync::oneshot::error::TryRecvError;
use tracing_subscriber::registry::Data;


// TODO: this ain't it
static TEXT_HEIGHT: f32 = 12f32;


#[derive(PartialEq)]
pub enum MenuPanels { Schema, Info, Filter}

pub struct ColumnLabel {
    pub column: String,
    icon: String,
    sort_ascending: bool
}

impl Default for ColumnLabel {
    fn default() -> Self {
        Self {
            column: "".to_string(),
            icon: "\u{2195}".to_string(),
            sort_ascending: false,
        }
    }
}

impl SelectionDepth<String> for ColumnLabel {
    fn inc(&mut self) -> () {
        self.sort_ascending = !self.sort_ascending;
        self.icon = if self.sort_ascending {"^".to_string()} else {"v".to_string()};
    }

    fn reset(&mut self) -> () {
        // TODO: there's likely a more idiomatic way to do this--derive builder?
        self.icon = Default::default();
        self.sort_ascending = Default::default();
    }

    fn icon(&self) -> String {
        format!("{}\t{}", self.column, self.icon)
    }
}


// FIXME: made changes to derived
trait SelectionDepth<Icon> {
    fn inc(
        &mut self
    ) -> ();

    fn reset(
        &mut self
    ) -> ();

    fn icon(
        &self
    ) -> Icon where Icon: Into<WidgetText>;
}

trait ExtraInteractions {
    fn toggleable_value<Value: PartialEq>(
        &mut self,
        current_value: &mut Option<Value>,
        selected_value: Value,
        text: impl Into<WidgetText>,
    ) -> Response;

    fn sort_button<Value: PartialEq + SelectionDepth<Icon>, Icon: Into<WidgetText>> (
        &mut self,
        current_value: &mut Option<Value>,
        selected_value: Value,
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

    fn sort_button<Value: PartialEq + SelectionDepth<Icon>, Icon: Into<WidgetText>> (
        &mut self,
        current_value: &mut Option<Value>,
        selected_value: Value,
    ) -> Response {
        let selected = match current_value {
            Some(value) => *value == selected_value,
            None => false,
        };
        let mut response = self.selectable_label(selected, selected_value.icon());
        if response.clicked() {
            if selected {
                current_value.unwrap().inc();
            } else {
                if let Some(value) = current_value {
                    value.reset();
                }
                *current_value = Some(selected_value);
            };
            response.mark_changed();
        }
        response
    }
}

fn concat_record_batches(batches: Vec<RecordBatch>) -> RecordBatch {
    RecordBatch::concat(&batches[0].schema(), &batches).unwrap()
}

struct ParquetTable {
    pub data: ParquetData,
    pub filters: DataFilters,
    labels: Vec<ColumnLabel>
}

impl ParquetTable {
    fn update_data(&mut self, filters: DataFilters) -> Self {
        todo!()
    }

    fn create_parquet_table(&mut self) -> Self {
        todo!()
    }

    fn render_parquet_table(&self, ui: &mut Ui) {
        TableBuilder::new(ui)
            .striped(true)
            // TODO: set sizes in font units
            // TODO: set widths by type, max(type_width, total_space/columns)
            .columns(Size::initial(8.0* TEXT_HEIGHT).at_least(8.0* TEXT_HEIGHT), self.data.data.num_columns())
            .resizable(true)
            .header(TEXT_HEIGHT * 1.5, |mut header| {
                for field in self.data.data.schema().fields() {
                    header.col(|ui| {
                        // TODO: sort with arrow, use 3 position switch
                        let response = ui.sort_button(format!("{}\t\u{2195}", field.name()));
                        if response.clicked {
                            // update filters
                        };
                    });
                }
            })
            .body(|body| {
                body.rows(TEXT_HEIGHT, self.data.data.num_rows() as usize, |row_index, mut row| {
                    for data_col in self.data.data.columns() {
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
            })
    }

}



pub struct DataFilters {
    sort: Option<String>,
    table_name: String,
    ascending: bool,
    query: Option<String>,
}


// TODO: serialize this for the query pane
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
        let ctx = SessionContext::new();
        ctx.register_parquet(filters.table_name.as_str(), filename, ParquetReadOptions::default()).await.ok();
        // FIXME: may be missing query
        match ctx.sql(filters.query.as_ref().unwrap().as_str()).await.ok() {
            Some(df) => {
                if let Some(data) = df.collect().await.ok() {
                    let data = concat_record_batches(data);
                    Some(ParquetData { filename: filename.to_string(), data: data, dataframe: df })
                } else {
                    // FIXME: syntax errors wipe data
                    None
                }
            },
            None => {
                None
            }
        }
    }

    pub async fn sort(self, filters: &DataFilters) -> Self {
        // FIXME: unsafe unwrap
        let df = self.dataframe.sort(vec![col(filters.sort.as_ref().unwrap().as_str()).sort(filters.ascending, false)]);

        // FIXME: panic on bad query
        match df.ok() {
            Some(df) => {
                ParquetData { filename: self.filename, data: self.data, dataframe: df }
            },
            None => {
                self
            }
        }
    }

    pub async fn load(filename: String) -> Option<Self> {
        let ctx = SessionContext::new();
        match ctx.read_parquet(&filename, ParquetReadOptions::default()).await.ok() {
            Some(df) => {
                let data = df.collect().await.unwrap();
                let data = concat_record_batches(data);
                Some(ParquetData { filename: filename.to_string(), data: data, dataframe: df })
            },
            None => {
                None
            }
        }
    }
}

async fn file_dialog() -> String {
    let file = AsyncFileDialog::new()
       .pick_file()
       .await;

    // TODO: handle wasm file object
    // FIXME: unsafe unwraps
    file.unwrap().inner().to_str().unwrap().to_string()
}

pub struct ParqBenchApp {
    pub menu_panel: Option<MenuPanels>,
    pub table: Option<ParquetTable>,

    // accessed only by methods
    runtime: tokio::runtime::Runtime,
    pipe: Option<tokio::sync::oneshot::Receiver<Option<ParquetData>>>,
}

impl Default for ParqBenchApp {
    fn default() -> Self {
        Self {
            // ui elements
            menu_panel: None,
            table: None,

            // table_name: "main".to_string(),
            runtime: tokio::runtime::Builder::new_multi_thread().worker_threads(1).enable_all().build().unwrap(),
            pipe: None,
        }
    }
}

impl ParqBenchApp {
    // TODO: re-export load, query, and sort. Need to update ui elements on load (columns), as well as filters
    pub fn new(_cc: &eframe::CreationContext<'_>) -> Self {
        Default::default()
    }

    pub fn data_pending(&mut self) -> bool {
        // hide implementation details of waiting for data to load
        return match &mut self.pipe {
            Some(output) => {
                match output.try_recv() {
                    Ok(data) => {
                        self.data = data;
                        self.pipe = None;
                        false
                    },
                    // TODO: match empty and closed
                    Err(e) => match e {
                        TryRecvError::Empty => {
                            true
                        },
                        TryRecvError::Closed => {
                            true
                        }
                    }
                }
            },
            _ => {
                false
            }
        }
    }


    // TODO: need to limit to data futures
    pub fn run_data_future<F>(&mut self, future: F, ctx: &egui::Context) -> ()
    where F: Future<Output=Option<ParquetData>> + Send + 'static,
    {
        if self.data_pending() {
            // FIXME, use vec of tasks?
            panic!("Cannot schedule future when future already running");
        }
        let (tx, rx) = tokio::sync::oneshot::channel::<Option<ParquetData>>();
        self.pipe = Some(rx);

        async fn inner<F>(future: F, ctx: egui::Context, tx: tokio::sync::oneshot::Sender<F::Output>)
        where F: Future<Output=Option<ParquetData>> + Send
        {
            let data = future.await;
            let result = tx.send(data);
            ctx.request_repaint();
        }

        // clones of ctx are cheap
        self.runtime.spawn(inner::<F>( future, ctx.clone(), tx));
    }
}

impl eframe::App for ParqBenchApp {
    // TODO: load partitioned dataset
    // TODO: fill in side panels
    // TODO: panel layout improvement
    // TODO: better file dialog (flow for querying data before load)
    // TODO: notification of loading failures, extend Option to have error, empty, and full states?
    // TODO: show 'drag file to load' before loaded
    // TODO: parse pandas format metadata
    // TODO: confirm exit

    fn update(&mut self, ctx: &egui::Context, frame: &mut eframe::Frame) {
        if !ctx.input().raw.dropped_files.is_empty() {
            // FIXME: unsafe unwraps
            let file: egui::DroppedFile = ctx.input().raw.dropped_files.last().unwrap().clone();
            let filename = file.path.unwrap().to_str().unwrap().to_string();
            self.run_data_future(ParquetData::load(filename), ctx);
        }

        egui::TopBottomPanel::top("top_panel").show(ctx, |ui| {
            egui::menu::bar(ui, |ui| {
                ui.menu_button("File", |ui| {
                    ui.menu_button("About", |ui| {
                        ui.label("Built with egui");
                    });

                    if ui.button("Open...").clicked() {
                        // TODO: move fully into state struct
                        let filename = self.runtime.block_on(file_dialog());
                        self.run_data_future(ParquetData::load(filename), ctx);
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
                    let _ = ui.toggleable_value(&mut self.menu_panel, MenuPanels::Schema, "\u{FF5B}");
                    let _ = ui.toggleable_value(&mut self.menu_panel, MenuPanels::Info, "\u{2139}");
                    let _ = ui.toggleable_value(&mut self.menu_panel, MenuPanels::Filter, "\u{1F50E}");
                });

                match &self.menu_panel {
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
                                    ui.label("Not Yet Implemented");
                                    ui.set_enabled(false);
                                    ui.label("Table Name");
                                    // ui.text_edit_singleline(&mut self.filters.table_name);
                                    ui.label("SQL Query");
                                    // ui.text_edit_singleline(&mut self.filters.query);
                                    // TODO: input, update data, output for errors
                                    // button
                                    // self.data.query(self.filters)
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
                match &self.table {
                    Some(table) => { ui.label(&format!("{:#?}", table.data.filename)); },
                    None => { ui.label("no file set"); },
                }
                egui::warn_if_debug_build(ui);
            });
        });


        egui::CentralPanel::default().show(ctx, |ui| {
            // The central panel the region left after adding TopPanel's and SidePanel's
            // let TEXT_HEIGHT = egui::TextStyle::Body.resolve(ui.style()).size;

            if self.data_pending() {
                ui.spinner();
                ui.set_enabled(false);
            }

            egui::ScrollArea::horizontal().show(ui, |ui| {
                if let Some(datatable = &self.table {
                    table.render_table(ui)
                }
                else {
                    ui.label("Drag and drop file, or use load file dialog");
                }

            });
        });

    }
}
