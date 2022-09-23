use eframe;
use datafusion::arrow::util::display::array_value_to_string;
use egui::{Response, Ui, WidgetText};
use egui_extras::{Size, TableBuilder};
use std::future::Future;
use std::marker::Send;

use rfd::AsyncFileDialog;
use tokio::sync::oneshot::error::TryRecvError;
use core::default::Default;
use std::thread::current;
use tracing_subscriber::registry::Data;

use crate::data::{DataFilters, ParquetData, SortState};

// TODO: create a layout struct with various sizes
static TEXT_HEIGHT: f32 = 12f32;

struct Layout {
    text_height: f32,
    tab_width: f32,
    column_width: f32
}

impl SelectionDepth<String> for SortState {
    fn inc(&self) -> Self {
        match self {
            SortState::NotSorted(col) => SortState::Descending(col.to_owned()),
            SortState::Ascending(col) => SortState::Descending(col.to_owned()),
            SortState::Descending(col) => SortState::Ascending(col.to_owned())
        }
    }

    fn reset(&self) -> Self {
        // one day, I'll be proficient enough with macros that they'll be worth the time...
        match self {
            SortState::NotSorted(col) => SortState::NotSorted(col.to_owned()),
            SortState::Ascending(col) => SortState::NotSorted(col.to_owned()),
            SortState::Descending(col) => SortState::NotSorted(col.to_owned())
        }
    }

    fn format(&self) -> String {
        match self {
            SortState::Descending(col) => format!("\u{23f7} {}", col),
            SortState::Ascending(col) => format!("\u{23f6} {}", col),
            SortState::NotSorted(col) => format!("\u{2195} {}", col),
        }.to_string()
    }
}

trait SelectionDepth<Icon> {
    fn inc(
        &self
    ) -> Self;

    fn reset(
        &self
    ) -> Self;

    fn format(
        &self
    ) -> Icon where Icon: Into<WidgetText>;
}

trait ExtraInteractions {
    // fn toggleable_value<Value: PartialEq>(
    //     &mut self,
    //     current_value: &mut Option<Value>,
    //     selected_value: Value,
    //     text: impl Into<WidgetText>,
    // ) -> Response;

    fn sort_button<Value: PartialEq + SelectionDepth<Icon>, Icon: Into<WidgetText>> (
        &mut self,
        current_value: &mut Option<Value>,
        selected_value: Value,
    ) -> Response;
}

impl ExtraInteractions for Ui {
    // fn toggleable_valume<Value: PartialEq>(
    //     &mut self,
    //     current_value: &mut Option<Value>,
    //     selected_value: Value,
    //     text: impl Into<WidgetText>,
    // ) -> Response {
    //     let selected = match current_value {
    //         Some(value) => *value == selected_value,
    //         None => false,
    //     };
    //     let mut response = self.selectable_label(selected, text);
    //     if response.clicked() {
    //         *current_value = if selected {None} else {Some(selected_value)};
    //         response.mark_changed();
    //     }
    //     response
    // }

    fn sort_button<Value: PartialEq + SelectionDepth<Icon>, Icon: Into<WidgetText>> (
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

async fn file_dialog() -> String {
    let file = AsyncFileDialog::new()
       .pick_file()
       .await;

    // FIXME: unsafe unwraps
    file.unwrap().inner().to_str().unwrap().to_string()
}

pub struct ParqBenchApp {
    pub table: Option<ParquetData>,

    runtime: tokio::runtime::Runtime,
    pipe: Option<tokio::sync::oneshot::Receiver<Result<ParquetData, String>>>,
}

impl Default for ParqBenchApp {
    fn default() -> Self {
        Self {
            // ui elements
            // table: None,

            table: None,
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
                    Ok(data) => match data {
                        Ok(data) => {
                            self.table = Some(data);
                            self.pipe = None;
                            false
                        },
                        _ => {
                            // FIXME
                            println!("data loading didn't work");
                            self.pipe = None;
                            false
                        }

                    },
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
    where F: Future<Output=Result<ParquetData, String>> + Send + 'static,
    {
        if self.data_pending() {
            // FIXME, use vec of tasks?
            panic!("Cannot schedule future when future already running");
        }
        let (tx, rx) = tokio::sync::oneshot::channel::<Result<ParquetData, String>>();
        self.pipe = Some(rx);

        async fn inner<F>(future: F, ctx: egui::Context, tx: tokio::sync::oneshot::Sender<F::Output>)
        where F: Future<Output=Result<ParquetData, String>> + Send
        {
            let data = future.await;
            let result = tx.send(data);
            println!("data loaded, requesting repaint");
            ctx.request_repaint();
        }

        self.runtime.spawn(inner::<F>( future, ctx.clone(), tx));
    }

    fn render_table(&mut self, ui: &mut Ui) -> Option<DataFilters> {
        fn is_sorted_column(sorted_col: &Option<SortState>, col: String) -> bool {
            match sorted_col {
                Some(sort) => {
                    match sort {
                        SortState::Ascending(sorted_col) => sorted_col.to_owned() == col,
                        SortState::Descending(sorted_col) => sorted_col.to_owned() == col,
                        _ => false
                    }
                },
                None => false
            }
        }

        let table = self.table.as_ref().unwrap();
        let mut filters: Option<DataFilters> = None;
        let mut sorted_column = table.filters.sort.clone();

        // TODO: manage sort_state through function?
        TableBuilder::new(ui)
            .striped(true)
            // TODO: set sizes in font units
            // TODO: set widths by type, max(type_width, total_space/columns)
            .columns(Size::initial(8.0* TEXT_HEIGHT).at_least(8.0 * TEXT_HEIGHT), table.data.num_columns())
            .resizable(true)
            .header(TEXT_HEIGHT * 1.5, |mut header| {
                for field in table.data.schema().fields() {
                    header.col(|ui| {
                        // {column: field.name().to_owned(), sort_state: table.filters.ascending, icon: Default::default()};
                        let column_label = if is_sorted_column(&sorted_column, field.name().to_string()) { sorted_column.clone().unwrap() } else { SortState::NotSorted(field.name().to_string()) };
                        let response = ui.sort_button( &mut sorted_column, column_label.clone());
                        if response.clicked() {
                            filters = Some(DataFilters { sort: sorted_column.clone(), ..table.filters.clone()});
                        };
                    });
                }
            })
            .body(|body| {
                body.rows(TEXT_HEIGHT, table.data.num_rows() as usize, |row_index, mut row| {
                    for data_col in table.data.columns() {
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
        filters
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
    // TODO: close open file
    // TODO: open with ... hooks (cmd line args)
    // TODO: confirm exit

    fn update(&mut self, ctx: &egui::Context, frame: &mut eframe::Frame) {
        //////////
        // Frame setup. Check if various interactions are in progress and resolve them
        //////////
        ctx.set_visuals(egui::style::Visuals::dark());

        if !ctx.input().raw.dropped_files.is_empty() {
            // FIXME: unsafe unwraps
            let file: egui::DroppedFile = ctx.input().raw.dropped_files.last().unwrap().clone();
            let filename = file.path.unwrap().to_str().unwrap().to_string();
            self.run_data_future(ParquetData::load(filename), ctx);
        }

        //////////
        // Main UI layout.
        //////////

        //////////
        //   Using static layout until I put together a TabTree that can make this dynamic
        //
        //   | menu_bar            |
        //   -----------------------
        //   |       |             |
        //   | query |     main    |
        //   | info  |     table   |
        //   |       |             |
        //   -----------------------
        //   | notification footer |
        //
        //////////

        egui::TopBottomPanel::top("top_panel").show(ctx, |ui| {
            egui::menu::bar(ui, |ui| {
                ui.menu_button("File", |ui| {
                    ui.menu_button("About", |ui| {
                        ui.label("Built with egui");
                    });

                    if ui.button("Open...").clicked() {
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


        // egui::SidePanel::left("side_panel").min_width(0f32).max_width(400f32).resizable(true).show(ctx, |ui| {
        //     // TODO: collapsing headers
        //     egui::ScrollArea::vertical().show(ui, |ui| {
        //         ui.label("Filter");
        //         ui.label("Not Yet Implemented");
        //         ui.set_enabled(false);
        //         ui.label("Table Name");
        //         // ui.text_edit_singleline(&mut self.filters.table_name);
        //         ui.label("SQL Query");
        //         // ui.text_edit_singleline(&mut self.filters.query);
        //         // TODO: input, update data, output for errors
        //         // button
        //         // self.data.query(self.filters)
        //     });
        // });


        egui::TopBottomPanel::bottom("bottom_panel").show(ctx, |ui| {
            ui.horizontal(|ui| {
                // match &self.table {
                //     Some(table) => { ui.label(&format!("{:#?}", table.data.filename)); },
                //     None => { ui.label("no file set"); },
                // }
                egui::warn_if_debug_build(ui);
            });
        });


        egui::CentralPanel::default().show(ctx, |ui| {
            // The central panel the region left after adding TopPanel's and SidePanel's
            // let TEXT_HEIGHT = egui::TextStyle::Body.resolve(ui.style()).size;

            if self.data_pending() {
                ui.spinner();
                ui.set_enabled(false);
            };

            egui::ScrollArea::horizontal().show(ui, |ui| {
                let filters = match &self.table {
                    Some(_) => self.render_table(ui),
                    None => {ui.label("Drag and drop file here."); None}
                };
                match filters {
                    Some(filters) => self.run_data_future(self.table.as_ref().unwrap().clone().sort(Some(filters)), ui.ctx()),
                    _ => {}
                }
            });
        });
    }
}
