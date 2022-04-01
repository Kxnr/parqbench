use eframe::{egui, epi};
use egui::{Response, WidgetText, Ui};
use native_dialog::FileDialog;
use std::path::PathBuf;
// use parquet::file::reader::SerializedFileReader;

#[derive(PartialEq)]
#[cfg_attr(feature = "persistence", derive(serde::Deserialize, serde::Serialize))]
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
#[cfg_attr(feature = "persistence", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "persistence", serde(default))] // if we add new fields, give them default values when deserializing old state
pub struct TemplateApp {
    // #[cfg_attr(feature = "persistence", serde(skip))]
    table: Option<PathBuf>,
    menu_panel: Option<MenuPanels>,
}

impl Default for TemplateApp {
    fn default() -> Self {
        Self {
            // Example stuff:
            table: None,
            menu_panel: None,
        }
    }
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
        #[cfg(feature = "persistence")]
        if let Some(storage) = _storage {
            *self = epi::get_value(storage, epi::APP_KEY).unwrap_or_default()
        }
    }

    #[cfg(feature = "persistence")]
    fn save(&mut self, storage: &mut dyn epi::Storage) {
        epi::set_value(storage, epi::APP_KEY, self);
    }

    fn update(&mut self, ctx: &egui::Context, frame: &epi::Frame) {
        let Self {table, menu_panel} = self;

        egui::TopBottomPanel::top("top_panel").show(ctx, |ui| {
            // The top panel is often a good place for a menu bar:
            egui::menu::bar(ui, |ui| {
                ui.menu_button("File", |ui| {
                    ui.menu_button("About", |ui| {
                        ui.label("Built with egui");
                    });

                    if ui.button("Open...").clicked(){
                        *table = FileDialog::new()
                            .set_location("~")
                            .show_open_single_file()
                            .unwrap();
                    }

                    if ui.button("Quit").clicked() {
                        frame.quit();
                    }
                });
            });
        });


        // show expand panel button
        // TODO: the collapse doesn't look like it'll work well. Instead, add a vertical set of
        // TODO: buttons in a narrow panel that will toggle other panels. Images will likely be
        // TODO: be best for this
        egui::SidePanel::left("side_panel").min_width(0f32).resizable(false).show(ctx, |ui| {
            ui.vertical(|ui| {
                // TODO: pop out panels with these buttons
                // tooltips
                let _ = ui.toggleable_value(menu_panel, MenuPanels::Schema, "\u{FF5B}");
                let _ = ui.toggleable_value(menu_panel, MenuPanels::Info, "\u{2139}");
                let _ = ui.toggleable_value(menu_panel, MenuPanels::Filter, "\u{1F50E}");
            });
        });

        match menu_panel {
            Some(panel) => {
                match panel {
                    MenuPanels::Schema => {
                        egui::SidePanel::left("side_panel").show(ctx, |ui| {
                            ui.label("schema menu");
                        });
                    },
                    MenuPanels::Info => {
                        egui::SidePanel::left("side_panel").show(ctx, |ui| {
                            ui.label("info menu");
                        });
                    },
                    MenuPanels::Filter => {
                        egui::SidePanel::left("side_panel").show(ctx, |ui| {
                            ui.label("filter menu");
                        });
                    }
                }
            },
            _ => {},
        }


        // TODO: table
        egui::CentralPanel::default().show(ctx, |ui| {
            // The central panel the region left after adding TopPanel's and SidePanel's
        });

        egui::TopBottomPanel::bottom("bottom_panel").show(ctx, |ui| {
            match table {
                Some(table) => {ui.label(&format!("{:#?}", table));},
                None => {ui.label("no file set");},
            }
            egui::warn_if_debug_build(ui);
        });
    }
}
