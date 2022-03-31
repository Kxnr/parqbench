use eframe::{egui, epi};
use native_dialog::{FileDialog, MessageDialog, MessageType};
use std::path::PathBuf;
use parquet::file::reader::SerializedFileReader;

/// We derive Deserialize/Serialize so we can persist app state on shutdown.
#[cfg_attr(feature = "persistence", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "persistence", serde(default))] // if we add new fields, give them default values when deserializing old state
pub struct TemplateApp {
    // #[cfg_attr(feature = "persistence", serde(skip))]
    table: Option<PathBuf>,
}

impl Default for TemplateApp {
    fn default() -> Self {
        Self {
            // Example stuff:
            table: None,
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
        let Self {table} = self;

        // Examples of how to create different panels and windows.
        // Pick whichever suits you.
        // Tip: a good default choice is to just keep the `CentralPanel`.
        // For inspiration and more examples, go to https://emilk.github.io/egui

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
        egui::SidePanel::left("side_panel").show(ctx, |ui| {
            let response = egui::CollapsingHeader::new("").default_open(true).show(ui, |ui| {});
            // TODO: filter
            // TODO: schema
            ui.vertical_centered(|ui| {
                ui.label("over");
                ui.label("under");
            });
            // match response.body_response {
            //     Some(body_respones) => {ui.label("open");},
            //     None => {},
            // }
        });


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
