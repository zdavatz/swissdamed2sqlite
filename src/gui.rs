//! Cross-platform GUI (Windows, macOS, Linux) using egui/eframe.
//! Provides one-click buttons for downloading Swissdamed data and CHRN lookup.

use std::io::Write;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;

use eframe::egui;

fn settings_path() -> PathBuf {
    crate::app_data_dir().join("settings.json")
}
fn logs_dir() -> PathBuf {
    crate::app_data_dir().join("logs")
}

/// Messages from the worker thread to the GUI.
enum WorkerMsg {
    Log(String),
    Done { ok: bool, summary: String },
}

/// Persistent state saved between sessions.
#[derive(Default, Clone, serde::Serialize, serde::Deserialize)]
struct Settings {
    chrn: String,
}

impl Settings {
    fn load() -> Self {
        std::fs::read_to_string(settings_path())
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default()
    }

    fn save(&self) {
        if let Ok(json) = serde_json::to_string_pretty(self) {
            let _ = std::fs::write(settings_path(), json);
        }
    }
}

pub struct App {
    settings: Settings,
    prev_settings: String,
    log_lines: Vec<String>,
    running: bool,
    rx: Option<mpsc::Receiver<WorkerMsg>>,
    icon_texture: Option<egui::TextureHandle>,
}

impl App {
    pub fn new(cc: &eframe::CreationContext<'_>) -> Self {
        cc.egui_ctx.set_visuals(egui::Visuals::light());
        let settings = Settings::load();
        let prev_settings = serde_json::to_string(&settings).unwrap_or_default();
        App {
            settings,
            prev_settings,
            log_lines: Vec::new(),
            running: false,
            rx: None,
            icon_texture: None,
        }
    }

    fn save_log(&self) {
        if self.log_lines.is_empty() {
            return;
        }
        let log_dir = logs_dir();
        let _ = std::fs::create_dir_all(&log_dir);
        let timestamp = chrono::Local::now().format("%Y-%m-%d_%H%M%S");
        let path = log_dir.join(format!("{}.log", timestamp));
        if let Ok(mut f) = std::fs::File::create(&path) {
            for line in &self.log_lines {
                let _ = writeln!(f, "{}", line);
            }
        }
    }

    fn start_products_download(&mut self, ctx: egui::Context) {
        let (tx, rx) = mpsc::channel();
        self.rx = Some(rx);
        self.running = true;
        self.log_lines.clear();

        thread::spawn(move || {
            run_products_pipeline(tx, ctx);
        });
    }

    fn start_chrn_lookup(&mut self, ctx: egui::Context) {
        let (tx, rx) = mpsc::channel();
        self.rx = Some(rx);
        self.running = true;
        self.log_lines.clear();

        let chrn = self.settings.chrn.trim().to_string();
        thread::spawn(move || {
            run_chrn_lookup(chrn, tx, ctx);
        });
    }

    fn start_migel_matching(&mut self, ctx: egui::Context) {
        let (tx, rx) = mpsc::channel();
        self.rx = Some(rx);
        self.running = true;
        self.log_lines.clear();

        thread::spawn(move || {
            run_migel_pipeline(tx, ctx);
        });
    }
}

impl eframe::App for App {
    fn on_exit(&mut self, _gl: Option<&eframe::glow::Context>) {
        self.settings.save();
        self.save_log();
    }

    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // Drain messages from worker thread
        if let Some(ref rx) = self.rx {
            while let Ok(msg) = rx.try_recv() {
                match msg {
                    WorkerMsg::Log(line) => self.log_lines.push(line),
                    WorkerMsg::Done { ok, summary } => {
                        self.log_lines.push(String::new());
                        if ok {
                            self.log_lines.push(format!("=== DONE === {}", summary));
                        } else {
                            self.log_lines.push(format!("=== FAILED === {}", summary));
                        }
                        self.running = false;
                        self.save_log();
                    }
                }
            }
            if self.running {
                ctx.request_repaint();
            }
        }

        egui::CentralPanel::default().show(ctx, |ui| {
            // Load icon texture once
            let icon_texture = self.icon_texture.get_or_insert_with(|| {
                let png_bytes = include_bytes!("../assets/icon_256x256.png");
                let img = image::load_from_memory(png_bytes).unwrap().into_rgba8();
                let size = [img.width() as usize, img.height() as usize];
                let pixels = img.into_raw();
                let color_image = egui::ColorImage::from_rgba_unmultiplied(size, &pixels);
                ctx.load_texture("app-icon", color_image, egui::TextureOptions::LINEAR)
            });

            // Header with icon
            ui.horizontal(|ui| {
                ui.heading(format!("swissdamed2sqlite v{}", env!("CARGO_PKG_VERSION")));
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    let icon_button = ui.add(
                        egui::ImageButton::new(egui::load::SizedTexture::new(
                            icon_texture.id(),
                            egui::vec2(48.0, 48.0),
                        ))
                        .frame(false),
                    ).on_hover_text("zdavatz@ywesee.com");
                    if icon_button.clicked() {
                        let _ = open::that("mailto:zdavatz@ywesee.com");
                    }
                });
            });
            ui.add_space(8.0);

            // --- Products DB section ---
            ui.separator();
            ui.add_space(4.0);
            ui.label("Download all Swissdamed UDI products and save as CSV + SQLite:");
            ui.add_space(4.0);

            ui.horizontal(|ui| {
                let btn_text = if self.running {
                    "Running..."
                } else {
                    "Download Products (CSV + SQLite)"
                };
                if ui
                    .add_enabled(
                        !self.running,
                        egui::Button::new(btn_text).min_size(egui::vec2(250.0, 32.0)),
                    )
                    .clicked()
                {
                    self.start_products_download(ctx.clone());
                }
            });

            ui.add_space(12.0);

            // --- CHRN Lookup section ---
            ui.separator();
            ui.add_space(4.0);
            ui.label("Look up all SRNs for a CHRN:");
            ui.add_space(4.0);

            ui.horizontal(|ui| {
                ui.add(
                    egui::TextEdit::singleline(&mut self.settings.chrn)
                        .desired_width(250.0)
                        .hint_text("CHRN-AR-20000807"),
                );

                let can_lookup = !self.running && !self.settings.chrn.trim().is_empty();
                if ui
                    .add_enabled(
                        can_lookup,
                        egui::Button::new("Lookup SRNs").min_size(egui::vec2(120.0, 32.0)),
                    )
                    .clicked()
                {
                    self.start_chrn_lookup(ctx.clone());
                }
            });

            ui.add_space(12.0);

            // --- MiGeL Matching section ---
            ui.separator();
            ui.add_space(4.0);
            ui.label("Match UDI devices against MiGeL codes:");
            ui.add_space(4.0);

            ui.horizontal(|ui| {
                let btn_text = if self.running {
                    "Running..."
                } else {
                    "MiGeL Matching (SQLite)"
                };
                if ui
                    .add_enabled(
                        !self.running,
                        egui::Button::new(btn_text).min_size(egui::vec2(200.0, 32.0)),
                    )
                    .clicked()
                {
                    self.start_migel_matching(ctx.clone());
                }
            });

            ui.add_space(12.0);

            // --- Open Output Folder ---
            ui.separator();
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                if ui
                    .add_enabled(
                        !self.running,
                        egui::Button::new("Open Output Folder").min_size(egui::vec2(160.0, 32.0)),
                    )
                    .on_hover_text(format!("{}", crate::app_data_dir().display()))
                    .clicked()
                {
                    let dir = crate::app_data_dir();
                    let _ = std::fs::create_dir_all(&dir);
                    let _ = open::that(&dir);
                }

                if ui
                    .add_enabled(
                        !self.running,
                        egui::Button::new("Open CSV Folder").min_size(egui::vec2(140.0, 32.0)),
                    )
                    .clicked()
                {
                    let dir = crate::app_data_dir().join("csv");
                    let _ = std::fs::create_dir_all(&dir);
                    let _ = open::that(&dir);
                }

                if ui
                    .add_enabled(
                        !self.running,
                        egui::Button::new("Open DB Folder").min_size(egui::vec2(130.0, 32.0)),
                    )
                    .clicked()
                {
                    let dir = crate::app_data_dir().join("db");
                    let _ = std::fs::create_dir_all(&dir);
                    let _ = open::that(&dir);
                }
            });

            ui.add_space(8.0);
            ui.separator();

            // --- Log output ---
            ui.label("Log:");
            let text_height = ui.available_height();
            egui::ScrollArea::vertical()
                .max_height(text_height)
                .stick_to_bottom(true)
                .show(ui, |ui| {
                    for line in &self.log_lines {
                        ui.label(egui::RichText::new(line).monospace().size(12.0));
                    }
                });
        });

        // Auto-save settings
        let current = serde_json::to_string(&self.settings).unwrap_or_default();
        if current != self.prev_settings {
            self.settings.save();
            self.prev_settings = current;
        }
    }
}

/// Download all UDI products and save as CSV + SQLite.
fn run_products_pipeline(tx: mpsc::Sender<WorkerMsg>, ctx: egui::Context) {
    let log = |msg: &str| {
        let _ = tx.send(WorkerMsg::Log(msg.to_string()));
        ctx.request_repaint();
    };
    let done = |ok: bool, summary: &str| {
        let _ = tx.send(WorkerMsg::Done {
            ok,
            summary: summary.to_string(),
        });
        ctx.request_repaint();
    };

    log("Downloading UDI products from swissdamed.ch ...");

    let values = match crate::download_all_pages(50) {
        Ok(v) => v,
        Err(e) => {
            done(false, &format!("Download failed: {}", e));
            return;
        }
    };

    if values.is_empty() {
        done(false, "No data found.");
        return;
    }

    log(&format!("Downloaded {} items.", values.len()));

    let (headers, trade_name_langs) = crate::collect_headers(&values);
    let rows = crate::build_rows(&values, &headers, &trade_name_langs);

    log(&format!(
        "Processed {} items → {} rows, {} columns.",
        values.len(),
        rows.len(),
        headers.len()
    ));

    // Write CSV
    let csv_path = crate::output_csv("swissdamed");
    match crate::write_csv(&headers, &rows, &csv_path) {
        Ok(()) => log(&format!("CSV written: {}", csv_path)),
        Err(e) => {
            done(false, &format!("CSV write failed: {}", e));
            return;
        }
    }

    // Write SQLite
    let db_path = crate::output_db("swissdamed");
    match crate::write_sqlite(&headers, &rows, &db_path) {
        Ok(()) => log(&format!("SQLite written: {}", db_path)),
        Err(e) => {
            done(false, &format!("SQLite write failed: {}", e));
            return;
        }
    }

    done(
        true,
        &format!(
            "{} rows saved to CSV + SQLite",
            rows.len()
        ),
    );
}

/// Look up all SRNs for a given CHRN.
fn run_chrn_lookup(chrn: String, tx: mpsc::Sender<WorkerMsg>, ctx: egui::Context) {
    let log = |msg: &str| {
        let _ = tx.send(WorkerMsg::Log(msg.to_string()));
        ctx.request_repaint();
    };
    let done = |ok: bool, summary: &str| {
        let _ = tx.send(WorkerMsg::Done {
            ok,
            summary: summary.to_string(),
        });
        ctx.request_repaint();
    };

    log(&format!("Looking up SRNs for {} ...", chrn));

    // Download actors
    log("Downloading actors...");
    let actors = match crate::download_all_pages_from(
        "https://swissdamed.ch/public/act/actors",
        "actors",
        50,
    ) {
        Ok(v) => v,
        Err(e) => {
            done(false, &format!("Actors download failed: {}", e));
            return;
        }
    };
    log(&format!("Downloaded {} actors.", actors.len()));

    // Find matching actors by CHRN
    let matching: Vec<&serde_json::Value> = actors
        .iter()
        .filter(|a| {
            a.get("chrn")
                .and_then(|v| v.as_str())
                .map(|s| s.eq_ignore_ascii_case(&chrn))
                .unwrap_or(false)
        })
        .collect();

    if matching.is_empty() {
        done(false, &format!("No actor found for {}", chrn));
        return;
    }
    log(&format!(
        "Found {} actor record(s) for {}.",
        matching.len(),
        chrn
    ));

    // Get actor IDs (as strings, matching CLI logic)
    let actor_ids: std::collections::HashSet<String> = matching
        .iter()
        .filter_map(|a| a.get("id").and_then(|v| v.as_str()).map(|s| s.to_string()))
        .collect();

    // Download mandates
    log("Downloading mandates...");
    let mandates = match crate::download_all_pages_from(
        "https://swissdamed.ch/public/act/mandates",
        "mandates",
        50,
    ) {
        Ok(v) => v,
        Err(e) => {
            done(false, &format!("Mandates download failed: {}", e));
            return;
        }
    };
    log(&format!("Downloaded {} mandates.", mandates.len()));

    // Find mandates for matching actors (using actorId string field, like CLI)
    let matching_mandate_ids: Vec<(String, String)> = mandates
        .iter()
        .filter_map(|m| {
            let actor_id = m.get("actorId").and_then(|v| v.as_str())?;
            if !actor_ids.contains(actor_id) {
                return None;
            }
            let mandate_id = m.get("id").and_then(|v| v.as_str())?;
            Some((actor_id.to_string(), mandate_id.to_string()))
        })
        .collect();

    if matching_mandate_ids.is_empty() {
        done(false, &format!("No mandates found for {}", chrn));
        return;
    }
    log(&format!(
        "Fetching details for {} mandates...",
        matching_mandate_ids.len()
    ));

    // Create HTTP client for mandate detail fetches
    let client = reqwest::blocking::Client::builder()
        .cookie_store(true)
        .redirect(reqwest::redirect::Policy::limited(10))
        .user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36")
        .build()
        .unwrap();

    let ids_only: Vec<String> = matching_mandate_ids.iter().map(|(_, mid)| mid.clone()).collect();
    let details = match crate::fetch_mandate_details(&client, &ids_only) {
        Ok(d) => d,
        Err(e) => {
            done(false, &format!("Mandate details fetch failed: {}", e));
            return;
        }
    };
    log(&format!("Fetched {} mandate details.", details.len()));

    // Build output rows (mirroring CLI run_lookup_chrn logic)
    let actor_headers = crate::collect_flat_headers(&actors);

    let mut detail_key_order: Vec<String> = Vec::new();
    let mut detail_key_set = std::collections::BTreeSet::new();
    for detail in &details {
        for (k, _) in crate::flatten_mandate_detail(detail) {
            if detail_key_set.insert(k.clone()) {
                detail_key_order.push(k);
            }
        }
    }

    let mut joined_headers: Vec<String> = actor_headers.iter().map(|h| format!("actor_{}", h)).collect();
    joined_headers.extend(detail_key_order.iter().map(|h| format!("mandate_{}", h)));

    let mut rows: Vec<Vec<String>> = Vec::new();
    for (i, (_actor_id_str, _mandate_id_str)) in matching_mandate_ids.iter().enumerate() {
        // Find the actor for this mandate
        let actor = matching.iter().find(|a| {
            a.get("id").and_then(|v| v.as_str()) == Some(_actor_id_str.as_str())
        });
        let actor_vals: Vec<String> = if let Some(actor) = actor {
            actor_headers.iter().map(|h| crate::get_field(actor, h)).collect()
        } else {
            actor_headers.iter().map(|_| String::new()).collect()
        };

        let detail_vals: Vec<String> = if let Some(detail) = details.get(i) {
            let flat = crate::flatten_mandate_detail(detail);
            detail_key_order.iter().map(|k| {
                flat.iter().find(|(fk, _)| fk == k).map(|(_, v)| v.clone()).unwrap_or_default()
            }).collect()
        } else {
            detail_key_order.iter().map(|_| String::new()).collect()
        };

        let mut row = actor_vals;
        row.extend(detail_vals);
        rows.push(row);
    }

    log(&format!(
        "Found {} SRN rows ({} columns).",
        rows.len(),
        joined_headers.len()
    ));

    // Write CSV
    let timestamp = chrono::Local::now().format("%Hh%M.%d.%m.%Y").to_string();
    let csv_dir = crate::app_data_dir().join("csv");
    let _ = std::fs::create_dir_all(&csv_dir);
    let csv_path = csv_dir
        .join(format!("{}_{}.csv", chrn, timestamp))
        .to_string_lossy()
        .to_string();

    match crate::write_csv(&joined_headers, &rows, &csv_path) {
        Ok(()) => log(&format!("CSV written: {}", csv_path)),
        Err(e) => {
            done(false, &format!("CSV write failed: {}", e));
            return;
        }
    }

    done(
        true,
        &format!("{} SRNs for {} saved to CSV", rows.len(), chrn),
    );
}

/// Download UDI data, match against MiGeL, save as SQLite.
fn run_migel_pipeline(tx: mpsc::Sender<WorkerMsg>, ctx: egui::Context) {
    let log = |msg: &str| {
        let _ = tx.send(WorkerMsg::Log(msg.to_string()));
        ctx.request_repaint();
    };
    let done = |ok: bool, summary: &str| {
        let _ = tx.send(WorkerMsg::Done {
            ok,
            summary: summary.to_string(),
        });
        ctx.request_repaint();
    };

    // 1. Download UDI data
    log("Downloading UDI products from swissdamed.ch ...");
    let values = match crate::download_all_pages(50) {
        Ok(v) => v,
        Err(e) => {
            done(false, &format!("Download failed: {}", e));
            return;
        }
    };

    if values.is_empty() {
        done(false, "No data found.");
        return;
    }

    let (headers, trade_name_langs) = crate::collect_headers(&values);
    let rows = crate::build_rows(&values, &headers, &trade_name_langs);
    log(&format!(
        "Downloaded {} items → {} rows.",
        values.len(),
        rows.len()
    ));

    // 2. Download MiGeL XLSX
    log("Downloading MiGeL XLSX...");
    let migel_url = "https://www.bag.admin.ch/dam/de/sd-web/77j5rwUTzbkq/Mittel-%20und%20Gegenst%C3%A4ndeliste%20per%2001.01.2026%20in%20Excel-Format.xlsx";
    let migel_file = crate::app_data_dir().join("migel.xlsx");
    let client = reqwest::blocking::Client::builder()
        .user_agent("swissdamed2sqlite/0.1")
        .build()
        .unwrap();
    let response = match client.get(migel_url).send() {
        Ok(r) => r,
        Err(e) => {
            done(false, &format!("MiGeL download failed: {}", e));
            return;
        }
    };
    if !response.status().is_success() {
        done(false, &format!("MiGeL download HTTP {}", response.status()));
        return;
    }
    let bytes = response.bytes().unwrap();
    let _ = std::fs::write(&migel_file, &bytes);
    log(&format!("MiGeL XLSX saved ({} bytes)", bytes.len()));

    // 3. Parse MiGeL items
    log("Parsing MiGeL items...");
    let migel_items = match crate::migel::parse_migel_items(migel_file.to_str().unwrap()) {
        Ok(items) => items,
        Err(e) => {
            done(false, &format!("MiGeL parse failed: {}", e));
            return;
        }
    };
    log(&format!("Found {} MiGeL items", migel_items.len()));

    let search_index = crate::migel::build_search_index(&migel_items);
    log("Built Aho-Corasick search index");

    // 4. Match rows
    log("Matching UDI rows against MiGeL...");
    let trade_name_indices: Vec<(String, usize)> = headers
        .iter()
        .enumerate()
        .filter(|(_, h)| h.starts_with("tradeName_"))
        .map(|(i, h)| (h.clone(), i))
        .collect();
    let idx_brand = headers.iter().position(|h| h == "companyName");
    let idx_device = headers.iter().position(|h| h == "deviceName");
    let idx_model = headers.iter().position(|h| h == "modelName");
    let idx_company = headers.iter().position(|h| h == "companyName");

    let excluded_companies: &[&str] = &[
        "Varian Medical Systems Inc",
        "Varian Medical Systems Inc.",
        "Sunstar Europe SA",
    ];

    use rayon::prelude::*;
    let matched_rows: Vec<Vec<String>> = rows
        .par_iter()
        .filter_map(|row| {
            if let Some(ci) = idx_company {
                if let Some(company) = row.get(ci) {
                    if excluded_companies.contains(&company.as_str()) {
                        return None;
                    }
                }
            }

            let mut desc_de = String::new();
            let mut desc_fr = String::new();
            let mut desc_it = String::new();

            for (col_name, idx) in &trade_name_indices {
                let val = row.get(*idx).cloned().unwrap_or_default();
                if val.is_empty() {
                    continue;
                }
                match col_name.as_str() {
                    "tradeName_DE" => desc_de = format!("{} {}", desc_de, val),
                    "tradeName_FR" => desc_fr = format!("{} {}", desc_fr, val),
                    "tradeName_IT" => desc_it = format!("{} {}", desc_it, val),
                    _ => {
                        desc_de = format!("{} {}", desc_de, val);
                        desc_fr = format!("{} {}", desc_fr, val);
                        desc_it = format!("{} {}", desc_it, val);
                    }
                }
            }

            let device = idx_device.and_then(|i| row.get(i)).cloned().unwrap_or_default();
            let model = idx_model.and_then(|i| row.get(i)).cloned().unwrap_or_default();
            if !device.is_empty() {
                desc_de = format!("{} {}", desc_de, device);
                desc_fr = format!("{} {}", desc_fr, device);
                desc_it = format!("{} {}", desc_it, device);
            }
            if !model.is_empty() {
                desc_de = format!("{} {}", desc_de, model);
                desc_fr = format!("{} {}", desc_fr, model);
                desc_it = format!("{} {}", desc_it, model);
            }

            let brand = idx_brand.and_then(|i| row.get(i)).cloned().unwrap_or_default();

            crate::migel::find_best_migel_match(
                &desc_de, &desc_fr, &desc_it, &brand, &migel_items, &search_index,
            )
            .map(|migel| {
                let mut matched_row = row.clone();
                matched_row.push(migel.position_nr.clone());
                matched_row.push(migel.bezeichnung.clone());
                matched_row.push(migel.limitation.clone());
                matched_row
            })
        })
        .collect();

    log(&format!(
        "MiGeL matches: {} out of {} rows",
        matched_rows.len(),
        rows.len()
    ));

    if matched_rows.is_empty() {
        done(false, "No MiGeL matches found.");
        return;
    }

    // 5. Write to SQLite
    let mut migel_headers = headers.clone();
    migel_headers.push("migel_code".to_string());
    migel_headers.push("migel_bezeichnung".to_string());
    migel_headers.push("migel_limitation".to_string());

    let db_path = crate::output_db("swissdamed_migel");
    match crate::write_sqlite(&migel_headers, &matched_rows, &db_path) {
        Ok(()) => log(&format!("SQLite written: {}", db_path)),
        Err(e) => {
            done(false, &format!("SQLite write failed: {}", e));
            return;
        }
    }

    done(
        true,
        &format!(
            "{} MiGeL matches out of {} rows",
            matched_rows.len(),
            rows.len()
        ),
    );
}

fn load_icon() -> Option<egui::IconData> {
    let png_bytes = include_bytes!("../assets/icon_256x256.png");
    let img = image::load_from_memory(png_bytes).ok()?.into_rgba8();
    let (w, h) = img.dimensions();
    Some(egui::IconData {
        rgba: img.into_raw(),
        width: w,
        height: h,
    })
}

/// Launch the GUI application.
pub fn run_gui() -> eframe::Result {
    let mut viewport = egui::ViewportBuilder::default()
        .with_title(format!(
            "swissdamed2sqlite v{}",
            env!("CARGO_PKG_VERSION")
        ))
        .with_inner_size([750.0, 600.0])
        .with_min_inner_size([500.0, 400.0]);

    if let Some(icon) = load_icon() {
        viewport = viewport.with_icon(std::sync::Arc::new(icon));
    }

    let options = eframe::NativeOptions {
        viewport,
        ..Default::default()
    };

    eframe::run_native(
        "swissdamed2sqlite",
        options,
        Box::new(|cc| Ok(Box::new(App::new(cc)))),
    )
}
