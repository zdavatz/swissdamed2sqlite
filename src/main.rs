#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

pub mod data;
pub mod diff;
pub mod download;
mod error_report;
pub mod export;
pub mod gdrive;
mod gui;
pub mod migel;
mod migel_stats;
pub mod reports;

use clap::Parser;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

const APP_DIR_NAME: &str = "swissdamed2sqlite";

/// Return the application data directory (`~/swissdamed2sqlite/`).
/// Under macOS App Sandbox, uses the container directory.
/// On Windows uses `%USERPROFILE%\swissdamed2sqlite\`.
/// Falls back to the current working directory.
pub fn app_data_dir() -> PathBuf {
    // macOS sandbox
    if let Ok(container) = std::env::var("APP_SANDBOX_CONTAINER_ID") {
        if !container.is_empty() {
            if let Some(home) = std::env::var_os("HOME") {
                let dir = PathBuf::from(home).join(APP_DIR_NAME);
                let _ = fs::create_dir_all(&dir);
                return dir;
            }
        }
    }

    #[cfg(target_os = "windows")]
    let home = std::env::var_os("USERPROFILE");
    #[cfg(not(target_os = "windows"))]
    let home = std::env::var_os("HOME");

    if let Some(home) = home {
        let dir = PathBuf::from(home).join(APP_DIR_NAME);
        let _ = fs::create_dir_all(&dir);
        return dir;
    }

    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

/// Configuration loaded from config.toml in the app data directory.
/// CLI arguments take precedence; config file provides fallback defaults.
#[derive(serde::Deserialize, Default)]
pub struct Config {
    pub scp: Option<String>,
    pub gdrive_folder: Option<String>,
    pub gdrive_key: Option<String>,
    pub gdrive_email: Option<String>,
}

impl Config {
    /// Load config from `<app_data_dir>/config.toml`, returning default if file doesn't exist.
    pub fn load() -> Config {
        let path = app_data_dir().join("config.toml");
        match fs::read_to_string(&path) {
            Ok(contents) => toml::from_str(&contents).unwrap_or_else(|e| {
                eprintln!("Warning: failed to parse {}: {}", path.display(), e);
                Config::default()
            }),
            Err(_) => Config::default(),
        }
    }
}

/// Resolve a setting: CLI arg takes precedence, then config file.
/// Returns an error message if neither provides a value.
pub fn resolve_setting(
    cli: &Option<String>,
    config: &Option<String>,
    name: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    cli.clone()
        .filter(|s| !s.is_empty())
        .or_else(|| config.clone().filter(|s| !s.is_empty()))
        .ok_or_else(|| {
            format!(
                "Missing required setting: --{name}. \
                 Provide it via CLI argument or set it in {}.",
                app_data_dir().join("config.toml").display()
            )
            .into()
        })
}

/// Download Swiss DAMED UDI data and convert to CSV or SQLite
#[derive(Parser, Debug)]
#[command(name = "swissdamed2sqlite", version, about)]
pub struct Args {
    /// Output as CSV file
    #[arg(long)]
    pub csv: bool,

    /// Output as SQLite database
    #[arg(long)]
    pub sqlite: bool,

    /// Use an existing JSON file instead of downloading
    #[arg(long, short = 'f')]
    pub file: Option<PathBuf>,

    /// Page size for API requests (default: 50)
    #[arg(long, default_value_t = 50)]
    pub page_size: u32,

    /// Deploy SQLite DB to remote server via scp
    #[arg(long)]
    pub deploy: bool,

    /// Remote scp target
    #[arg(long)]
    pub scp: Option<String>,

    /// Diff two CSV files and output changes to diff/ folder
    #[arg(long, num_args = 2, value_names = ["OLD_CSV", "NEW_CSV"])]
    pub diff: Option<Vec<PathBuf>>,

    /// Match UDI entries against MiGel codes and output matched results
    #[arg(long)]
    pub migel: bool,

    /// Render the MiGeL stats PNG from the latest existing migel SQLite DB
    #[arg(long)]
    pub migel_stats: bool,

    /// Download actors data
    #[arg(long)]
    pub actors: bool,

    /// Download mandates data
    #[arg(long)]
    pub mandates: bool,

    /// Show all mandates for actors of type AR (joined output)
    #[arg(long)]
    pub ar_mandates: bool,

    /// CH-REP only: companies with only AR/IM roles (no MF or PR under same UID)
    #[arg(long)]
    pub ch_rep: bool,

    /// CH-REP companies ranked by number of mandates
    #[arg(long)]
    pub ch_rep_mandates: bool,

    /// Restrict --ch-rep-mandates to AR role only (true CH-REPs)
    #[arg(long)]
    pub ar_only: bool,

    /// Look up all SRNs for a given CHRN (e.g. CHRN-AR-20000807)
    #[arg(long)]
    pub lookup_chrn: Option<String>,

    /// Upload CSV files to Google Drive
    #[arg(long)]
    pub gdrive: bool,

    /// Google Drive folder ID
    #[arg(long)]
    pub gdrive_folder: Option<String>,

    /// Path to .p12 service account key
    #[arg(long)]
    pub gdrive_key: Option<String>,

    /// Service account email
    #[arg(long)]
    pub gdrive_email: Option<String>,

    /// Google Workspace user to impersonate for Drive upload (required with --gdrive)
    #[arg(long)]
    pub gdrive_sub: Option<String>,

    /// Send CSV as email attachment to this address (comma-separated for multiple)
    #[arg(long)]
    pub mailto: Option<String>,

    /// Custom email subject line (default: "swissdamed2sqlite: <filename>")
    #[arg(long)]
    pub mail_subject: Option<String>,

    /// Rank companies by number of UDI products (descending)
    #[arg(long)]
    pub company_ranking: bool,

    /// Export unique SRNs with manufacturer and mandate holder info
    #[arg(long)]
    pub unique_srns: bool,
}

// --- Main ---

/// Show an error dialog using a minimal eframe window (GUI mode)
/// or just print to stderr (CLI mode).
fn show_error_dialog(message: &str, is_gui_mode: bool) {
    eprintln!("Error: {}", message);
    if is_gui_mode {
        gui::run_error_dialog(message);
    }
}

fn main() {
    let is_gui_mode = std::env::args().skip(1).next().is_none();
    if let Err(e) = run() {
        show_error_dialog(&e.to_string(), is_gui_mode);
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    // No arguments → launch GUI
    let cli_args: Vec<String> = std::env::args().skip(1).collect();
    if cli_args.is_empty() {
        gui::run_gui().map_err(|e| format!("GUI error: {}", e))?;
        return Ok(());
    }

    // Re-attach console for CLI mode (windows_subsystem = "windows" hides it)
    #[cfg(windows)]
    {
        extern "system" {
            fn AttachConsole(dwProcessId: u32) -> i32;
        }
        const ATTACH_PARENT_PROCESS: u32 = 0xFFFFFFFF;
        unsafe { AttachConsole(ATTACH_PARENT_PROCESS); }
    }

    let args = Args::parse();

    // Handle --diff mode
    if let Some(ref diff_files) = args.diff {
        return diff::diff_csv_files(&diff_files[0], &diff_files[1]);
    }

    // Handle --migel mode
    if args.migel {
        return reports::run_migel(&args);
    }

    // Handle --migel-stats mode (render PNG from existing DBs, no download)
    if args.migel_stats {
        let db_dir = app_data_dir().join("db");
        let (migel_db, full_db) = migel_stats::find_latest_dbs(&db_dir);
        let migel_db = migel_db.ok_or_else(|| {
            format!(
                "No swissdamed_migel_*.db found in {}",
                db_dir.display()
            )
        })?;
        eprintln!("Reading from {}", migel_db.display());
        if let Some(ref p) = full_db {
            eprintln!("Total products from {}", p.display());
        }
        migel_stats::generate(&migel_db, full_db.as_deref())?;
        return Ok(());
    }

    // Handle --company-ranking mode
    if args.company_ranking {
        return reports::run_company_ranking(&args);
    }

    // Handle --unique-srns mode
    if args.unique_srns {
        return reports::run_unique_srns(&args);
    }

    // Handle --ch-rep mode
    if args.ch_rep {
        return reports::run_ch_rep(&args);
    }

    // Handle --lookup-chrn mode
    if let Some(ref chrn) = args.lookup_chrn {
        return reports::run_lookup_chrn(chrn, &args);
    }

    // Handle --ch-rep-mandates mode
    if args.ch_rep_mandates {
        return reports::run_ch_rep_mandates(&args);
    }

    // Handle --ar-mandates mode
    if args.ar_mandates {
        return reports::run_ar_mandates(&args);
    }

    // Handle --actors and --mandates
    if args.actors || args.mandates {
        let (do_csv, do_sqlite) = if !args.csv && !args.sqlite {
            (true, true)
        } else {
            (args.csv, args.sqlite)
        };

        if args.actors {
            reports::download_and_export(
                "https://swissdamed.ch/public/act/actors",
                "actors",
                50,
                do_csv,
                do_sqlite,
                &args,
            )?;
        }

        if args.mandates {
            reports::download_and_export(
                "https://swissdamed.ch/public/act/mandates",
                "mandates",
                50,
                do_csv,
                do_sqlite,
                &args,
            )?;
        }

        return Ok(());
    }

    // Default: download UDI products
    let (do_csv, do_sqlite) = if !args.csv && !args.sqlite {
        (true, true)
    } else if args.deploy && !args.sqlite {
        (args.csv, true)
    } else {
        (args.csv, args.sqlite)
    };

    let values = if let Some(ref path) = args.file {
        eprintln!("Loading from file: {}", path.display());
        download::load_json_file(path)?
    } else {
        download::download_all_pages(args.page_size)?
    };

    if values.is_empty() {
        eprintln!("No data found.");
        return Ok(());
    }

    let (headers, trade_name_langs) = data::collect_headers(&values);
    let rows = data::build_rows(&values, &headers, &trade_name_langs);

    eprintln!(
        "Processed {} items, generated {} rows with {} columns.",
        values.len(),
        rows.len(),
        headers.len()
    );

    if do_csv {
        let filename = export::output_csv("swissdamed")?;
        export::write_csv(&headers, &rows, &filename)?;
        eprintln!("CSV written: {}", filename);
        if args.gdrive {
            gdrive::gdrive_upload_csv(&args, &filename)?;
        }
        if let Some(ref to) = args.mailto {
            gdrive::send_email_with_attachment(&args, &filename, to)?;
        }
    }

    if do_sqlite {
        let filename = export::output_db("swissdamed")?;
        export::write_sqlite(&headers, &rows, &filename)?;
        eprintln!("SQLite written: {}", filename);

        if args.deploy {
            let config = Config::load();
            let scp_target = resolve_setting(&args.scp, &config.scp, "scp")?;
            eprintln!("Deploying {} to {} ...", filename, scp_target);
            let status = Command::new("scp")
                .arg(&filename)
                .arg(&scp_target)
                .status()?;

            if status.success() {
                eprintln!("Deploy successful.");
            } else {
                eprintln!("Deploy failed with exit code: {}", status);
                return Err("scp failed".into());
            }
        }
    }

    Ok(())
}
