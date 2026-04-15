# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Single-binary Rust CLI that downloads swissdamed UDI (Unique Device Identification) data, actors, and mandates from `swissdamed.ch` and exports as CSV and/or SQLite. Output files are date-stamped and organized into `csv/` and `db/` subdirectories (e.g., `csv/swissdamed_25.02.2026.csv`, `db/actors_25.02.2026.db`).

## Build & Run

```bash
cargo build              # debug build
cargo build --release    # release build
cargo run -- --csv --sqlite          # download and output both formats
cargo run -- --csv                   # CSV only
cargo run -- --sqlite                # SQLite only
cargo run -- -f data.json --sqlite   # load from local JSON instead of downloading
cargo run -- --sqlite --deploy       # build SQLite and scp to remote server
cargo run -- --diff old.csv new.csv  # diff two CSVs, output to diff/ folder
cargo run -- --actors                # download actors (CSV + SQLite)
cargo run -- --mandates              # download mandates (CSV + SQLite)
cargo run -- --actors --mandates     # download both
cargo run -- --ar-mandates           # join AR actors with their mandates
cargo run -- --ch-rep                # CH-REP only companies (only AR/IM, no MF/PR)
cargo run -- --ch-rep-mandates       # CH-REP companies ranked by mandate count
cargo run -- --ch-rep-mandates --ar-only  # AR-only CH-REPs ranked by mandate count
cargo run -- --migel                 # match UDI devices to MiGeL codes
cargo run -- --migel --deploy        # match and deploy to remote server
cargo run -- --lookup-chrn CHRN-AR-20000807  # find all SRNs for a given CHRN
cargo run -- --company-ranking               # rank companies by product count
cargo run -- --unique-srns                   # export all unique SRNs with manufacturer + mandate holder
cargo run -- --csv --gdrive --gdrive-sub user@domain.com  # upload CSV to Google Drive
cargo run -- --company-ranking --mailto "a@gs1.ch,b@gs1.ch" --mail-subject "Subject" --gdrive-sub user@domain.com  # email CSV
```

No tests exist. No linter/formatter configuration — use `cargo fmt` and `cargo clippy`.

## Architecture

Four-file application: `src/main.rs` (CLI, download, output, `app_data_dir()`) + `src/gui.rs` (egui/eframe cross-platform GUI) + `src/migel.rs` (MiGeL matching engine, shared with fb2sqlite) + `src/error_report.rs` (SRN validation and HTML error reports).

### GUI (`src/gui.rs`)

When launched without arguments, opens a native GUI window (egui/eframe, wgpu rendering). Features:
- **Download Products (CSV + SQLite)** — runs `download_all_pages()` + `write_csv()` + `write_sqlite()` in a background thread
- **Lookup SRNs for CHRN** — text input + button, mirrors `run_lookup_chrn()` logic
- **MiGeL Matching (SQLite)** — downloads UDI + MiGeL XLSX, runs Aho-Corasick matching, saves SQLite
- **Open Output/CSV/DB Folder** — opens `~/swissdamed2sqlite/` subdirectories
- Worker thread with `mpsc` channel for non-blocking UI updates
- Persistent settings saved to `~/swissdamed2sqlite/settings.json`
- Light theme (white background)
- App icon embedded in binary from `assets/icon_256x256.png`

### Output Directory

All output files go to `~/swissdamed2sqlite/` (`app_data_dir()`):
- macOS sandbox: container directory
- Windows: `%USERPROFILE%\swissdamed2sqlite\`
- Linux/macOS: `~/swissdamed2sqlite/`
- Subdirectories: `csv/`, `db/`, `diff/`, `html/`, `logs/`

### CLI Key flow:

1. **CLI parsing** — `clap` derive API (`Args` struct). Flags: `--csv`, `--sqlite`, `--file`, `--page-size`, `--deploy`, `--scp`, `--diff`, `--actors`, `--mandates`, `--ar-mandates`, `--ch-rep`, `--ch-rep-mandates`, `--ar-only`, `--lookup-chrn`, `--gdrive`, `--gdrive-folder`, `--gdrive-key`, `--gdrive-email`, `--gdrive-sub`, `--mailto`, `--mail-subject`, `--company-ranking`, `--unique-srns`. If neither `--csv` nor `--sqlite` is given, both are produced. `--deploy` implies `--sqlite`. `--diff` takes two CSV paths and skips download/export.
2. **Data acquisition** — `download_all_pages_from(base_url, label, page_size)` paginates POST requests to the swissdamed.ch public API, or `load_json_file()` reads a local JSON file. Three endpoints: UDI (`/public/udi/basic-udis`), actors (`/public/act/actors`), mandates (`/public/act/mandates`).
3. **Schema discovery** — `collect_headers()` for UDI (flattens `udiDis` nested array), `collect_flat_headers()` for actors/mandates (flat JSON).
4. **Row building** — `build_rows()` for UDI (one row per udiDis entry), `build_flat_rows()` for actors/mandates.
5. **Output** — `write_csv()` (UTF-8 BOM for Excel) and `write_sqlite_table()` (configurable table name, all TEXT columns). CSV files go to `csv/`, SQLite files go to `db/`. Directories are created automatically via `output_csv()` and `output_db()` helpers.
6. **Deploy** — optional `scp` to a remote server.
7. **Diff** — `diff_csv_files()` compares two CSVs by `udiDiCode` key, outputs a diff CSV to `diff/` with a `diff_status` column (`added`, `removed`, `changed_old`, `changed_new`).
8. **Actors/Mandates** — `download_and_export()` handles flat data download and export for actors and mandates endpoints.
9. **CH-REP** — `run_ch_rep()` downloads all actors, groups by `companyUid`, keeps only companies where all roles are AR and/or IM (no MF or PR under the same UID). Outputs filtered actor rows.
10. **AR Mandates** — `run_ar_mandates()` downloads both actors and mandates, filters AR-type actors, fetches individual mandate details via `/public/act/mandates/{id}` (provides SRN, mandateType, validFrom/validTo, full address), and produces a joined output with `actor_`/`mandate_` prefixed columns.
11. **CH-REP Mandates** — `run_ch_rep_mandates()` downloads actors and mandates, counts mandates per CH-REP company, outputs a ranked list (rank, companyName, companyUid, city, country, mandate_count). `--ar-only` restricts to companies with AR role (true CH-REPs, ~1,109 companies) vs all AR/IM companies (~2,271).
12. **Lookup CHRN** — `run_lookup_chrn()` finds all SRNs for a given CHRN (e.g. `CHRN-AR-20000807`). Downloads actors, matches by `chrn` field, fetches mandates and their details (which contain SRN), outputs joined actor+mandate CSV to `csv/CHRN-AR-20000807_14h30.28.03.2026.csv`.
13. **Google Drive upload** — `gdrive_upload_csv()` uploads CSV to Google Drive via service account .p12 key with domain-wide delegation. Uses JWT (RS256) auth, multipart/related upload to Drive API v3.
14. **Email attachment** — `send_email_with_attachment()` sends CSV as email attachment via Gmail API. Uses same service account delegation with `gmail.send` scope. Builds RFC 2822 MIME message with base64-encoded attachment. Supports `--mail-subject` for custom subject and comma-separated `--mailto` for multiple recipients. Non-ASCII subject lines (e.g. umlauts) are RFC 2047 encoded (`=?UTF-8?B?...?=`).
15. **Company Ranking** — `run_company_ranking()` downloads UDI data, counts unique `udiDiCode` per `companyName`, outputs a ranked CSV (`csv/company_ranking_DD.MM.YYYY.csv`) with columns: rank, companyName, produkte. Supports `--mailto` and `--gdrive`.
16. **Unique SRNs** — `run_unique_srns()` downloads actors and mandates, fetches mandate details for all AR actors, deduplicates by SRN, outputs `csv/unique_srns_DD.MM.YYYY.csv` (date-stamped) and `csv/srn_unique.csv` (latest snapshot, checked into repo) with columns: srn, manufacturer, mandateType, manufacturer_country, mandate_holder_chrn, mandate_holder_name, mandate_holder_uid. Invalid SRNs are filtered and written to an HTML error report.

## SRN Validation (src/error_report.rs)

- `is_valid_srn()` validates SRN format: 2-3 letter country code + `-MF-` or `-PR-` + 6+ digits. Tolerates minor variants (underscores, unicode dashes, missing dash before digits). Rejects `-AR-`/`-IM-` role types.
- `InvalidSrn` struct holds invalid SRN with manufacturer and mandate holder context.
- `write_srn_error_report()` generates `html/srn_error_report_HHhMM.dd.mm.yyyy.html` with styled table of invalid entries, deduplicated by SRN.

## Key Details

## Release & CI/CD (`.github/workflows/release.yml`)

Triggered by `git tag v* && git push --tags`. Builds for all platforms in parallel:
- **macOS**: universal binary (arm64 + x86_64), .app bundle, signed DMG, notarized, App Store .pkg upload via iTMSTransporter
- **Windows**: portable ZIP, signed MSIX, Microsoft Store submission via Partner Center API
- **Linux**: tar.gz + AppImage
- **GitHub Release**: collects all artifacts via `softprops/action-gh-release`
- Version synced from git tag to Cargo.toml automatically

Platform configs: `build.rs` (Windows icon), `entitlements.plist` / `entitlements-appstore.plist` (macOS), `windows/AppxManifest.xml` + `windows/assets/` (MSIX/Store).

## Key Details

- All SQLite columns are TEXT type; no numeric types used.
- CSV output includes a UTF-8 BOM (`\xEF\xBB\xBF`) for Excel compatibility.
- The API client uses a browser-like User-Agent and cookie jar.
- `rusqlite` uses the `bundled` feature (ships its own SQLite, no system dependency needed).

## MiGeL Matching (src/migel.rs)

Shared matching engine (identical to fb2sqlite). 837 matches from ~8,162 rows (10.3%). Key features:
- **Aho-Corasick** automaton for single-pass candidate finding
- **IDF-weighted ranking** (capped at 5.0) for choosing the best MiGeL code
- **English-to-German enrichment**: ~80 medical terms translated (e.g., "knee" → "knie knieorthese", "nebulizer" → "vernebler aerosol", "scoli" → "skoliose rumpf orthesen"); context-aware: "ortho" + "rehab" → "spezialschuhe"
- **Category hierarchy keywords** from MiGeL XLSX parent categories
- **Per-language scoring**: DE (suffix + fuzzy >= 6 chars + compound decomposition), FR/IT (exact word only)
- **Precision filters**: stop words, universal exclusions (PTA/stent/ERCP/surgical gloves), negative keywords per MiGeL code, company exclusions (Varian, Sunstar)
- **Thresholds**: 2+ keywords: score >= 0.3, max len >= 6; single keyword: score >= 0.5, len >= 8 (>= 0.7 for verbose)
- swissdamed-specific: company exclusions for radiation therapy (Varian) and dental (Sunstar) in main.rs
- Key matches: Künzli shoes (464), Aspen orthoses (272), Guido Buschmeier infusion sets (40), PRIM (15), Angelini ThermaCare (14), O2 concentrators (4), nebulizers (2), CGM sensors (1), condoms (2), prosthetics (1)
- Auto-generates `swissdamed_migel_stats.png` dashboard after each run (via `generate_migel_stats.py`, matplotlib). Timestamped copies saved as `swissdamed_migel_stats_hhHmm.dd.mm.yyyy.png`
