# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Single-binary Rust CLI that downloads swissdamed UDI (Unique Device Identification) data, actors, and mandates from `swissdamed.ch` and exports as CSV and/or SQLite. Output files are date-stamped and organized into `csv/` and `db/` subdirectories (e.g., `csv/swissdamed_25.02.2026.csv`, `db/actors_25.02.2026.db`).

## Configuration

Sensitive defaults (scp target, Google Drive credentials) are stored in `config.toml`, which is gitignored. Before building for the first time, copy the sample:

```bash
cp config.sample.toml config.toml
# then edit config.toml and fill in real values
```

CLI arguments always override `config.toml`. If a required value is missing from both, the app shows an error dialog and exits. `config.sample.toml` is committed and contains empty-string placeholders; `config.toml` is gitignored and holds real credentials. The release and CI workflows copy `config.sample.toml → config.toml` automatically before building.

## Build & Run

```bash
cp config.sample.toml config.toml  # first time only
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
cargo run -- --migel-stats           # re-render stats PNG from latest DBs (no download)
cargo run -- --sigvaris-shop         # scrape shop.sigvaris.com → GTIN→MiGeL override DB
cargo run -- --lookup-chrn CHRN-AR-20000807  # find all SRNs for a given CHRN
cargo run -- --company-ranking               # rank companies by product count
cargo run -- --unique-srns                   # export all unique SRNs with manufacturer + mandate holder
cargo run -- --csv --gdrive --gdrive-sub user@domain.com  # upload CSV to Google Drive
cargo run -- --company-ranking --mailto "a@gs1.ch,b@gs1.ch" --mail-subject "Subject" --gdrive-sub user@domain.com  # email CSV
```

No tests exist. No linter/formatter configuration — use `cargo fmt` and `cargo clippy`.

## Architecture

Modular Rust binary. `src/main.rs` holds CLI parsing (`Args`), `app_data_dir()`, config loading, error-dialog plumbing, and dispatch into the modules below:

- `src/data.rs` — JSON → header/row flattening (`collect_headers`, `build_rows`, `collect_flat_headers`, `build_flat_rows`).
- `src/download.rs` — paginated POSTs to the swissdamed.ch API (`download_all_pages*`, `load_json_file`).
- `src/export.rs` — `write_csv` (UTF-8 BOM), `write_sqlite[_table]` (identifier-quoted SQL), `output_csv`/`output_db` path helpers.
- `src/diff.rs` — `diff_csv_files` (compares two CSVs by `udiDiCode`).
- `src/gdrive.rs` — JWT-signed Google Drive upload + Gmail send (RFC 2047 subject encoding).
- `src/migel.rs` — Aho-Corasick MiGeL matching engine (shared with fb2sqlite).
- `src/migel_stats.rs` — pure-Rust stats PNG renderer via `plotters` (`generate`, `find_latest_dbs`, `read_stats`).
- `src/sigvaris_shop.rs` — scrapes `shop.sigvaris.com` Shopify endpoints, derives MiGeL codes per GTIN, persists to `db/sigvaris_shop_DD.MM.YYYY.db`. Exposes `find_latest_db` + `load_overrides` consumed by `run_migel` as a GTIN→MiGeL precedence layer.
- `src/error_report.rs` — SRN validation and XSS-escaped HTML error report.
- `src/reports.rs` — high-level workflows: `run_migel`, `run_ch_rep[_mandates]`, `run_ar_mandates`, `run_lookup_chrn`, `run_company_ranking`, `run_unique_srns`.
- `src/gui.rs` — egui/eframe GUI (background worker, error dialog).

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
- Version shown only in window title bar (not duplicated inside the window)
- On Windows release builds: `windows_subsystem = "windows"` hides console window; CLI mode re-attaches parent console via `AttachConsole(ATTACH_PARENT_PROCESS)`

### Output Directory

All output files go to `~/swissdamed2sqlite/` (`app_data_dir()`):
- macOS sandbox: container directory
- Windows: `%USERPROFILE%\swissdamed2sqlite\`
- Linux/macOS: `~/swissdamed2sqlite/`
- Subdirectories: `csv/`, `db/`, `diff/`, `html/`, `logs/`

### CLI Key flow:

1. **CLI parsing** — `clap` derive API (`Args` struct). Flags: `--csv`, `--sqlite`, `--file`, `--page-size`, `--deploy`, `--scp`, `--diff`, `--actors`, `--mandates`, `--ar-mandates`, `--ch-rep`, `--ch-rep-mandates`, `--ar-only`, `--lookup-chrn`, `--gdrive`, `--gdrive-folder`, `--gdrive-key`, `--gdrive-email`, `--gdrive-sub`, `--mailto`, `--mail-subject`, `--company-ranking`, `--unique-srns`, `--migel`, `--migel-stats`. If neither `--csv` nor `--sqlite` is given, both are produced. `--deploy` implies `--sqlite`. `--diff` takes two CSV paths and skips download/export.
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

## CI/CD

### CI (`.github/workflows/ci.yml`)

Triggered on every push (non-`v*` tags) and pull request. Builds all three platforms in parallel (macOS universal, Linux, Windows) without signing, packaging, or releasing. Copies `config.sample.toml → config.toml` before building.

### Release (`.github/workflows/release.yml`)

Triggered by `git tag v* && git push --tags`. Builds for all platforms in parallel:
- **macOS**: universal binary (arm64 + x86_64), .app bundle with ICNS icon (generated from `assets/icon.iconset/` via `iconutil`), signed DMG (Developer ID), notarized, App Store .pkg (signed with Mac App Distribution + Mac Installer Distribution certs) uploaded via `xcrun altool` (iTMSTransporter fallback)
- **Windows**: portable ZIP, signed MSIX, Microsoft Store submission via Partner Center API (listings, pricing=Free, visibility=Public, publishMode=Immediate)
- **Linux**: tar.gz + AppImage
- **GitHub Release**: collects all artifacts via `softprops/action-gh-release`
- Version synced from git tag to Cargo.toml automatically

Platform configs: `build.rs` (Windows icon), `entitlements.plist` / `entitlements-appstore.plist` (macOS), `windows/AppxManifest.xml` + `windows/assets/` (MSIX/Store).

Store screenshots: `screenshots/windows/` (PNG, 1366x768+), `screenshots/macos/` (PNG, 1280x800 / 1440x900 / 2560x1600 / 2880x1800).

### winit Patch (App Store Compliance)

The `winit` crate is patched locally (`winit-patched/`) to remove `_CGSSetWindowBackgroundBlurRadius` — a private macOS API that causes App Store rejection. Applied via `[patch.crates-io]` in `Cargo.toml`. Same patch used in eudamed2firstbase/eudamed2swissdamed.

### macOS Signing Details

- DMG: signed with `Developer ID Application: ywesee GmbH` + `entitlements.plist`, notarized via `notarytool`
- App Store .pkg: re-signed with `Apple Distribution` / `Mac App Distribution` / `3rd Party Mac Developer Application` + `entitlements-appstore.plist`, packaged with `3rd Party Mac Developer Installer`
- Provisioning profile (`MACOS_PROVISIONING_PROFILE` secret) must use `MAC_APP_DISTRIBUTION` cert type (not `DISTRIBUTION`) to match the signing identity
- ICNS icon generated at build time from `assets/icon.iconset/` (contains 16x16 through 512x512@2x PNGs)

## Key Details

- All SQLite columns are TEXT type; no numeric types used.
- CSV output includes a UTF-8 BOM (`\xEF\xBB\xBF`) for Excel compatibility.
- The API client uses a browser-like User-Agent and cookie jar.
- `rusqlite` uses the `bundled` feature (ships its own SQLite, no system dependency needed).

## MiGeL Matching (src/migel.rs + src/sigvaris_shop.rs)

Two-layer pipeline reaching ~9,510 matches from ~45,204 rows (~21%):

**Layer 1 — GTIN overrides** (`src/sigvaris_shop.rs`): scrapes `shop.sigvaris.com` Shopify endpoints (~432 products, ~18k variants), derives MiGeL codes per GTIN from `option2` (Kompressionsklasse) + `product_type` (Wadenstrumpf/Schenkelstrumpf/Strumpfhose/Maternity/Armkompressionsstrumpf/Flachstrick/Wraps/Anziehhilfe), then persists to `db/sigvaris_shop_DD.MM.YYYY.db` with table `sigvaris_shop_variants(gtin13, gtin14, sku, title, product_type, klasse, migel_code, migel_reason)`. Trigger via `--sigvaris-shop`; takes ~7 min with 1s throttle, retries on 403, fail-safes against empty overwrites. In `run_migel`, `find_latest_db` + `load_overrides` build a `HashMap<gtin, Option<MigelCode>>` consulted before the heuristic matcher: `Some(code)` → assign that code, `None` → explicit skip (BAG Kap.17: Stützstrumpf / Anti-Thrombose / Reisestrumpf / Klasse 1 are NOT Pflichtleistung).

**Layer 2 — Heuristic matcher** (shared with fb2sqlite). Adds ~1,650 matches for non-SIGVARIS manufacturers via Aho-Corasick. Key features:
- **Aho-Corasick** automaton for single-pass candidate finding
- **IDF-weighted ranking** (capped at 5.0) for choosing the best MiGeL code
- **English-to-German enrichment**: ~80 medical terms translated (e.g., "knee" → "knie knieorthese", "nebulizer" → "vernebler aerosol", "scoli" → "skoliose rumpf orthesen"); context-aware: "ortho" + "rehab" → "spezialschuhe"
- **Category hierarchy keywords** from MiGeL XLSX parent categories
- **Per-language scoring**: DE (suffix + fuzzy >= 6 chars + compound decomposition), FR/IT (exact word only)
- **Precision filters**: stop words, universal exclusions (PTA/stent/ERCP/surgical gloves), negative keywords per MiGeL code, company exclusions (Varian, Sunstar)
- **Thresholds**: 2+ keywords: score >= 0.3, max len >= 6; single keyword: score >= 0.5, len >= 8 (>= 0.7 for verbose)
- swissdamed-specific: company exclusions for radiation therapy (Varian) and dental (Sunstar) in main.rs
- Key matches: Künzli shoes (464), Aspen orthoses (272), Guido Buschmeier infusion sets (40), PRIM (15), Angelini ThermaCare (14), O2 concentrators (4), nebulizers (2), CGM sensors (1), condoms (2), prosthetics (1)
- Auto-generates timestamped stats PNG (`swissdamed_migel_stats_hhHmm.dd.mm.yyyy.png`) after each run via `src/migel_stats.rs` (pure Rust, `plotters` crate). Renders title, key-metrics card, company donut, and top-categories horizontal bar chart; updates the README image link and removes prior timestamped PNGs. Use `--migel-stats` to re-render from the latest DBs without re-downloading.
