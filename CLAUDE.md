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
cargo run -- --migel                 # match UDI devices to MiGeL codes
cargo run -- --migel --deploy        # match and deploy to remote server
```

No tests exist. No linter/formatter configuration — use `cargo fmt` and `cargo clippy`.

## Architecture

Two-file application: `src/main.rs` (CLI, download, output) + `src/migel.rs` (MiGeL matching engine, shared with fb2sqlite). Key flow:

1. **CLI parsing** — `clap` derive API (`Args` struct). Flags: `--csv`, `--sqlite`, `--file`, `--page-size`, `--deploy`, `--scp`, `--diff`, `--actors`, `--mandates`, `--ar-mandates`, `--ch-rep`. If neither `--csv` nor `--sqlite` is given, both are produced. `--deploy` implies `--sqlite`. `--diff` takes two CSV paths and skips download/export.
2. **Data acquisition** — `download_all_pages_from(base_url, label, page_size)` paginates POST requests to the swissdamed.ch public API, or `load_json_file()` reads a local JSON file. Three endpoints: UDI (`/public/udi/basic-udis`), actors (`/public/act/actors`), mandates (`/public/act/mandates`).
3. **Schema discovery** — `collect_headers()` for UDI (flattens `udiDis` nested array), `collect_flat_headers()` for actors/mandates (flat JSON).
4. **Row building** — `build_rows()` for UDI (one row per udiDis entry), `build_flat_rows()` for actors/mandates.
5. **Output** — `write_csv()` (UTF-8 BOM for Excel) and `write_sqlite_table()` (configurable table name, all TEXT columns). CSV files go to `csv/`, SQLite files go to `db/`. Directories are created automatically via `output_csv()` and `output_db()` helpers.
6. **Deploy** — optional `scp` to a remote server.
7. **Diff** — `diff_csv_files()` compares two CSVs by `udiDiCode` key, outputs a diff CSV to `diff/` with a `diff_status` column (`added`, `removed`, `changed_old`, `changed_new`).
8. **Actors/Mandates** — `download_and_export()` handles flat data download and export for actors and mandates endpoints.
9. **CH-REP** — `run_ch_rep()` downloads all actors, groups by `companyUid`, keeps only companies where all roles are AR and/or IM (no MF or PR under the same UID). Outputs filtered actor rows.
10. **AR Mandates** — `run_ar_mandates()` downloads both actors and mandates, filters AR-type actors, fetches individual mandate details via `/public/act/mandates/{id}` (provides SRN, mandateType, validFrom/validTo, full address), and produces a joined output with `actor_`/`mandate_` prefixed columns.

## Key Details

- All SQLite columns are TEXT type; no numeric types used.
- CSV output includes a UTF-8 BOM (`\xEF\xBB\xBF`) for Excel compatibility.
- The API client uses a browser-like User-Agent and cookie jar.
- `rusqlite` uses the `bundled` feature (ships its own SQLite, no system dependency needed).

## MiGeL Matching (src/migel.rs)

Shared matching engine (identical to fb2sqlite). Key features:
- **Aho-Corasick** automaton for single-pass candidate finding
- **IDF-weighted ranking** (capped at 5.0) for choosing the best MiGeL code
- **English-to-German enrichment**: ~60 medical terms translated (e.g., "knee" → "knie knieorthese"); context-aware: "ortho" + "rehab" → "spezialschuhe"
- **Category hierarchy keywords** from MiGeL XLSX parent categories
- **Per-language scoring**: DE (suffix + fuzzy >= 6 chars + compound decomposition), FR/IT (exact word only)
- **Precision filters**: stop words, universal exclusions (PTA/stent/ERCP/surgical gloves), negative keywords per MiGeL code, company exclusions (Varian, Sunstar)
- **Thresholds**: 2+ keywords: score >= 0.3, max len >= 6; single keyword: score >= 0.5, len >= 8 (>= 0.7 for verbose)
- swissdamed-specific: company exclusions for radiation therapy (Varian) and dental (Sunstar) in main.rs
