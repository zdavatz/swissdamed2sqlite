# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Single-binary Rust CLI that downloads swissdamed UDI (Unique Device Identification) data from `swissdamed.ch` and exports it as CSV and/or SQLite. Output files are date-stamped (e.g., `swissdamed_25.02.2026.csv`).

## Build & Run

```bash
cargo build              # debug build
cargo build --release    # release build
cargo run -- --csv --sqlite          # download and output both formats
cargo run -- --csv                   # CSV only
cargo run -- --sqlite                # SQLite only
cargo run -- -f data.json --sqlite   # load from local JSON instead of downloading
cargo run -- --sqlite --deploy       # build SQLite and scp to remote server
```

No tests exist. No linter/formatter configuration — use `cargo fmt` and `cargo clippy`.

## Architecture

Single-file application (`src/main.rs`, ~500 lines). Key flow:

1. **CLI parsing** — `clap` derive API (`Args` struct). Flags: `--csv`, `--sqlite`, `--file`, `--page-size`, `--deploy`, `--scp`. If neither `--csv` nor `--sqlite` is given, both are produced.
2. **Data acquisition** — `download_all_pages()` paginates POST requests to the swissdamed.ch public API, or `load_json_file()` reads a local JSON file.
3. **Schema discovery** — `collect_headers()` scans all JSON objects to discover columns dynamically. The `udiDis` nested array is flattened: each UDI DI entry becomes a separate row with `udiDiCode` + per-language `tradeName_{lang}` columns.
4. **Row building** — `build_rows()` produces a flat `Vec<Vec<String>>` with one row per udiDis entry, duplicating parent fields.
5. **Output** — `write_csv()` (UTF-8 BOM for Excel) and `write_sqlite()` (single `swissdamed` table, all TEXT columns, indexes on `udiDiCode` and `tradeName_*`).
6. **Deploy** — optional `scp` to a remote server.

## Key Details

- All SQLite columns are TEXT type; no numeric types used.
- CSV output includes a UTF-8 BOM (`\xEF\xBB\xBF`) for Excel compatibility.
- The API client uses a browser-like User-Agent and cookie jar.
- `rusqlite` uses the `bundled` feature (ships its own SQLite, no system dependency needed).
