# swissdamed2sqlite

Download swissdamed UDI (Unique Device Identification) data, actors, and mandates from [swissdamed.ch](https://swissdamed.ch) and export as CSV and/or SQLite.

## Installation

Requires Rust toolchain. Then:

```bash
cargo build --release
```

The binary will be at `target/release/swissdamed2sqlite`.

## Usage

```bash
# Download and export both CSV and SQLite (default)
swissdamed2sqlite

# Export only CSV or only SQLite
swissdamed2sqlite --csv
swissdamed2sqlite --sqlite

# Load from a local JSON file instead of downloading
swissdamed2sqlite -f data.json --csv --sqlite

# Customize API page size (default: 50)
swissdamed2sqlite --page-size 100

# Export SQLite and deploy to remote server via scp
swissdamed2sqlite --sqlite --deploy

# Deploy to a custom scp target
swissdamed2sqlite --sqlite --deploy --scp user@host:/path/to/swissdamed.db

# Download actors
swissdamed2sqlite --actors
swissdamed2sqlite --actors --csv       # CSV only

# Download mandates
swissdamed2sqlite --mandates
swissdamed2sqlite --mandates --sqlite  # SQLite only

# Download both actors and mandates
swissdamed2sqlite --actors --mandates

# Join AR actors with their mandates
swissdamed2sqlite --ar-mandates

# Diff two CSV files (output to diff/ folder)
swissdamed2sqlite --diff swissdamed_24.02.2026.csv swissdamed_25.02.2026.csv
```

Output files are date-stamped:
- UDI: `swissdamed_25.02.2026.csv` / `.db`
- Actors: `actors_25.02.2026.csv` / `.db`
- Mandates: `mandates_25.02.2026.csv` / `.db`
- AR Mandates: `ar_mandates_25.02.2026.csv` / `.db`

## Output Format

- **CSV** — UTF-8 with BOM for Excel compatibility
- **SQLite** — single table per dataset (all TEXT columns). UDI table indexed on `udiDiCode` and `tradeName_*` columns

The nested `udiDis` array from the UDI API is flattened: each UDI DI entry becomes its own row with a `udiDiCode` column and per-language `tradeName_{lang}` columns.

- **Actors** — flat export from `swissdamed.ch/public/act/actors` (table: `actors`)
- **Mandates** — flat export from `swissdamed.ch/public/act/mandates` (table: `mandates`)
- **AR Mandates** — joins AR-type actors with their mandates into a single table (`ar_mandates`) with `actor_`/`mandate_` prefixed columns
- **Diff** — compares two CSVs by `udiDiCode`, outputs to `diff/diff_swissdamed_DD.MM.YYYY_DD.MM.YYYY.csv` with a `diff_status` column (`added`, `removed`, `changed_old`, `changed_new`)

## License

GPL-3.0 — see [LICENSE](LICENSE).
