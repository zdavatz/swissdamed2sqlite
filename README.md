# swissdamed2sqlite

Download swissdamed UDI (Unique Device Identification) data from [swissdamed.ch](https://swissdamed.ch) and export it as CSV and/or SQLite.

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
```

Output files are date-stamped, e.g. `swissdamed_25.02.2026.csv` and `swissdamed_25.02.2026.db`.

## Output Format

- **CSV** — UTF-8 with BOM for Excel compatibility
- **SQLite** — single `swissdamed` table with all TEXT columns, indexed on `udiDiCode` and `tradeName_*` columns

The nested `udiDis` array from the API is flattened: each UDI DI entry becomes its own row with a `udiDiCode` column and per-language `tradeName_{lang}` columns.

## License

GPL-3.0 — see [LICENSE](LICENSE).
