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
cargo run -- --migel --linkedin      # match + generate PNG + publish PNG to LinkedIn
cargo run -- --migel --twitter       # match + generate PNG + publish PNG to X / Twitter
cargo run -- --migel --linkedin --twitter  # publish to both
cargo run -- --migel-stats           # re-render stats PNG from latest DBs (no download)
cargo run -- --migel-stats --linkedin  # re-render PNG and publish to LinkedIn
cargo run -- --migel-stats --twitter   # re-render PNG and publish to X / Twitter
cargo run -- --sigvaris-shop         # scrape shop.sigvaris.com → GTIN→MiGeL override DB
cargo run -- --lookup-chrn CHRN-AR-20000807  # find all SRNs for a given CHRN
cargo run -- --company-ranking               # rank companies by product count
cargo run -- --unique-srns                   # export all unique SRNs with manufacturer + mandate holder
cargo run -- --csv --gdrive --gdrive-sub user@domain.com  # upload CSV to Google Drive
cargo run -- --company-ranking --mailto "a@gs1.ch,b@gs1.ch" --mail-subject "Subject" --gdrive-sub user@domain.com  # email CSV
```

`cargo test` runs the golden-set regression test (`migel::tests::golden_set`): ~280 rows in `tests/fixtures/golden_set.tsv` (genuine-match exemplars per company × code family, every verified FP cluster as expected-NONE, the excluded-company list, deliberate never-match exemplars, forced-match pins) evaluated against the pinned MiGeL XLSX in `tests/fixtures/migel.xlsx`. Run it after ANY matcher rule change; if a delta is intended, verify each failing row and regenerate the fixture from a verified run. No linter/formatter configuration — use `cargo fmt` and `cargo clippy`.

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
- `src/linkedin.rs` — LinkedIn Image upload + Posts API. Reads `linkedin_credentials.json` + `linkedin_token.json` (cwd, then `$HOME`) — same files as `li_push_rs`. Refreshes the token if a `refresh_token` is present and persists it back. Caption auto-built from the MiGeL DB (matched count, %, distinct codes, companies, top manufacturers, top categories). Optional `SWISSDAMED_CAPTION_EXTRA` env var is prepended to the caption (used for one-off context like daily-additions summaries). Triggered by `--linkedin` on `--migel` and `--migel-stats`; failure is non-fatal (logged, exit 0).
- `src/twitter.rs` — X / Twitter media upload (`/2/media/upload`) + tweet create (`/2/tweets`), OAuth 1.0a-signed (HMAC-SHA1, same shape as gigacrawl). Reads `twitter_credentials.json` (cwd, then `$HOME`) with `consumer_key` + `consumer_secret` + user-access `token` + `secret`; falls back to the first profile in `~/.twurlrc`. Caption is a compact (<280-char) summary built from the MiGeL DB; optional `SWISSDAMED_CAPTION_EXTRA` env var is prepended (keep it short — tweet budget after base ~206 chars is roughly 70 chars). Triggered by `--twitter` on `--migel` and `--migel-stats`; failure is non-fatal.
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

Two-layer pipeline reaching ~12,500 matches from ~84,700 rows (~14.7%):

**Layer 1 — GTIN overrides** (`src/sigvaris_shop.rs`): scrapes `shop.sigvaris.com` Shopify endpoints (~432 products, ~18k variants), derives MiGeL codes per GTIN from `option2` (Kompressionsklasse) + `product_type` (Wadenstrumpf/Schenkelstrumpf/Strumpfhose/Maternity/Armkompressionsstrumpf/Flachstrick/Wraps/Anziehhilfe), then persists to `db/sigvaris_shop_DD.MM.YYYY.db` with table `sigvaris_shop_variants(handle, gtin13, gtin14, sku, title, product_type, klasse, migel_code, migel_reason)`. Trigger via `--sigvaris-shop`; takes ~7 min with 1s throttle, retries on 403. In `run_migel`, `find_latest_db` + `load_overrides` build a `HashMap<gtin, Option<MigelCode>>` consulted before the heuristic matcher: `Some(code)` → assign that code, `None` → explicit skip (BAG Kap.17: Stützstrumpf / Anti-Thrombose / Reisestrumpf / Klasse 1 are NOT Pflichtleistung).

The scraper is **resume-capable**: per-product variants are appended to `db/sigvaris_shop_partial.db` (with a `done_handles` checkpoint table) immediately after each successful fetch. Rerunning `--sigvaris-shop` skips already-processed handles. When an individual fetch fails after retries, the scraper falls back to the cached variants for that handle from the previous finalized DB. The master handle list per run is the union of (newly discovered) ∪ (partial done) ∪ (baseline DB handles), so Cloudflare-blocked discovery doesn't drop known products. A final **80 % baseline floor** rejects partial scrapes: if the final variant count is below 80 % of the previous finalized DB, the partial is preserved and a future `--sigvaris-shop` resumes from it (no destruction of the override DB). `find_latest_db` excludes `sigvaris_shop_partial.db` and sorts by date-from-filename (`DD.MM.YYYY` → `YYYYMMDD`) so a manually restored older DB is honoured even if its mtime is fresher than a junk replacement.

**Layer 2 — Heuristic matcher** (shared with fb2sqlite). Adds ~4,600 matches for non-SIGVARIS manufacturers via Aho-Corasick. Match order inside `find_best_migel_match`: (1) **FORCED_MATCHES**, (2) **metadata gate**, (3) universal exclusions, (4) Aho-Corasick scoring. Key features:
- **FORCED_MATCHES** (`src/migel.rs`): curated recall pins `(all_of, none_of, position_nr)` checked against the RAW pre-enrichment text — for verified brand/category-exclusive tokens where scoring can't reach the right position (verbose Bezeichnung dilutes the single-keyword score below threshold, e.g. "Krücken für Erwachsene, ergonomischer Griff") or IDF would drift to a sibling. Rules: GCE MediSelect/MediReg→14.10.42 (O2 regulators), gehstuetze→10.01.01 (crutches), contact lens/kontaktlinse→25.01.01, Respironics Trilogy/Garbin/BiPAP-A30/A40→14.12.02 (home ventilators), DreamStation/System One/REMstar/Dorma/BiPAP→14.11.02 (autoSV→14.11.03) with accessory stop-lists (circ/tubing/humidifier/mask/filter/accessor/...), spirobank/spirometer→21.01.15, omnipod→03.02.01, doff→17.12.01.01. Rule order matters (first match wins; "bipap a30/a40" ventilators before bare "bipap"). Forced pins deliberately outrank the metadata gate (Omnipod 5 is CLASS_III yet genuine MiGeL).
- **Metadata hard gate**: `is_metadata_excluded()` — deviceType IVDR/IVDD and riskClass CLASS_III rows (~5.2% of corpus) never reach the heuristic matcher, immunizing them against keyword drift.
- **Aho-Corasick** automaton for single-pass candidate finding
- **IDF-weighted ranking** (capped at 5.0) for choosing the best MiGeL code; ties broken deterministically by position_nr (candidates come from a HashSet — without the tiebreak, sibling positions like Kauf/Miete variants flipped randomly between runs, producing phantom daily diffs)
- **English-to-German enrichment**: ~85 medical terms translated (e.g., "knee" → "knie knieorthese", "nebulizer" → "vernebler aerosol", "petrolatum" → "impraegnierte wundkompresse"); context-aware: "ortho" + "rehab" → "spezialschuhe". Region-gated recall blocks: compression `garment`→Leib/Rumpf 05.11 (body) / 17.15.01 (leg) / 17.15.03 (arm) / 17.15.05 (face/head/neck/ear) via "<region> garment" **bigrams** with precedence leg > arm > face > ear (bare region words mis-route: Macom's deviceName "Leg, Arm and Ear Garments" feeds all three words to every row); `ostomy`/`stomabandage`→Stoma-/Fistelversorgung 29.01; `urine`+`bag` / `leg bag`→Bein-/Bettbeutel 15.14/15.15 incl. accessory positions (Beinbeuteltasche/Haltebänder/Halterung); `superabsorbent`/`gelling`→35.05.05/.07; `Halskrawatte`→Cervikalstütze; `Gilchrist`→Schultergürtel-Orthese 22.09; pen+needle→Penkanülen 03.07.09; insulin+syringe→03.07.10.10; incontinence/inkontinenz-compounds→15.01; CGM gate (glucose+monitoring+continuous|flash)→Sensoren 21.07.02 / Lesegerät 21.07.01; breast+pump→Milchpumpen 01.01; ketone strips→21.03.01.03; armtraggurt / arm+sling→Armtraggurten 05.10; cast/post-op/offloading shoe→Spezialschuhe für Verbände 26.01.04.02/.03.
- **Category hierarchy keywords** from MiGeL XLSX parent categories
- **Per-language scoring**: DE (suffix + fuzzy >= 6 chars + compound decomposition), FR/IT (exact word only). `COMPOUND_PREFIXES` includes body-part prefixes (knie/ellenbogen/sprunggelenk/unterschenkel/finger/inkontinenz) so German one-word compounds like `Knieschiene`→`knie`+`schiene` match the body-part keyword.
- **Precision filters**: stop words (incl. generic FR company tokens `fabrication`/`medicaux`/`produits`/`conception` that otherwise leak in via the appended company name), universal exclusions (PTA/stent/ERCP/surgical gloves, AGFA imaging, CSF/ventricular catheters, staining reagents, traction devices, full-body garments, hot/cold compresses — lift that one if a ch.16 recall rule is ever added), ~75 negative keywords per MiGeL code prefix incl. chapter-wide rules (catheters / blood-pressure monitors / coils / arrays ∉ orthosis chapters 22/23), company exclusions.
- **Thresholds**: 2+ keywords: score >= 0.3, max len >= 6; single keyword: score >= 0.5, len >= 8 (>= 0.7 for verbose)
- swissdamed-specific company exclusions live in the shared `EXCLUDED_COMPANIES` const in `src/migel.rs` (single source of truth used by both `src/reports.rs` and `src/gui.rs`; exact-string match on companyName): ~40 entries — radiation therapy (Varian), dental (Sunstar, Dr. Jean Bausch, Alpha-Bio), transfer furniture (Diacor), sleep-lab sensors (SOMNOmedics, Braebon, Lifemotion, Itamar — the 21.07.02 "Sensoren" magnet), ECMO/ICU (Maquet ×2), surgical (Accuratus, Aesculap, MANI, Oertli, Silony), imaging (Philips entities, Invivo), heat wraps (Angelini ThermaCare — a proven code-hopper), contraceptive condoms (RFSU), cosmetic prostheses (Steeper), emergency trauma (SAM), IV cannulas (BD Infusion Therapy — 'Infusion' in the company name itself triggers), vascular closure (Cordis), factory insoles (Dongguan Jiuhui), cath-lab (medK), etc. Each verified: entire matched output was false positives.
- Key matches: Macom/LymphCare compression garments (~1,300 post-liposuction/lymphedema, region-routed), Künzli shoes (464), GCE O2 regulators (509), Aspen orthoses, REBOTEC crutches (216), Respironics home ventilators + CPAP/BiPAP + InnoSpire nebulizers (~180), Achim Ruthner German orthoses + Stomabandagen, Huizhou Foryou dressings, Salts ostomy, Genray/embecta pen needles (93), contact lenses (92), Primecare urine bags + accessories (~90), Guido Buschmeier infusion sets/stands, PRIM, ESSITY TENA + retail incontinence, MIR spirometers, Omnipod patch pumps (forced pin overrides the CLASS_III gate), Derma Sciences petrolatum gauze, breast pumps, O2 concentrators, nebulizers, prosthetics.
- **Precision/recall iteration (Jun 2026)**: a diagnostic workflow audited the full matched set and removed ~1,120 false positives (company-name keyword contamination → GO-TAPER dental files matching compression pantyhose; unconditional body-part→orthese enrichment → wrist BP monitor / knee surgical guide; "Compression Bra" 558× → Arm-Kompressionsbandage; "transfer" furniture; dental alginate; pure non-MiGeL makers) while adding ~740 correct matches (the recall blocks above). Adversarially-verified: blanket-gating the body-part enrichment was rejected because ~150 correct Aspen/Span-Link/PRIM orthoses depend on it; the FPs were killed surgically with negative keywords instead.
- **Full audit (Jul 2026, see `MIGEL_AUDIT_02.07.2026.md`)**: 108-agent adversarially-verified audit of precision + recall + architecture. Pruned 624 verified FP rows (Macom "Full Body Garment" 456 — no full-body position exists; 20 new company exclusions; universal exclusions; hop-corrections), re-routed 84 wrong-code rows (Macom leg/ear garments carried the arm code; cast shoes on the Orthesen instead of Verbände sub-position; arm slings to 05.10 Armtraggurten), and added ~1,400 verified recall rows via FORCED_MATCHES + new enrichment rules. Introduced the metadata hard gate, the shared EXCLUDED_COMPANIES const, deterministic tie-breaking, and the golden-set regression test. Category-level precision ~99.9%. The §2b clusters TZMO Seni (windelhose/vorlage triggers, +33), HANS HEPP first-aid refills (+14 incl. ZCC bonus), IVF Hartmann DermaPlast retail (+18) and SIGVARIS Inc. MAK wraps (+245 → ch.17.06, previously empty; Compreboot→Fuss, Coolflex Calf→Wade, Strap Extender→Zubehör .10.1, generic Compreflex→Wade as accepted modal default since the rows carry zero body-region text) shipped 02.07.2026 (all trigger tokens verified single-company corpus-wide). Still open (maintainer decisions, see audit §2b): walker boots (187 rows, hinges on the 22-vs-23 prefab/custom policy), TZMO Fixierhosen/light pads; plus audit §4 architecture items #5–#8 (scoped exclusion semantics, domain priors, TOML rule externalization, keyword-generation/morphology fixes).
- Auto-generates timestamped stats PNG (`png/swissdamed_migel_stats_hhHmm.dd.mm.yyyy.png` under the app data dir) after each run via `src/migel_stats.rs` (pure Rust, `plotters` crate). Renders title, key-metrics card, company donut, and top-categories horizontal bar chart; updates the README image link (`png/...`) and removes prior timestamped PNGs in `png/` (and sweeps any legacy ones from the cwd). Use `--migel-stats` to re-render from the latest DBs without re-downloading. Add `--linkedin` to also publish the freshly generated PNG to LinkedIn via `src/linkedin.rs`.
