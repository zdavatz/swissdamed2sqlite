use chrono::Local;
use plotters::prelude::*;
use plotters::style::text_anchor::{HPos, Pos, VPos};
use rusqlite::Connection;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

const BG: RGBColor = RGBColor(255, 255, 255);
const TITLE_COLOR: RGBColor = RGBColor(51, 51, 51);
const TEXT_COLOR: RGBColor = RGBColor(85, 85, 85);
const SUBTLE_COLOR: RGBColor = RGBColor(119, 119, 119);
const ACCENT: RGBColor = RGBColor(46, 125, 50);
const BAR_COLOR: RGBColor = RGBColor(67, 160, 71);

const COMPANY_COLORS: &[RGBColor] = &[
    RGBColor(46, 125, 50),
    RGBColor(67, 160, 71),
    RGBColor(102, 187, 106),
    RGBColor(129, 199, 132),
    RGBColor(165, 214, 167),
    RGBColor(200, 230, 201),
    RGBColor(232, 245, 233),
    RGBColor(255, 245, 157),
    RGBColor(255, 204, 128),
    RGBColor(239, 154, 154),
    RGBColor(206, 147, 216),
];

const MIGEL_TOTAL_ITEMS: i64 = 786;

pub struct Stats {
    pub total_products: i64,
    pub total_matched: i64,
    pub num_migel_codes: i64,
    pub num_companies: i64,
    /// All companies sorted by match count desc.
    pub company_breakdown: Vec<(String, i64)>,
    /// Top 8 MiGeL categories: (bezeichnung, count, companies sorted desc)
    pub top_categories: Vec<(String, i64, Vec<(String, i64)>)>,
}

fn ch_fmt(n: i64) -> String {
    let s = n.abs().to_string();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    if n < 0 {
        out.push('-');
    }
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i) % 3 == 0 {
            out.push('\'');
        }
        out.push(*b as char);
    }
    out
}

pub fn read_stats(
    migel_db: &Path,
    full_db: Option<&Path>,
) -> Result<Stats, Box<dyn Error>> {
    let conn = Connection::open(migel_db)?;

    let total_matched: i64 =
        conn.query_row("SELECT COUNT(*) FROM swissdamed", [], |r| r.get(0))?;
    let num_migel_codes: i64 = conn.query_row(
        "SELECT COUNT(DISTINCT migel_code) FROM swissdamed",
        [],
        |r| r.get(0),
    )?;
    let num_companies: i64 = conn.query_row(
        "SELECT COUNT(DISTINCT companyName) FROM swissdamed",
        [],
        |r| r.get(0),
    )?;

    let mut stmt = conn.prepare(
        "SELECT companyName, COUNT(*) FROM swissdamed \
         GROUP BY companyName ORDER BY 2 DESC",
    )?;
    let company_breakdown: Vec<(String, i64)> = stmt
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?
        .filter_map(|r| r.ok())
        .collect();

    let mut stmt = conn.prepare(
        "SELECT migel_code, migel_bezeichnung, COUNT(*) FROM swissdamed \
         GROUP BY migel_code ORDER BY 3 DESC LIMIT 8",
    )?;
    let top_codes: Vec<(String, String, i64)> = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
            ))
        })?
        .filter_map(|r| r.ok())
        .collect();

    let mut top_categories = Vec::with_capacity(top_codes.len());
    for (code, bez, cnt) in top_codes {
        let mut companies_stmt = conn.prepare(
            "SELECT companyName, COUNT(*) FROM swissdamed \
             WHERE migel_code = ?1 GROUP BY companyName ORDER BY 2 DESC",
        )?;
        let companies: Vec<(String, i64)> = companies_stmt
            .query_map([&code], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
            })?
            .filter_map(|r| r.ok())
            .collect();
        top_categories.push((bez, cnt, companies));
    }

    let total_products = match full_db {
        Some(p) => {
            let conn2 = Connection::open(p)?;
            conn2
                .query_row("SELECT COUNT(*) FROM swissdamed", [], |r| r.get(0))
                .unwrap_or(0)
        }
        None => conn
            .query_row(
                "SELECT value FROM meta WHERE key = 'total_products'",
                [],
                |r| r.get::<_, String>(0),
            )
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0),
    };

    Ok(Stats {
        total_products,
        total_matched,
        num_migel_codes,
        num_companies,
        company_breakdown,
        top_categories,
    })
}

fn donut_wedge(
    cx: f64,
    cy: f64,
    r_outer: f64,
    r_inner: f64,
    a_start: f64,
    a_end: f64,
) -> Vec<(i32, i32)> {
    let steps = ((a_end - a_start).abs() * 64.0).ceil() as usize + 8;
    let mut pts = Vec::with_capacity(steps * 2 + 2);
    for i in 0..=steps {
        let t = a_start + (a_end - a_start) * (i as f64 / steps as f64);
        let x = cx + r_outer * t.cos();
        let y = cy - r_outer * t.sin();
        pts.push((x.round() as i32, y.round() as i32));
    }
    for i in 0..=steps {
        let t = a_end - (a_end - a_start) * (i as f64 / steps as f64);
        let x = cx + r_inner * t.cos();
        let y = cy - r_inner * t.sin();
        pts.push((x.round() as i32, y.round() as i32));
    }
    pts
}

pub fn render(stats: &Stats, out_path: &Path) -> Result<(), Box<dyn Error>> {
    const W: u32 = 1800;
    const H: u32 = 2500;

    let root = BitMapBackend::new(out_path, (W, H)).into_drawing_area();
    root.fill(&BG)?;

    let center_h = TextStyle::from(("sans-serif", 38).into_font())
        .color(&ACCENT)
        .pos(Pos::new(HPos::Center, VPos::Center));
    root.draw_text(
        "swissdamed MiGeL Matching Results",
        &center_h,
        (W as i32 / 2, 60),
    )?;

    let now = Local::now();
    let timestamp = now.format("%Hh%M-%d.%m.%Y").to_string();
    let ts_style = TextStyle::from(("sans-serif", 22).into_font())
        .color(&TEXT_COLOR)
        .pos(Pos::new(HPos::Right, VPos::Center));
    root.draw_text(&timestamp, &ts_style, (W as i32 - 40, H as i32 - 30))?;

    // ----- Top-left: Key metrics -----
    let panel_title = TextStyle::from(("sans-serif", 32).into_font().style(FontStyle::Bold))
        .color(&TITLE_COLOR);
    root.draw_text("Key Metrics", &panel_title, (110, 150))?;

    let pct_mapped = if stats.total_products > 0 {
        format!(
            "{:.1}%",
            stats.total_matched as f64 / stats.total_products as f64 * 100.0
        )
    } else {
        "N/A".to_string()
    };

    let metrics: Vec<(String, String)> = vec![
        (ch_fmt(stats.total_products), "Total swissdamed products".into()),
        (
            ch_fmt(stats.total_matched),
            format!("MiGeL matched ({})", pct_mapped),
        ),
        (stats.num_migel_codes.to_string(), "Distinct MiGeL codes".into()),
        (stats.num_companies.to_string(), "Companies with matches".into()),
        (MIGEL_TOTAL_ITEMS.to_string(), "Total MiGeL items in XLSX".into()),
    ];

    let value_style = TextStyle::from(
        ("sans-serif", 44).into_font().style(FontStyle::Bold),
    )
    .color(&ACCENT)
    .pos(Pos::new(HPos::Left, VPos::Center));
    let label_style = TextStyle::from(("sans-serif", 28).into_font())
        .color(&TEXT_COLOR)
        .pos(Pos::new(HPos::Left, VPos::Center));

    let metrics_top = 220;
    let metrics_step = 130;
    for (i, (value, label)) in metrics.iter().enumerate() {
        let y = metrics_top + i as i32 * metrics_step;
        root.draw_text(value, &value_style, (140, y))?;
        root.draw_text(label, &label_style, (430, y))?;
    }

    // ----- Top-right: Company donut -----
    let donut_title_style = TextStyle::from(
        ("sans-serif", 32).into_font().style(FontStyle::Bold),
    )
    .color(&TITLE_COLOR)
    .pos(Pos::new(HPos::Center, VPos::Center));
    let donut_cx = 1295.0_f64;
    let donut_cy = 600.0_f64;
    let r_outer = 235.0_f64;
    let r_inner = 130.0_f64;
    root.draw_text(
        "Matches by Company",
        &donut_title_style,
        (donut_cx as i32, 165),
    )?;

    let threshold = stats.total_matched as f64 * 0.015;
    let mut wedge_data: Vec<(String, i64)> = Vec::new();
    let mut other_total: i64 = 0;
    for (name, cnt) in &stats.company_breakdown {
        if (*cnt as f64) >= threshold {
            wedge_data.push((name.clone(), *cnt));
        } else {
            other_total += *cnt;
        }
    }
    if other_total > 0 {
        wedge_data.push(("Other".into(), other_total));
    }

    let total_for_pie: i64 = wedge_data.iter().map(|(_, c)| *c).sum();
    let mut a_cursor = std::f64::consts::FRAC_PI_2;
    let pct_style = TextStyle::from(("sans-serif", 26).into_font().style(FontStyle::Bold))
        .color(&TITLE_COLOR)
        .pos(Pos::new(HPos::Center, VPos::Center));

    for (idx, (_name, cnt)) in wedge_data.iter().enumerate() {
        let frac = *cnt as f64 / total_for_pie as f64;
        let sweep = frac * std::f64::consts::TAU;
        let a_end = a_cursor - sweep; // clockwise
        let color = COMPANY_COLORS[idx % COMPANY_COLORS.len()];
        let pts = donut_wedge(donut_cx, donut_cy, r_outer, r_inner, a_end, a_cursor);
        root.draw(&Polygon::new(pts, color.filled()))?;

        if frac >= 0.03 {
            let mid = (a_cursor + a_end) / 2.0;
            let r_label = (r_outer + r_inner) / 2.0;
            let lx = donut_cx + r_label * mid.cos();
            let ly = donut_cy - r_label * mid.sin();
            root.draw_text(
                &format!("{:.0}%", frac * 100.0),
                &pct_style,
                (lx.round() as i32, ly.round() as i32),
            )?;
        }
        a_cursor = a_end;
    }

    let center_count_style = TextStyle::from(
        ("sans-serif", 30).into_font().style(FontStyle::Bold),
    )
    .color(&ACCENT)
    .pos(Pos::new(HPos::Center, VPos::Center));
    let center_label_style = TextStyle::from(("sans-serif", 22).into_font())
        .color(&ACCENT)
        .pos(Pos::new(HPos::Center, VPos::Center));
    root.draw_text(
        &ch_fmt(stats.total_matched),
        &center_count_style,
        (donut_cx as i32, donut_cy as i32 - 14),
    )?;
    root.draw_text(
        "matches",
        &center_label_style,
        (donut_cx as i32, donut_cy as i32 + 22),
    )?;

    // Legend below donut, two columns
    let legend_top = (donut_cy + r_outer + 40.0) as i32;
    let legend_left = 870_i32;
    let legend_right = 1730_i32;
    let col_width = (legend_right - legend_left) / 2;
    let row_height = 32_i32;
    let legend_text = TextStyle::from(("sans-serif", 21).into_font())
        .color(&TEXT_COLOR)
        .pos(Pos::new(HPos::Left, VPos::Center));

    for (idx, (name, cnt)) in wedge_data.iter().enumerate() {
        let col = idx % 2;
        let row = idx / 2;
        let x0 = legend_left + col as i32 * col_width;
        let y = legend_top + row as i32 * row_height;
        let color = COMPANY_COLORS[idx % COMPANY_COLORS.len()];
        root.draw(&Rectangle::new(
            [(x0, y - 9), (x0 + 22, y + 9)],
            color.filled(),
        ))?;
        let truncated = truncate(name, 38);
        let entry = format!("{}  ({})", truncated, ch_fmt(*cnt));
        root.draw_text(&entry, &legend_text, (x0 + 32, y))?;
    }

    // ----- Bottom: Top MiGeL categories bar chart -----
    let bar_title_style = TextStyle::from(
        ("sans-serif", 32).into_font().style(FontStyle::Bold),
    )
    .color(&TITLE_COLOR);
    root.draw_text("Top MiGeL Categories", &bar_title_style, (110, 1140))?;

    let bar_area_left = 110_i32;
    let bar_area_right = 1720_i32;
    let bar_area_top = 1200_i32;
    let bar_area_bottom = 2460_i32;
    let n = stats.top_categories.len().max(1);
    let slot_height = (bar_area_bottom - bar_area_top) / n as i32;
    let bar_height = (slot_height as f64 * 0.4) as i32;
    let max_val = stats
        .top_categories
        .iter()
        .map(|(_, c, _)| *c)
        .max()
        .unwrap_or(1);
    let plot_max = (max_val as f64 * 1.15).max(1.0);
    let bar_x_max = (bar_area_right - bar_area_left - 80) as f64;

    let cat_label_style = TextStyle::from(
        ("sans-serif", 26).into_font().style(FontStyle::Bold),
    )
    .color(&TEXT_COLOR)
    .pos(Pos::new(HPos::Left, VPos::Center));
    let cat_companies_style =
        TextStyle::from(("sans-serif", 20).into_font().style(FontStyle::Italic))
            .color(&SUBTLE_COLOR)
            .pos(Pos::new(HPos::Left, VPos::Center));
    let bar_inside_style = TextStyle::from(
        ("sans-serif", 22).into_font().style(FontStyle::Bold),
    )
    .color(&BG)
    .pos(Pos::new(HPos::Center, VPos::Center));
    let bar_outside_style = TextStyle::from(
        ("sans-serif", 28).into_font().style(FontStyle::Bold),
    )
    .color(&TEXT_COLOR)
    .pos(Pos::new(HPos::Left, VPos::Center));

    for (i, (bez, cnt, companies)) in stats.top_categories.iter().enumerate() {
        let slot_top = bar_area_top + i as i32 * slot_height;
        let slot_mid = slot_top + slot_height / 2;
        let bar_y_top = slot_mid - bar_height / 2;
        let bar_y_bot = slot_mid + bar_height / 2;
        let width_px = ((*cnt as f64 / plot_max) * bar_x_max).round() as i32;

        // Category name above the bar
        root.draw_text(
            &truncate(bez, 100),
            &cat_label_style,
            (bar_area_left, bar_y_top - 30),
        )?;

        // The bar itself
        root.draw(&Rectangle::new(
            [
                (bar_area_left, bar_y_top),
                (bar_area_left + width_px, bar_y_bot),
            ],
            BAR_COLOR.filled(),
        ))?;

        // Count: inside if bar wide enough, otherwise outside
        if width_px as f64 > bar_x_max * 0.08 {
            root.draw_text(
                &ch_fmt(*cnt),
                &bar_inside_style,
                (bar_area_left + width_px / 2, slot_mid),
            )?;
        } else {
            root.draw_text(
                &ch_fmt(*cnt),
                &bar_outside_style,
                (bar_area_left + width_px + 12, slot_mid),
            )?;
        }

        // Company list below the bar
        let companies_text: String = companies
            .iter()
            .map(|(n, c)| format!("{} ({})", n, c))
            .collect::<Vec<_>>()
            .join(", ");
        root.draw_text(
            &truncate(&companies_text, 180),
            &cat_companies_style,
            (bar_area_left, bar_y_bot + 28),
        )?;
    }

    root.present()?;
    Ok(())
}

fn truncate(s: &str, max: usize) -> String {
    let count = s.chars().count();
    if count <= max {
        s.to_string()
    } else {
        let cutoff: String = s.chars().take(max.saturating_sub(1)).collect();
        format!("{}…", cutoff)
    }
}

fn update_readme(out_filename: &str) -> Result<(), Box<dyn Error>> {
    let path = Path::new("README.md");
    if !path.exists() {
        return Ok(());
    }
    let content = fs::read_to_string(path)?;
    let mut new_content = String::with_capacity(content.len());
    let mut updated = false;
    for line in content.lines() {
        if line.starts_with("![MiGeL Matching Stats](swissdamed_migel_stats")
            && line.ends_with(".png)")
        {
            new_content.push_str(&format!(
                "![MiGeL Matching Stats]({})",
                out_filename
            ));
            updated = true;
        } else {
            new_content.push_str(line);
        }
        new_content.push('\n');
    }
    if updated && new_content.trim_end() != content.trim_end() {
        fs::write(path, new_content)?;
        eprintln!("Updated README.md -> {}", out_filename);
    }
    Ok(())
}

fn cleanup_old_pngs(keep_filename: &str) -> Result<(), Box<dyn Error>> {
    for entry in fs::read_dir(".")? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.starts_with("swissdamed_migel_stats_")
            && name_str.ends_with(".png")
            && name_str != keep_filename
        {
            if let Err(e) = fs::remove_file(entry.path()) {
                eprintln!("Could not remove old {}: {}", name_str, e);
            } else {
                eprintln!("Removed old {}", name_str);
            }
        }
    }
    Ok(())
}

pub fn generate(
    migel_db: &Path,
    full_db: Option<&Path>,
) -> Result<PathBuf, Box<dyn Error>> {
    let stats = read_stats(migel_db, full_db)?;
    let timestamp = Local::now().format("%Hh%M.%d.%m.%Y").to_string();
    let out_filename = format!("swissdamed_migel_stats_{}.png", timestamp);
    render(&stats, Path::new(&out_filename))?;
    eprintln!("Saved {}", out_filename);
    update_readme(&out_filename)?;
    cleanup_old_pngs(&out_filename)?;
    Ok(PathBuf::from(out_filename))
}

/// Find the latest swissdamed_migel_*.db and swissdamed_<date>.db in the
/// app data dir's `db/` subdirectory.
pub fn find_latest_dbs(db_dir: &Path) -> (Option<PathBuf>, Option<PathBuf>) {
    let mut migel_dbs: Vec<PathBuf> = Vec::new();
    let mut full_dbs: Vec<PathBuf> = Vec::new();
    if let Ok(entries) = fs::read_dir(db_dir) {
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if name.ends_with(".db") {
                if name.starts_with("swissdamed_migel_") {
                    migel_dbs.push(e.path());
                } else if name.starts_with("swissdamed_")
                    && name
                        .trim_start_matches("swissdamed_")
                        .chars()
                        .next()
                        .map(|c| c.is_ascii_digit())
                        .unwrap_or(false)
                {
                    full_dbs.push(e.path());
                }
            }
        }
    }
    let pick_latest = |mut v: Vec<PathBuf>| -> Option<PathBuf> {
        v.sort_by_key(|p| {
            fs::metadata(p)
                .and_then(|m| m.modified())
                .ok()
        });
        v.pop()
    };
    (pick_latest(migel_dbs), pick_latest(full_dbs))
}
