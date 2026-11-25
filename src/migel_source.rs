//! Where the MiGeL XLSX comes from.
//!
//! The BAG publishes a new Mittel- und Gegenständeliste every few months under a
//! new, unpredictable URL (`/dam/de/sd-web/<id>/<filename>.xlsx`). The download
//! URL used to be hard-coded, which is how the matcher kept running on the
//! 01.01.2026 edition long after the 01.07.2026 one was out. Now every run reads
//! the BAG MiGeL page, collects every `.xlsx` link, parses the edition date out
//! of each file name and downloads the newest.
//!
//! File names follow no convention (`migel-01012020-excel.xlsx`,
//! `migel_gesamtliste_010722_excel.xlsx`, `…per%2001.01.2026…`,
//! `…per%201.%20Juli%202026…`, `…korr20250204.xlsx`), so [`edition_date`]
//! tries several shapes and takes the left-most date in the name (a later
//! "korrigierte Version vom …" date never outranks the edition date). The page
//! lists the newest edition first; document order breaks ties.
//!
//! Fallbacks, in order: the newest page link → [`FALLBACK_URL`] (the newest
//! edition known when this was written) → the previously downloaded file on
//! disk. A run only fails if none of the three is available.

use std::error::Error;
use std::path::Path;

pub const BAG_MIGEL_PAGE: &str = "https://www.bag.admin.ch/de/mittel-und-gegenstaendeliste-migel";

/// Newest edition known when this module was written (per 01.07.2026).
pub const FALLBACK_URL: &str = "https://www.bag.admin.ch/dam/de/sd-web/4GEgEMgJ45A6/Mittel-%20und%20Gegenst%C3%A4ndeliste%20per%201.%20Juli%202026%20(in%20Excel-Format).xlsx";
pub const FALLBACK_EDITION: &str = "01.07.2026";

/// The XLSX a run used.
#[derive(Debug, Clone)]
pub struct MigelSource {
    pub url: String,
    /// Edition date `DD.MM.YYYY`, or "unknown".
    pub edition: String,
    /// How it was found: "bag-page", "fallback-url" or "cached-file".
    pub how: &'static str,
}

/// `YYYYMMDD` → `DD.MM.YYYY`.
pub fn format_key(key: u32) -> String {
    format!("{:02}.{:02}.{:04}", key % 100, (key / 100) % 100, key / 10000)
}

fn valid(y: u32, m: u32, d: u32) -> Option<u32> {
    if (2000..=2099).contains(&y) && (1..=12).contains(&m) && (1..=31).contains(&d) {
        Some(y * 10000 + m * 100 + d)
    } else {
        None
    }
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

const MONTHS: &[(&str, u32)] = &[
    ("januar", 1), ("februar", 2), ("märz", 3), ("maerz", 3), ("april", 4), ("mai", 5),
    ("juni", 6), ("juli", 7), ("august", 8), ("september", 9), ("oktober", 10),
    ("november", 11), ("dezember", 12), ("jan", 1), ("feb", 2), ("mar", 3), ("apr", 4),
    ("jun", 6), ("jul", 7), ("aug", 8), ("sep", 9), ("oct", 10), ("okt", 10), ("nov", 11),
    ("dec", 12), ("dez", 12),
];

/// Edition date of an XLSX file name as `YYYYMMDD`: the left-most date found.
pub fn edition_date(file_name: &str) -> Option<u32> {
    let s = percent_decode(file_name).to_lowercase();
    let c: Vec<char> = s.chars().collect();
    let n = c.len();
    let digits = |from: usize| -> usize {
        let mut j = from;
        while j < n && c[j].is_ascii_digit() {
            j += 1;
        }
        j - from
    };
    let num = |from: usize, len: usize| -> u32 { c[from..from + len].iter().collect::<String>().parse().unwrap_or(0) };
    let mut i = 0;
    while i < n {
        if !c[i].is_ascii_digit() || (i > 0 && c[i - 1].is_ascii_digit()) {
            i += 1;
            continue;
        }
        let l = digits(i);
        // D.M.YYYY / DD-MM-YYYY / D.M.YY
        if (1..=2).contains(&l) && i + l < n && (c[i + l] == '.' || c[i + l] == '-') {
            let sep = c[i + l];
            let j = i + l + 1;
            let lm = if j < n { digits(j) } else { 0 };
            if (1..=2).contains(&lm) && j + lm < n && c[j + lm] == sep {
                let k = j + lm + 1;
                let ly = if k < n { digits(k) } else { 0 };
                let (d, m) = (num(i, l), num(j, lm));
                let y = match ly {
                    4 => Some(num(k, 4)),
                    2 => Some(2000 + num(k, 2)),
                    _ => None,
                };
                if let Some(key) = y.and_then(|y| valid(y, m, d)) {
                    return Some(key);
                }
            }
            // "1. juli 2026"
            if sep == '.' {
                let rest: String = c[i + l + 1..].iter().collect();
                let rest = rest.trim_start();
                for (name, m) in MONTHS {
                    if let Some(after) = rest.strip_prefix(name) {
                        let after = after.trim_start();
                        let y: String = after.chars().take_while(|ch| ch.is_ascii_digit()).collect();
                        if y.len() == 4 {
                            if let Some(key) = valid(y.parse().unwrap_or(0), *m, num(i, l)) {
                                return Some(key);
                            }
                        }
                    }
                }
            }
        }
        // "01apr22"
        if l == 2 {
            let rest: String = c[i + 2..].iter().collect();
            for (name, m) in MONTHS {
                if name.len() == 3 {
                    if let Some(after) = rest.strip_prefix(name) {
                        let y: String = after.chars().take_while(|ch| ch.is_ascii_digit()).collect();
                        if y.len() == 2 {
                            if let Some(key) = valid(2000 + y.parse::<u32>().unwrap_or(0), *m, num(i, 2)) {
                                return Some(key);
                            }
                        }
                    }
                }
            }
        }
        match l {
            8 => {
                // DDMMYYYY, else YYYYMMDD
                if let Some(k) = valid(num(i + 4, 4), num(i + 2, 2), num(i, 2)) {
                    return Some(k);
                }
                if let Some(k) = valid(num(i, 4), num(i + 4, 2), num(i + 6, 2)) {
                    return Some(k);
                }
            }
            6 => {
                if let Some(k) = valid(2000 + num(i + 4, 2), num(i + 2, 2), num(i, 2)) {
                    return Some(k);
                }
            }
            _ => {}
        }
        i += l;
    }
    None
}

/// All `.xlsx` hrefs in a page, in document order, de-duplicated.
pub fn xlsx_links(html: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for part in html.split("href=\"").skip(1) {
        let Some(end) = part.find('"') else { continue };
        let href = &part[..end];
        if !href.to_lowercase().ends_with(".xlsx") {
            continue;
        }
        let url = if href.starts_with('/') {
            format!("https://www.bag.admin.ch{}", href)
        } else {
            href.to_string()
        };
        if !out.contains(&url) {
            out.push(url);
        }
    }
    out
}

/// The newest edition among `links`: max parsed date, document order breaks ties;
/// the first link if no date parses.
pub fn pick_latest(links: &[String]) -> Option<(String, Option<u32>)> {
    let mut best: Option<(usize, u32)> = None;
    for (idx, url) in links.iter().enumerate() {
        let name = url.rsplit('/').next().unwrap_or(url);
        if let Some(d) = edition_date(name) {
            if best.map_or(true, |(_, bd)| d > bd) {
                best = Some((idx, d));
            }
        }
    }
    match best {
        Some((idx, d)) => Some((links[idx].clone(), Some(d))),
        None => links.first().map(|u| (u.clone(), None)),
    }
}

fn get(client: &reqwest::blocking::Client, url: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    let r = client.get(url).send()?;
    if !r.status().is_success() {
        return Err(format!("HTTP {} for {}", r.status(), url).into());
    }
    Ok(r.bytes()?.to_vec())
}

/// Find the newest MiGeL XLSX on the BAG page, download it to `dest` and
/// say which one it was. See the module docs for the fallbacks.
pub fn download_latest(dest: &Path, log: &dyn Fn(&str)) -> Result<MigelSource, Box<dyn Error>> {
    let client = reqwest::blocking::Client::builder()
        .user_agent("Mozilla/5.0 (X11; Linux x86_64) swissdamed2sqlite")
        .timeout(std::time::Duration::from_secs(300))
        .build()?;

    let mut candidates: Vec<(String, String, &'static str)> = Vec::new();
    match get(&client, BAG_MIGEL_PAGE).map(|b| String::from_utf8_lossy(&b).into_owned()) {
        Ok(html) => {
            let links = xlsx_links(&html);
            match pick_latest(&links) {
                Some((url, date)) => {
                    let ed = date.map(format_key).unwrap_or_else(|| "unknown".into());
                    log(&format!("MiGeL: BAG page lists {} XLSX files, newest edition per {}", links.len(), ed));
                    candidates.push((url, ed, "bag-page"));
                }
                None => log("MiGeL: no XLSX link found on the BAG page"),
            }
        }
        Err(e) => log(&format!("MiGeL: BAG page not reachable ({e})")),
    }
    if candidates.first().map_or(true, |(u, _, _)| u != FALLBACK_URL) {
        candidates.push((FALLBACK_URL.to_string(), FALLBACK_EDITION.to_string(), "fallback-url"));
    }

    for (url, edition, how) in candidates {
        match get(&client, &url) {
            Ok(bytes) if bytes.starts_with(b"PK") => {
                std::fs::write(dest, &bytes)?;
                log(&format!("MiGeL XLSX per {} saved ({} bytes, {})", edition, bytes.len(), how));
                return Ok(MigelSource { url, edition, how });
            }
            Ok(bytes) => log(&format!("MiGeL: {} is not an XLSX ({} bytes), skipping", url, bytes.len())),
            Err(e) => log(&format!("MiGeL: download failed ({e})")),
        }
    }
    if dest.exists() {
        log(&format!("MiGeL: using the previously downloaded {} — check the BAG page", dest.display()));
        return Ok(MigelSource { url: String::new(), edition: "unknown".into(), how: "cached-file" });
    }
    Err("MiGeL XLSX: BAG page, fallback URL and cached file all unavailable".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edition_dates_from_real_bag_file_names() {
        let cases = [
            ("Mittel-%20und%20Gegenst%C3%A4ndeliste%20per%201.%20Juli%202026%20(in%20Excel-Format).xlsx", 20260701),
            ("Mittel-%20und%20Gegenst%C3%A4ndeliste%20per%2001.01.2026%20in%20Excel-Format.xlsx", 20260101),
            ("Mittel-%20und%20Gegenst%C3%A4ndeliste%20per%2001.07.2025%20%20in%20Excel-Format%2C%20korrigierte%20Version%20vom%203.%20September%202025.xlsx", 20250701),
            ("migel_gesamtliste_excel_korr20250204.xlsx", 20250204),
            ("gsamtliste-migel-excel-01072024.xlsx", 20240701),
            ("migel_gesamtliste_010722_excel.xlsx", 20220701),
            ("mittel-und-gegenstaendeliste-01-10-2018-excel.xlsx", 20181001),
            ("migel_gesamtliste_per01apr22_korrigierte_version_11may22.xlsx", 20220401),
            ("migel-1-7-2018-excelformat.xlsx", 20180701),
            ("mittel-und-gegenstaendeliste-vom-01.07.2021-in-excel-format.xlsx", 20210701),
        ];
        for (name, want) in cases {
            assert_eq!(edition_date(name), Some(want), "{name}");
        }
    }

    #[test]
    fn picks_newest_edition_not_document_order() {
        let links: Vec<String> = [
            "https://x/a/migel-01012020-excel.xlsx",
            "https://x/b/Mittel-%20und%20Gegenst%C3%A4ndeliste%20per%2001.01.2026%20in%20Excel-Format.xlsx",
            "https://x/c/Mittel-%20und%20Gegenst%C3%A4ndeliste%20per%201.%20Juli%202026%20(in%20Excel-Format).xlsx",
            "https://x/d/migel_gesamtliste_excel_korr20250204.xlsx",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let (url, d) = pick_latest(&links).unwrap();
        assert!(url.starts_with("https://x/c/"));
        assert_eq!(d, Some(20260701));
        assert_eq!(format_key(20260701), "01.07.2026");
    }

    #[test]
    fn links_are_collected_in_order_and_deduplicated() {
        let html = r#"<a href="/dam/x/a.xlsx">a</a><a href="https://y/b.pdf">b</a><a href="/dam/x/a.xlsx">a</a><a href="https://z/c.XLSX">c</a>"#;
        assert_eq!(xlsx_links(html), vec!["https://www.bag.admin.ch/dam/x/a.xlsx".to_string(), "https://z/c.XLSX".to_string()]);
    }
}
