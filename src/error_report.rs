use chrono::Local;
use std::collections::HashSet;
use std::fs;

/// An invalid SRN entry with context about the manufacturer and mandate holder.
pub struct InvalidSrn {
    pub srn: String,
    pub manufacturer: String,
    pub mandate_type: String,
    pub mandate_holder_chrn: String,
    pub mandate_holder_name: String,
    pub mandate_holder_uid: String,
}

/// Validate SRN format: must be XX-MF-NNNNNN or XX-PR-NNNNNN
/// where XX is a 2-3 letter country code, followed by -MF- or -PR-, then 6+ digits.
/// Also accepts minor variants: underscores, unicode dashes, missing dash before digits.
pub fn is_valid_srn(srn: &str) -> bool {
    // Normalize: replace underscores and unicode dashes with ASCII dash
    let normalized: String = srn.chars().map(|c| match c {
        '_' | '\u{2010}' | '\u{2011}' | '\u{2012}' | '\u{2013}' | '\u{2014}' | '\u{2015}' => '-',
        _ => c,
    }).collect();
    let upper = normalized.to_uppercase();

    // Must contain -MF- or -PR- (or MF/PR without trailing dash for formats like DE-MF000005277)
    let has_mf = upper.contains("-MF-") || upper.contains("-MF0");
    let has_pr = upper.contains("-PR-") || upper.contains("-PR0");
    if !has_mf && !has_pr {
        return false;
    }

    // Must start with 2-3 letter country code
    let first_dash = match upper.find('-') {
        Some(pos) => pos,
        None => return false,
    };
    if first_dash < 2 || first_dash > 3 {
        return false;
    }
    if !upper[..first_dash].chars().all(|c| c.is_ascii_alphabetic()) {
        return false;
    }

    // Must end with at least 6 digits (possibly with dashes)
    let digits: String = upper.chars().rev().take_while(|c| c.is_ascii_digit() || *c == '-').collect();
    let digit_count = digits.chars().filter(|c| c.is_ascii_digit()).count();
    if digit_count < 6 {
        return false;
    }

    // Reject -AR- and -IM- (not manufacturer SRNs)
    if upper.contains("-AR-") || upper.contains("-IM-") {
        return false;
    }

    true
}

/// Write an HTML error report for invalid SRNs to `html/srn_error_report_HHhMM.dd.mm.yyyy.html`.
/// Returns the path to the written file, or None if there are no invalid SRNs.
pub fn write_srn_error_report(invalid_srns: &[InvalidSrn]) -> Option<String> {
    if invalid_srns.is_empty() {
        return None;
    }

    let html_dir = crate::app_data_dir().join("html");
    fs::create_dir_all(&html_dir).ok();
    let timestamp = Local::now().format("%Hh%M.%d.%m.%Y").to_string();
    let html_path = html_dir
        .join(format!("srn_error_report_{}.html", timestamp))
        .to_string_lossy()
        .to_string();

    // Deduplicate by SRN
    let mut seen = HashSet::new();
    let mut unique: Vec<&InvalidSrn> = Vec::new();
    for entry in invalid_srns {
        if seen.insert(&entry.srn) {
            unique.push(entry);
        }
    }
    unique.sort_by(|a, b| a.srn.cmp(&b.srn));

    let mut html = String::from(
        "<!DOCTYPE html>\n<html><head><meta charset=\"UTF-8\">\n\
         <title>SRN Error Report</title>\n\
         <style>\n\
         body { font-family: Arial, sans-serif; margin: 20px; }\n\
         h1 { color: #c0392b; }\n\
         table { border-collapse: collapse; width: 100%; }\n\
         th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }\n\
         th { background: #2c3e50; color: white; }\n\
         tr:nth-child(even) { background: #f2f2f2; }\n\
         .srn { font-family: monospace; color: #c0392b; font-weight: bold; }\n\
         </style></head><body>\n"
    );
    html.push_str(&format!(
        "<h1>SRN Error Report — {}</h1>\n",
        Local::now().format("%d.%m.%Y")
    ));
    html.push_str(&format!(
        "<p>{} invalid SRNs found in swissdamed.ch data (filtered from unique SRNs export).</p>\n",
        unique.len()
    ));
    html.push_str(
        "<table>\n<tr>\
         <th>#</th>\
         <th>Invalid SRN</th>\
         <th>Manufacturer</th>\
         <th>Type</th>\
         <th>Mandate Holder CHRN</th>\
         <th>Mandate Holder</th>\
         <th>Mandate Holder UID</th>\
         </tr>\n"
    );
    for (i, entry) in unique.iter().enumerate() {
        html.push_str(&format!(
            "<tr><td>{}</td><td class=\"srn\">{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>\n",
            i + 1,
            entry.srn,
            entry.manufacturer,
            entry.mandate_type,
            entry.mandate_holder_chrn,
            entry.mandate_holder_name,
            entry.mandate_holder_uid,
        ));
    }
    html.push_str("</table>\n</body></html>\n");

    if let Err(e) = fs::write(&html_path, &html) {
        eprintln!("Error writing HTML report: {}", e);
        return None;
    }

    eprintln!("Error report written: {}", html_path);
    Some(html_path)
}
