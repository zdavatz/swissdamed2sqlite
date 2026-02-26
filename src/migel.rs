use calamine::{open_workbook, Reader, Xlsx};
use std::collections::HashMap;
use std::error::Error;

pub struct MigelItem {
    pub position_nr: String,
    pub bezeichnung: String,
    pub limitation: String,
    /// DE first-line keywords (used for primary scoring)
    pub keywords_de: Vec<String>,
    /// FR first-line keywords (used for primary scoring)
    pub keywords_fr: Vec<String>,
    /// IT first-line keywords (used for primary scoring)
    pub keywords_it: Vec<String>,
    /// DE bonus keywords from additional lines (>= 8 chars, counted toward match count)
    pub secondary_de: Vec<String>,
    /// FR bonus keywords from additional lines
    pub secondary_fr: Vec<String>,
    /// IT bonus keywords from additional lines
    pub secondary_it: Vec<String>,
    /// Union of all keywords (used for candidate index)
    pub all_keywords: Vec<String>,
}

const STOP_WORDS: &[&str] = &[
    // German articles, prepositions, conjunctions
    "der", "die", "das", "den", "dem", "des", "ein", "eine", "eines", "einem", "einen", "einer",
    "fuer", "mit", "von", "und", "oder", "bei", "auf", "nach", "ueber", "unter", "aus", "bis",
    "pro", "als", "inkl", "exkl", "max", "min", "per", "zur", "zum", "ins", "vom", "ohne",
    "auch", "sich", "noch", "wenn", "muss", "darf", "resp", "bzw",
    // German generic terms (too common in both MiGeL and products)
    "kauf", "miete", "tag", "jahr", "monate", "stueck", "set", "alle", "nur",
    "wird", "ist", "kann", "sind", "werden", "wurde", "hat", "haben",
    "steril", "unsteril", "sterile", "non", // too common across all medical products
    "diverse", "divers", "diversi",          // MiGeL catch-all qualifier
    "gross", "klein", "lang", "kurz",        // size/length descriptors
    "position", "definierte", "einstellbare", // MiGeL qualifiers
    // French
    "les", "des", "pour", "avec", "par", "une", "dans", "sur", "qui", "que",
    "achat", "location", "piece", "sans",
    // Italian
    "acquisto", "noleggio", "pezzo", "senza",
    // English
    "the", "for", "and", "with", "per",
    // Generic medical/product terms that match too broadly at word level
    "material", "produkt", "products", "product", "medical", "device",
    "system", "systeme", "systems", "geraet", "geraete", "appareil",
    // Cross-type medical terms (used for both screws/stockings/catheters/etc.)
    "compression", "compressione", "kompression",
    "verlaengerung", "extension", "estensione", "prolongation",
    "silikon", "silicone",
    // Generic surgical instrument terms (match across many unrelated instrument types)
    "ecarteur", "divaricatore", "retraktor",
];

/// Normalize German umlauts so ALL-CAPS text (e.g. ABSAUGGERAETE) matches
/// proper text (e.g. Absauggeräte).
pub fn normalize_german(text: &str) -> String {
    text.replace('ä', "ae")
        .replace('ö', "oe")
        .replace('ü', "ue")
        .replace('ß', "ss")
        .replace('Ä', "Ae")
        .replace('Ö', "Oe")
        .replace('Ü', "Ue")
        .replace('é', "e")
        .replace('è', "e")
        .replace('ê', "e")
        .replace('à', "a")
        .replace('â', "a")
        .replace('ù', "u")
        .replace('û', "u")
        .replace('ô', "o")
        .replace('î', "i")
        .replace('ç', "c")
}

/// Extract search keywords from first line of text (min 3 chars).
fn extract_keywords(text: &str) -> Vec<String> {
    let first_line = text.lines().next().unwrap_or(text);
    extract_keywords_from(first_line, 3)
}

/// Extract search keywords from ALL lines of text (min 3 chars).
fn extract_keywords_full(text: &str) -> Vec<String> {
    extract_keywords_from(text, 3)
}

/// Extract only long (>= 8 char) keywords from additional lines (not first line).
/// These are specific enough to use as bonus scoring keywords.
fn extract_secondary_keywords(text: &str) -> Vec<String> {
    let mut lines = text.lines();
    lines.next(); // skip first line
    let rest: String = lines.collect::<Vec<_>>().join(" ");
    if rest.trim().is_empty() {
        return Vec::new();
    }
    extract_keywords_from(&rest, 8)
}

/// Shared keyword extraction logic.
fn extract_keywords_from(text: &str, min_len: usize) -> Vec<String> {
    let normalized = normalize_german(text).to_lowercase();
    let mut keywords: Vec<String> = normalized
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| w.len() >= min_len)
        .filter(|w| !STOP_WORDS.contains(w))
        .map(|w| w.to_string())
        .collect();
    keywords.sort();
    keywords.dedup();
    keywords
}

/// Read a cell from a calamine row as a trimmed string.
fn cell_str(row: &[calamine::Data], idx: usize) -> String {
    row.get(idx)
        .map(|d| d.to_string())
        .unwrap_or_default()
        .trim()
        .to_string()
}

/// Parse all MiGeL items (rows with a Positions-Nr.) from the XLSX file.
/// Keeps per-language keywords separate for scoring, and builds a combined
/// keyword set for candidate finding.
pub fn parse_migel_items(path: &str) -> Result<Vec<MigelItem>, Box<dyn Error>> {
    let mut workbook: Xlsx<_> = open_workbook(path)?;
    let sheet_names: Vec<String> = workbook.sheet_names().to_vec();

    // --- Pass 1: Parse German sheet (index 0) ---
    let range_de = workbook.worksheet_range(&sheet_names[0])?;

    // Track category hierarchy descriptions (levels B through G = indices 1..7)
    let mut category_texts: Vec<String> = vec![String::new(); 7];
    let mut items: Vec<MigelItem> = Vec::new();

    for (row_idx, row) in range_de.rows().enumerate() {
        if row_idx == 0 {
            continue; // skip header
        }

        let pos_nr = cell_str(row, 7); // H = Positions-Nr.
        let bezeichnung = cell_str(row, 9); // J = Bezeichnung
        let limitation = cell_str(row, 10); // K = Limitation

        if pos_nr.is_empty() {
            // Category header row — update hierarchy
            for i in (1..7).rev() {
                let val = cell_str(row, i);
                if !val.is_empty() {
                    category_texts[i] =
                        bezeichnung.lines().next().unwrap_or("").trim().to_string();
                    for j in (i + 1)..7 {
                        category_texts[j] = String::new();
                    }
                    break;
                }
            }
        } else {
            // Item with position number
            let first_line = bezeichnung.lines().next().unwrap_or("").trim().to_string();

            // DE primary keywords: first line only (used for score ratio)
            let keywords_de = extract_keywords(&first_line);
            // DE secondary keywords: long keywords from additional lines (bonus matches)
            let secondary_de = extract_secondary_keywords(&bezeichnung);

            // All keywords: full Bezeichnung text (all lines) + Limitation text
            // for broader candidate finding via the inverted index.
            let mut all_kw = extract_keywords_full(&bezeichnung);
            if !limitation.is_empty() {
                let lim_kw = extract_keywords_full(&limitation);
                all_kw.extend(lim_kw);
                all_kw.sort();
                all_kw.dedup();
            }

            items.push(MigelItem {
                position_nr: pos_nr,
                bezeichnung: first_line,
                limitation,
                keywords_de,
                keywords_fr: Vec::new(),
                keywords_it: Vec::new(),
                secondary_de,
                secondary_fr: Vec::new(),
                secondary_it: Vec::new(),
                all_keywords: all_kw,
            });
        }
    }

    // --- Pass 2: Parse French and Italian sheets for per-language keywords ---
    let pos_map: HashMap<String, usize> = items
        .iter()
        .enumerate()
        .map(|(i, item)| (item.position_nr.clone(), i))
        .collect();

    for sheet_idx in 1..sheet_names.len().min(3) {
        let range = workbook.worksheet_range(&sheet_names[sheet_idx])?;
        for (row_idx, row) in range.rows().enumerate() {
            if row_idx == 0 {
                continue;
            }
            let pos_nr = cell_str(row, 7);
            if let Some(&item_idx) = pos_map.get(&pos_nr) {
                let bezeichnung = cell_str(row, 9);
                let limitation = cell_str(row, 10);
                // Primary scoring keywords: first line only
                let kw = extract_keywords(&bezeichnung);
                // Secondary keywords: long keywords from additional lines
                let secondary = extract_secondary_keywords(&bezeichnung);
                match sheet_idx {
                    1 => {
                        items[item_idx].keywords_fr = kw.clone();
                        items[item_idx].secondary_fr = secondary;
                    }
                    2 => {
                        items[item_idx].keywords_it = kw.clone();
                        items[item_idx].secondary_it = secondary;
                    }
                    _ => {}
                }
                // Candidate index: full text + limitation
                let full_kw = extract_keywords_full(&bezeichnung);
                items[item_idx].all_keywords.extend(full_kw);
                if !limitation.is_empty() {
                    let lim_kw = extract_keywords_full(&limitation);
                    items[item_idx].all_keywords.extend(lim_kw);
                }
            }
        }
    }

    // Deduplicate all_keywords per item
    for item in &mut items {
        item.all_keywords.sort();
        item.all_keywords.dedup();
    }

    Ok(items)
}

/// Build an inverted index: keyword → list of MigelItem indices.
/// Uses all_keywords (DE+FR+IT) for broad candidate finding.
pub fn build_keyword_index(items: &[MigelItem]) -> HashMap<String, Vec<usize>> {
    let mut index: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, item) in items.iter().enumerate() {
        for kw in &item.all_keywords {
            index.entry(kw.clone()).or_default().push(i);
        }
    }
    index
}

/// Split text into words (split on non-alphanumeric characters).
fn split_words(text: &str) -> Vec<&str> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|w| !w.is_empty())
        .collect()
}

/// Check if a keyword matches in the text at word level.
/// - `suffix`: if true, also matches as a suffix of a compound word
///   (e.g., "katheter" in "verweilkatheter"). Only for German.
/// - `fuzzy`: if true, also tries keyword truncated by 1 char (German plural/case).
///   Only for German.
/// FR/IT should use suffix=false, fuzzy=false to prevent cross-type matches
/// (e.g., "prothese" in "endoprothese" matching eye prosthesis).
fn word_match(text_words: &[&str], keyword: &str, suffix: bool, fuzzy: bool) -> bool {
    for word in text_words {
        // Exact word match
        if *word == keyword {
            return true;
        }
        // Suffix match in German compound words (keyword must be head of compound)
        if suffix && word.len() > keyword.len() + 2 && word.ends_with(keyword) {
            return true;
        }
    }
    if fuzzy && keyword.len() >= 7 {
        let trunc = &keyword[..keyword.len() - 1];
        for word in text_words {
            if *word == trunc {
                return true;
            }
            if suffix && word.len() > trunc.len() + 2 && word.ends_with(trunc) {
                return true;
            }
        }
    }
    false
}

/// Check if keyword matches anywhere in text as a substring (for candidate pre-filter).
/// Uses fuzzy suffix matching for keywords >= 7 chars.
fn fuzzy_contains(haystack: &str, keyword: &str) -> bool {
    if haystack.contains(keyword) {
        return true;
    }
    if keyword.len() >= 7 {
        let trunc = &keyword[..keyword.len() - 1];
        if haystack.contains(trunc) {
            return true;
        }
    }
    false
}

/// Compute keyword overlap score using word-level matching.
/// Returns (score, max_matched_keyword_len, matched_count).
/// `suffix`: allow compound word suffix matching (German only)
/// `fuzzy`: allow truncated keyword matching (German only)
fn keyword_score(text_words: &[&str], keywords: &[String], suffix: bool, fuzzy: bool) -> (f64, usize, usize) {
    let total: f64 = keywords.iter().map(|k| k.len() as f64).sum();
    if total == 0.0 {
        return (0.0, 0, 0);
    }
    let mut matched_weight = 0.0;
    let mut max_matched_len = 0;
    let mut matched_count = 0;
    for kw in keywords {
        if word_match(text_words, kw, suffix, fuzzy) {
            matched_weight += kw.len() as f64;
            matched_count += 1;
            if kw.len() > max_matched_len {
                max_matched_len = kw.len();
            }
        }
    }
    (matched_weight / total, max_matched_len, matched_count)
}

/// Find the best-matching MiGeL item for a product.
/// CRITICAL: Each language's keywords are scored ONLY against the same language's
/// product description. This prevents cross-language false positives (e.g.,
/// French "pression" matching inside German "Kompressionsschraube").
pub fn find_best_migel_match<'a>(
    desc_de: &str,
    desc_fr: &str,
    desc_it: &str,
    brand: &str,
    migel_items: &'a [MigelItem],
    keyword_index: &HashMap<String, Vec<usize>>,
) -> Option<&'a MigelItem> {
    let de_lower = normalize_german(&format!("{} {}", desc_de, brand)).to_lowercase();
    let fr_lower = normalize_german(&format!("{} {}", desc_fr, brand)).to_lowercase();
    let it_lower = normalize_german(&format!("{} {}", desc_it, brand)).to_lowercase();
    // Combined text only for candidate finding (broad pre-filter)
    let combined = format!("{} {} {}", de_lower, fr_lower, it_lower);

    // Pre-split text into words for word-level matching in scoring
    let de_words = split_words(&de_lower);
    let fr_words = split_words(&fr_lower);
    let it_words = split_words(&it_lower);

    // Step 1: Find candidate items via the broad keyword index (substring matching OK here)
    let mut candidates: HashMap<usize, bool> = HashMap::new();
    for (keyword, indices) in keyword_index {
        if fuzzy_contains(&combined, keyword) {
            for &idx in indices {
                candidates.insert(idx, true);
            }
        }
    }

    // Step 2: Score each candidate using WORD-LEVEL matching against per-language text
    // DE uses fuzzy word matching (handles German plural/case: Orthese/Orthesen)
    // FR/IT use exact word matching only
    // Secondary keywords from additional lines count as bonus matches
    candidates
        .keys()
        .filter_map(|&idx| {
            let item = &migel_items[idx];
            // Primary scores (first-line keywords)
            let (score_de, max_len_de, count_de) = keyword_score(&de_words, &item.keywords_de, true, true);
            let (score_fr, max_len_fr, count_fr) = keyword_score(&fr_words, &item.keywords_fr, false, false);
            let (score_it, max_len_it, count_it) = keyword_score(&it_words, &item.keywords_it, false, false);

            // Secondary bonus matches: only count if at least 1 primary keyword matched
            // This prevents secondary-only matches (e.g., "Verlängerung" from MiGeL line 2
            // matching unrelated products that happen to have "Verlängerung")
            let (_, sec_max_de, sec_count_de) = if count_de > 0 {
                keyword_score(&de_words, &item.secondary_de, true, true)
            } else {
                (0.0, 0, 0)
            };
            let (_, sec_max_fr, sec_count_fr) = if count_fr > 0 {
                keyword_score(&fr_words, &item.secondary_fr, false, false)
            } else {
                (0.0, 0, 0)
            };
            let (_, sec_max_it, sec_count_it) = if count_it > 0 {
                keyword_score(&it_words, &item.secondary_it, false, false)
            } else {
                (0.0, 0, 0)
            };

            // Total count = primary + secondary bonus
            let total_de = count_de + sec_count_de;
            let total_fr = count_fr + sec_count_fr;
            let total_it = count_it + sec_count_it;
            let max_de = max_len_de.max(sec_max_de);
            let max_fr = max_len_fr.max(sec_max_fr);
            let max_it = max_len_it.max(sec_max_it);

            // Pick the best-scoring language (by primary score, using total count for threshold)
            let (best_score, best_max_len, best_count) = [
                (score_de, max_de, total_de),
                (score_fr, max_fr, total_fr),
                (score_it, max_it, total_it),
            ]
                .iter()
                .copied()
                .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal))
                .unwrap_or((0.0, 0, 0));

            // Match criteria:
            // - 2+ matched keywords (primary+secondary): score >= 0.3, max keyword len >= 6
            // - 1 matched keyword: score >= 0.5, keyword len >= 10
            let passes = if best_count >= 2 {
                best_score >= 0.3 && best_max_len >= 6
            } else {
                best_score >= 0.5 && best_max_len >= 10
            };

            if passes {
                Some((idx, best_score, best_max_len))
            } else {
                None
            }
        })
        .max_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.2.cmp(&b.2))
        })
        .map(|(idx, _, _)| &migel_items[idx])
}
