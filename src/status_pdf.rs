//! `--status-pdf` mode: render a one-page A4 status sheet (plus a nomenclature
//! legend page with clickable source links) for the triage classifier, from the
//! latest `udi_details` DB.
//!
//! Pure Rust, no Chrome: rendered with [`genpdf`] (which writes via `printpdf`)
//! and the DejaVu Sans family embedded in the binary (`assets/fonts/`) — same
//! approach as chrome2linkedin's report generator, so the single-binary property
//! is preserved. genpdf 0.2 has no native hyperlinks, so [`add_links`] overlays
//! `Link` annotations onto the URL lines afterwards via `lopdf` (the URL lines
//! are the only text set in [`LINK_FONT_SIZE`], which lets the post-process find
//! them unambiguously and map them, in order, to [`LEGEND`]'s URLs).
//!
//! Reads the live distribution (intendedUser × iuConfidence counts) from the
//! newest `udi_details_*.db` and writes `pdf/swissdamed_triage_status_DD.MM.YYYY.pdf`.

use genpdf::elements::{Break, PageBreak, Paragraph};
use genpdf::style::{Color, Style};
use genpdf::{Alignment, Element};
use rusqlite::Connection;
use std::error::Error;
use std::path::Path;

// Palette (matches the chrome2linkedin report house style).
const INK: Color = Color::Rgb(0x1b, 0x1b, 0x1d);
const GOLD: Color = Color::Rgb(0xa0, 0x8b, 0x6a);
const SLATE: Color = Color::Rgb(0x3a, 0x3d, 0x44);
const MUTED: Color = Color::Rgb(0x8a, 0x8d, 0x94);
const LINK: Color = Color::Rgb(0x2c, 0x5a, 0x8a);

// URL lines are the ONLY text set at this size; `add_links` keys on it. Do not
// reuse this size anywhere else in the document.
const LINK_FONT_SIZE: u8 = 8;
const A4_WIDTH_PT: f64 = 595.276;
const MARGIN_MM: f64 = 20.0;
const MARGIN_PT: f64 = MARGIN_MM * 72.0 / 25.4;
const AVG_ADVANCE_EM: f64 = 0.55; // mean DejaVu Sans advance for lowercase, in em
const MAX_LINK_CHARS: usize = 92;

// DejaVu Sans embedded in the binary. The sheet uses no italics, so the italic
// slots reuse the upright faces (genpdf's FontFamily requires all four).
const FONT_REGULAR: &[u8] =
    include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/assets/fonts/DejaVuSans.ttf"));
const FONT_BOLD: &[u8] =
    include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/assets/fonts/DejaVuSans-Bold.ttf"));

/// Nomenclature / abbreviation legend: (term, explanation, optional source URL).
/// Entries with a URL emit one [`LINK_FONT_SIZE`] line; `add_links` maps those
/// lines, in this order, to these URLs — so the order here is the source of truth.
const LEGEND: &[(&str, &str, Option<&str>)] = &[
    ("EMDN", "European Medical Device Nomenclature — strukturierte Geräte-Kategorie (Buchstabe = Kategorie), dient hier als Proxy für die Zweckbestimmung (~100% Abdeckung).", Some("https://webgate.ec.europa.eu/dyna2/emdn/")),
    ("EMDN — prof. Kategorien", "L Chirurgie-Instrumente · K minimalinvasiv/Elektrochirurgie · C kardiozirkulatorisch · G Endoskopie · H Nahtmaterial · J aktive Implantate · S Sterilisation · B Bluttransfusion/Hämatologie · D MP-Desinfektion · P Implantat-Prothetik/Osteosynthese. Chirurgisch/interventionell → als «professional»-Ausschluss verwendet.", Some("https://webgate.ec.europa.eu/dyna2/emdn/")),
    ("EMDN — Konsum-nah", "M Verbandmaterial · Y technische Hilfen für Behinderte (Orthesen) · T Inkontinenz-/Schutzhilfen · A Applikation/Entnahme/Sammlung (Stoma) · N Neuro/Muskel (TENS). Für die Klasse-IIa-Abgabevermutung genutzt.", Some("https://webgate.ec.europa.eu/dyna2/emdn/")),
    ("MDR", "Medical Device Regulation (EU) 2017/745 — EU-Medizinprodukteverordnung.", Some("https://eur-lex.europa.eu/eli/reg/2017/745/oj")),
    ("MDR-Risikoklassen", "Anhang VIII: I (gering, meist nicht-invasiv) · IIa · IIb · III (höchstes Risiko). Rein risikobasiert — kein Anwender-Signal (sagt nichts über Laie/Fachperson).", Some("https://eur-lex.europa.eu/eli/reg/2017/745/oj#d1e32-92-1")),
    ("IVDR", "In-vitro Diagnostic Regulation (EU) 2017/746 — trägt die Felder Selbst-/patientennahe/Profi-Testung.", Some("https://eur-lex.europa.eu/eli/reg/2017/746/oj")),
    ("IVDR-Risikoklassen", "Anhang VIII: A (gering) · B · C · D (höchstes Risiko). Selbsttests spannen B–C — Risikoklasse ≠ Anwender.", Some("https://eur-lex.europa.eu/eli/reg/2017/746/oj#d1e32-176-1")),
    ("AIMDD", "Active Implantable Medical Devices Directive 90/385/EWG — aktive Implantate (Alt-Recht), stets professionell.", Some("https://eur-lex.europa.eu/eli/dir/1990/385/oj")),
    ("MDD", "Medical Device Directive 93/42/EWG — Alt-Recht-Medizinprodukte (Legacy-Bestand vor MDR).", Some("https://eur-lex.europa.eu/eli/dir/1993/42/oj")),
    ("EUDAMED", "European Database on Medical Devices — EU-Datenbank; swissdamed spiegelt ihren Datenbestand.", Some("https://ec.europa.eu/tools/eudamed")),
    ("swissdamed", "Schweizer UDI-/Medizinprodukte-Datenbank (Swissmedic) — Quelle dieses Korpus.", Some("https://swissdamed.ch")),
    ("MiGeL", "Mittel- und Gegenständeliste (BAG) — von der OKP vergütete Mittel; Ausgabe per 01.01.2026.", Some("https://www.bag.admin.ch/de/mittel-und-gegenstaendeliste-migel")),
    ("KLV Art. 20", "Krankenpflege-Leistungsverordnung (SR 832.112.31) — MiGeL = Mittel zur Selbstanwendung (Laienanwendung).", Some("https://www.fedlex.admin.ch/eli/cc/1995/4964_4964_4964/de")),
    ("MepV", "Medizinprodukteverordnung (SR 812.213) — Schweizer Abgabe-/Inverkehrbringen-Regeln für Medizinprodukte.", Some("https://www.fedlex.admin.ch/eli/cc/2020/552/de")),
    ("UDI / udiDi", "Unique Device Identification — udiDi ist die marktspezifische Geräte-ID (nicht die GTIN/udiDiCode).", None),
    ("SPP", "System or Procedure Pack (MDR Art. 22) — gebündelte Zusammenstellung; einzige Trägerin des EUDAMED-Feldes medicalPurpose.", None),
    ("IFU", "Instructions for Use / Gebrauchsanweisung — enthält die rechtsverbindliche Zweckbestimmung inkl. Anwenderkreis (nicht in EUDAMED).", None),
];

fn sep(n: i64) -> String {
    let s = n.abs().to_string();
    let b = s.as_bytes();
    let len = b.len();
    let mut out = String::new();
    for (i, ch) in b.iter().enumerate() {
        if i > 0 && (len - i) % 3 == 0 {
            out.push('\u{2019}'); // Swiss apostrophe thousands separator
        }
        out.push(*ch as char);
    }
    out
}

fn pct(n: i64, total: i64) -> String {
    if total == 0 {
        return "0.0%".into();
    }
    format!("{:.1}%", 100.0 * n as f64 / total as f64)
}

/// Display form of a URL: drop scheme + `www.`, elide an over-long path in the
/// middle. The annotation always targets the full address.
fn link_text(url: &str) -> String {
    let s = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .unwrap_or(url);
    let s = s.strip_prefix("www.").unwrap_or(s);
    if s.chars().count() <= MAX_LINK_CHARS {
        return s.to_string();
    }
    let (host, rest) = match s.find('/') {
        Some(i) => s.split_at(i),
        None => (s, ""),
    };
    let room = MAX_LINK_CHARS.saturating_sub(host.chars().count() + 2);
    let rest: Vec<char> = rest.chars().collect();
    let cut = rest.len().saturating_sub(room);
    let tail: String = rest[cut..].iter().collect();
    format!("{host}/\u{2026}{tail}")
}

// --- genpdf helpers ---

fn push_lines(doc: &mut genpdf::Document, text: &str, style: Style, align: Alignment) {
    for line in text.split('\n') {
        let mut p = Paragraph::default();
        p.push_styled(line.to_string(), style);
        doc.push(p.aligned(align).styled(style));
    }
}

fn h1(doc: &mut genpdf::Document, kicker: &str, titel: &str) {
    push_lines(doc, kicker, Style::new().with_color(GOLD).with_font_size(9).bold(), Alignment::Left);
    doc.push(Break::new(0.25));
    push_lines(doc, titel, Style::new().with_color(SLATE).with_font_size(17).bold(), Alignment::Left);
    doc.push(Break::new(0.5));
}

fn h2(doc: &mut genpdf::Document, titel: &str) {
    doc.push(Break::new(0.5));
    push_lines(doc, titel, Style::new().with_color(SLATE).with_font_size(12).bold(), Alignment::Left);
    doc.push(Break::new(0.3));
}

fn meta(doc: &mut genpdf::Document, text: &str) {
    push_lines(doc, text, Style::new().with_color(MUTED).with_font_size(9), Alignment::Left);
}

/// One metric line: bold gold label + bold ink value, then a muted basis sub-line.
fn metric(doc: &mut genpdf::Document, label: &str, value: &str, basis: &str) {
    let mut p = Paragraph::default();
    p.push_styled(format!("{label}    "), Style::new().with_color(GOLD).with_font_size(10).bold());
    p.push_styled(value.to_string(), Style::new().with_color(INK).with_font_size(10).bold());
    doc.push(p);
    if !basis.is_empty() {
        let s = Style::new().with_color(MUTED).with_font_size(9);
        let mut b = Paragraph::default();
        b.push_styled(basis.to_string(), s);
        doc.push(b.styled(s));
    }
    doc.push(Break::new(0.3));
}

/// Bulleted body line (style set on the element too, so wrapped lines get the
/// right line height).
fn bullet(doc: &mut genpdf::Document, text: &str) {
    let s = Style::new().with_color(INK).with_font_size(9);
    let mut p = Paragraph::default();
    p.push_styled("\u{2022}  ".to_string(), Style::new().with_color(GOLD).with_font_size(9).bold());
    p.push_styled(text.to_string(), s);
    doc.push(p.styled(s));
    doc.push(Break::new(0.2));
}

/// Legend entry: bold term + explanation on one line, then the source URL on a
/// dedicated [`LINK_FONT_SIZE`] line (the sentinel `add_links` overlays).
fn legend_entry(doc: &mut genpdf::Document, term: &str, expl: &str, url: Option<&str>) {
    let s = Style::new().with_color(INK).with_font_size(9);
    let mut p = Paragraph::default();
    p.push_styled(format!("{term}   "), Style::new().with_color(SLATE).with_font_size(9).bold());
    p.push_styled(expl.to_string(), s);
    doc.push(p.styled(s));
    if let Some(u) = url {
        let ls = Style::new().with_color(LINK).with_font_size(LINK_FONT_SIZE);
        let mut lp = Paragraph::default();
        lp.push_styled(link_text(u), ls);
        doc.push(lp.styled(ls));
    }
    doc.push(Break::new(0.12));
}

fn font_family() -> Result<genpdf::fonts::FontFamily<genpdf::fonts::FontData>, Box<dyn Error>> {
    let reg = genpdf::fonts::FontData::new(FONT_REGULAR.to_vec(), None)
        .map_err(|e| format!("embedded regular font: {}", e))?;
    let bold = genpdf::fonts::FontData::new(FONT_BOLD.to_vec(), None)
        .map_err(|e| format!("embedded bold font: {}", e))?;
    Ok(genpdf::fonts::FontFamily {
        italic: reg.clone(),
        bold_italic: bold.clone(),
        regular: reg,
        bold,
    })
}

struct Dist {
    prof: i64,
    ph: i64,
    pm: i64,
    pl: i64,
    rev: i64,
}

fn read_dist(db_path: &Path) -> Result<Dist, Box<dyn Error>> {
    let conn = Connection::open(db_path)?;
    let mut stmt =
        conn.prepare("SELECT intendedUser, iuConfidence, COUNT(*) FROM udi_details GROUP BY 1,2")?;
    let rows = stmt.query_map([], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, i64>(2)?))
    })?;
    let mut d = Dist { prof: 0, ph: 0, pm: 0, pl: 0, rev: 0 };
    for row in rows {
        let (u, c, n) = row?;
        match (u.as_str(), c.as_str()) {
            ("professional", _) => d.prof += n,
            ("public", "high") => d.ph += n,
            ("public", "medium") => d.pm += n,
            ("public", "low") => d.pl += n,
            ("review", _) => d.rev += n,
            _ => {}
        }
    }
    Ok(d)
}

fn build(d: &Dist, dbname: &str) -> Result<genpdf::Document, Box<dyn Error>> {
    let ptot = d.ph + d.pm + d.pl;
    let total = d.prof + ptot + d.rev;

    let mut doc = genpdf::Document::new(font_family()?);
    doc.set_title("swissdamed Triage — Status");
    doc.set_minimal_conformance();
    doc.set_font_size(10);
    doc.set_line_spacing(1.3);
    let mut deco = genpdf::SimplePageDecorator::new();
    deco.set_margins(MARGIN_MM as u8);
    doc.set_page_decorator(deco);

    // --- Page 1: status ---
    h1(&mut doc, "SWISSDAMED TRIAGE", "Anwenderbestimmung & Shop-Verkäuflichkeit");
    meta(
        &mut doc,
        &format!(
            "Entscheidungshilfe, keine Konformitätsbestimmung.  Stand {}  ·  Quelle {}  ·  {} Geräte-Datensätze (udiDis)",
            chrono::Local::now().format("%d.%m.%Y"),
            dbname,
            sep(total)
        ),
    );

    h2(&mut doc, "Verteilung");
    metric(&mut doc, "Professional", &format!("{}   ({})", sep(d.prof), pct(d.prof, total)),
        "Implantat / Klasse III · IVD-Profitesting · chirurgische EMDN L/K/C/G/H/J/S/B/D/P");
    metric(&mut doc, "Public — hoch", &format!("{}   ({})", sep(d.ph), pct(d.ph, total)),
        "IVD-Selbsttestung (strukturiertes Feld, echtes Intended-User-Signal)");
    metric(&mut doc, "Public — mittel", &format!("{}   ({})", sep(d.pm), pct(d.pm, total)),
        "MiGeL-Listung / KLV Art. 20 Selbstanwendung (Laienanwendung)");
    metric(&mut doc, "Public — tief", &format!("{}   ({})", sep(d.pl), pct(d.pl, total)),
        "MepV-Abgabevermutung: Klasse I / IIa, keine Abgabebeschränkung gefunden — IFU prüfen");
    metric(&mut doc, "Public — total", &format!("{}   ({})", sep(ptot), pct(ptot, total)), "");
    metric(&mut doc, "Review", &format!("{}   ({})", sep(d.rev), pct(d.rev, total)),
        "Höheres Risiko: IIa ausserhalb Konsum-EMDN, IIb, IVD, System-/Prozedurpacks");

    h2(&mut doc, "Methode — zwei Fragen, getrennt nach Konfidenz");
    bullet(&mut doc, "Intended-User (streng, Haftungsseite: für wen ist das Gerät bestimmt): «public» nur aus einem strukturierten IVD-Selbsttest-Feld (hoch), MiGeL-/KLV-Art.-20-Listung (mittel) oder einer expliziten Laienanwendungs-Aussage. Ein falsches «public» = Verkauf eines rein professionellen Geräts = Verstoss; ein falsches «professional» = nur ein entgangener Verkauf.");
    bullet(&mut doc, "Shop-Verkäuflichkeit (MepV-Grundsatz: an das Publikum abgebbar, sofern nicht beschränkt): public/tief = kuratierte Konsum-EMDN-Liste (Antidekubitus-Matratzen) plus jedes risikoarme Gerät nach den Profi-Gattern — Klasse I breit, Klasse IIa für Konsum-EMDN (Verband, Hilfsmittel, Inkontinenz, Stoma, TENS). Eine «keine-Beschränkung-gefunden»-Vermutung: stets die IFU prüfen.");
    bullet(&mut doc, "Professionell ausgeschlossen: AIMDD / implantierbar / Klasse III–D, IVD-Profi- & patientennahe Testung sowie chirurgisch-interventionelle EMDN-Kategorien L/K/C/G/H/J/S/B/D/P.");

    h2(&mut doc, "Kernbefunde");
    bullet(&mut doc, "swissdamed / EUDAMED führen KEIN Intended-User-Feld für MDR oder IVD (das EUDAMED-Feld medicalPurpose ist nur bei System-/Prozedurpacks befüllt) — durch Regulatory-Review bestätigt. Das einzige strukturierte Signal ist die IVD-Trias Selbst-/patientennahe/Profi-Testung, die die Testmodalität beschreibt, nicht den Anwender.");
    bullet(&mut doc, "Freitext-Laienphrasen (laien, selbstanwendung, home use, …) treffen unter 100 des gesamten Korpus — Text-Mining ist für diese Frage eine Sackgasse.");
    bullet(&mut doc, "MiGeL (KLV Art. 20) ist das stärkste ableitbare Laienanwendungs-Signal, aber eine Vergütungsliste ist schmaler als «an Konsumenten verkäuflich»: die offizielle BAG-MiGeL per 01.01.2026 (884 Positionen) enthält null Matratzen-Positionen — Konsumgüter wie Antidekubitus-Matratzen brauchen daher die kuratierte Liste, nicht MiGeL.");
    bullet(&mut doc, "Wie der Review-Anteil von 58 % auf 15.3 % fiel: drei positive Signale ordnen Geräte aus «review» neu als «public» ein — MiGeL-/KLV-Art.-20-Listung (Laienanwendung), die kuratierte Konsum-EMDN-Liste (Antidekubitus) und die MepV-Abgabevermutung für risikoarme Klasse-I/IIa-Geräte; im «review» bleibt nur der höher-riskante Rest.");

    // --- Page 2: nomenclature legend ---
    doc.push(PageBreak::new());
    h2(&mut doc, "Legende — Nomenklatur & Quellen");
    meta(&mut doc, "Kürzel dieses Berichts mit Links zu den amtlichen Erklärungen (klickbar).");
    doc.push(Break::new(0.25));
    for (term, expl, url) in LEGEND {
        legend_entry(&mut doc, term, expl, *url);
    }

    doc.push(Break::new(0.6));
    push_lines(
        &mut doc,
        "Erzeugt von swissdamed2sqlite (--status-pdf) · jedes gelistete Produkt ist vor der Aufnahme gegen die Hersteller-IFU/Zweckbestimmung + MepV-Abgaberegeln zu prüfen.",
        Style::new().with_color(MUTED).with_font_size(7),
        Alignment::Left,
    );
    Ok(doc)
}

/// Overlay a clickable `Link` annotation onto each [`LINK_FONT_SIZE`] URL line,
/// mapping them in document order to `urls`. Returns how many were placed.
fn add_links(pdf: &Path, urls: &[&str]) -> Result<usize, Box<dyn Error>> {
    use lopdf::{Dictionary, Document, Object, StringFormat};

    let mut doc = Document::load(pdf)?;
    let pages: Vec<(u32, lopdf::ObjectId)> = doc.get_pages().into_iter().collect();

    let num = |o: &Object| -> Option<f64> {
        match o {
            Object::Real(r) => Some(*r as f64),
            Object::Integer(i) => Some(*i as f64),
            _ => None,
        }
    };

    let mut placed = 0usize;
    for (_, page_id) in pages {
        let content = doc.get_and_decode_page_content(page_id)?;
        let mut pos = (0.0f64, 0.0f64);
        let mut size = 0.0f64;
        let mut origins: Vec<(f64, f64)> = Vec::new();
        for op in &content.operations {
            match op.operator.as_str() {
                "Td" | "TD" if op.operands.len() >= 2 => {
                    if let (Some(x), Some(y)) = (num(&op.operands[0]), num(&op.operands[1])) {
                        pos = (x, y);
                    }
                }
                "Tm" if op.operands.len() >= 6 => {
                    if let (Some(x), Some(y)) = (num(&op.operands[4]), num(&op.operands[5])) {
                        pos = (x, y);
                    }
                }
                "Tf" if op.operands.len() >= 2 => {
                    if let Some(s) = num(&op.operands[1]) {
                        size = s;
                    }
                }
                "Tj" | "TJ" => {
                    if (size - LINK_FONT_SIZE as f64).abs() < 0.01 && origins.last() != Some(&pos) {
                        origins.push(pos);
                    }
                }
                _ => {}
            }
        }
        if origins.is_empty() {
            continue;
        }

        let mut annots: Vec<Object> = Vec::new();
        for (x, y) in &origins {
            let Some(url) = urls.get(placed) else { break };
            placed += 1;
            let width =
                (link_text(url).chars().count() as f64) * LINK_FONT_SIZE as f64 * AVG_ADVANCE_EM;
            let right = (x + width + 2.0).min(A4_WIDTH_PT - MARGIN_PT);

            let mut action = Dictionary::new();
            action.set("S", Object::Name(b"URI".to_vec()));
            action.set("URI", Object::String(url.as_bytes().to_vec(), StringFormat::Literal));

            let mut annot = Dictionary::new();
            annot.set("Type", Object::Name(b"Annot".to_vec()));
            annot.set("Subtype", Object::Name(b"Link".to_vec()));
            annot.set(
                "Rect",
                Object::Array(vec![
                    Object::Real((*x - 2.0) as f32),
                    Object::Real((*y - 2.0) as f32),
                    Object::Real(right as f32),
                    Object::Real((*y + LINK_FONT_SIZE as f64 + 2.0) as f32),
                ]),
            );
            annot.set("Border", Object::Array(vec![0.into(), 0.into(), 0.into()]));
            annot.set("A", Object::Dictionary(action));
            annots.push(Object::Dictionary(annot));
        }
        if let Ok(page) = doc.get_object_mut(page_id).and_then(|o| o.as_dict_mut()) {
            page.set("Annots", Object::Array(annots));
        }
    }

    doc.save(pdf)?;
    Ok(placed)
}

/// Entry point for `--status-pdf` (no download; reads the latest udi_details DB).
pub fn run() -> Result<(), Box<dyn Error>> {
    let db_dir = crate::app_data_dir().join("db");
    let db_path = crate::details::find_latest_db(&db_dir).ok_or_else(|| {
        format!(
            "No udi_details_*.db found in {} — run --details or --details-update first",
            db_dir.display()
        )
    })?;
    let dbname = db_path
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();
    eprintln!("[status-pdf] Reading distribution from {}", db_path.display());

    let d = read_dist(&db_path)?;
    let doc = build(&d, &dbname)?;
    let out = crate::export::output_pdf("swissdamed_triage_status")?;
    doc.render_to_file(&out)
        .map_err(|e| format!("PDF render {}: {}", out, e))?;

    // Overlay clickable source links on the legend page.
    let urls: Vec<&str> = LEGEND.iter().filter_map(|(_, _, u)| *u).collect();
    match add_links(Path::new(&out), &urls) {
        Ok(placed) if placed == urls.len() => {
            eprintln!("[status-pdf] {} legend links set", placed)
        }
        Ok(placed) => eprintln!(
            "[status-pdf] WARN: {} link lines found but {} URLs expected (links may be misaligned)",
            placed,
            urls.len()
        ),
        Err(e) => eprintln!("[status-pdf] WARN: link overlay failed (PDF still written): {}", e),
    }

    eprintln!(
        "[status-pdf] wrote {} (professional={} public={} [h{} m{} l{}] review={})",
        out,
        d.prof,
        d.ph + d.pm + d.pl,
        d.ph,
        d.pm,
        d.pl,
        d.rev
    );
    Ok(())
}
