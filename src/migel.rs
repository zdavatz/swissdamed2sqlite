use aho_corasick::{AhoCorasick, Input, StartKind};
use calamine::{open_workbook, Reader, Xlsx};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use unicode_normalization::UnicodeNormalization;

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
    /// DE category hierarchy keywords (from parent categories in XLSX)
    pub category_de: Vec<String>,
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
    "laenge", "breite", "hoehe", "durchmesser", // dimensions
    "links", "rechts",  // left/right
    // French
    "les", "des", "pour", "avec", "par", "une", "dans", "sur", "qui", "que",
    "achat", "location", "piece", "sans", "usage", "unique", "jetable",
    "securite", "securise",
    "largeur", "longueur", "hauteur", "diametre",  // dimensions — match across all products
    "gauche", "droite", "droit",  // left/right — match across all products
    // Italian
    "acquisto", "noleggio", "pezzo", "senza", "monouso", "perdere",
    "sicurezza",
    "larghezza", "lunghezza", "altezza", "diametro",  // dimensions
    "sinistra", "destra",  // left/right
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
    // Shape/form descriptors (match across unrelated product types)
    "tubolare", "tubulaire", "tubular",
    // Generic anatomical terms (match across surgical vs. orthopedic/support devices)
    "addominale", "abdominale", "abdominal",
    "cervicale", "cervical", "zervikal",
    // Generic functional terms (match across surgical/orthopedic devices)
    "sostegno", "soutien", "support", "stuetze",
    // Generic material/property terms (match across bandages, gauze, tape, etc.)
    "elastique", "elastico", "elastic",  // FR/IT/EN "elastic" — too generic cross-language
    // NOTE: "elastisch" (DE) intentionally NOT stop-worded — needed for "Tape elastisch" matching
    "stumpf", "mousse",  // "blunt" — matches across cannulas, retractors, screws
    // Generic body part / anatomy terms (too broad when used alone)
    "smussa", "smusso",  // IT "blunt" — matches across cannulas, screws, retractors
    // Generic device type terms (too many subtypes to match reliably)
    "aiguille",  // FR "needle" — matches all needle/cannula products
    "seringue",  // FR "syringe" — matches all syringe types
    "siringa",   // IT "syringe" — matches all syringe types
];

/// English-to-German medical term dictionary for matching products with English-only
/// descriptions against German MiGeL keywords. When an English term is found in the
/// product text, its German equivalents are appended to improve matching.
const EN_DE_MEDICAL_TERMS: &[(&str, &[&str])] = &[
    // Body parts / anatomical regions
    ("cervical", &["cervikalstuetze", "halskrause", "halswirbelsaeule"]),
    ("lumbar", &["lumbal", "lendenwirbelsaeule", "lumbalstuetze"]),
    ("thoracic", &["thorakal", "brustwirbelsaeule"]),
    ("spinal", &["wirbelsaeule", "spinal"]),
    ("knee", &["knie", "knieorthese", "kniebandage"]),
    ("ankle", &["sprunggelenk", "sprunggelenksorthese"]),
    ("wrist", &["handgelenk", "handgelenkorthese"]),
    ("shoulder", &["schulter", "schulterorthese"]),
    ("elbow", &["ellenbogen", "ellenbogenorthese"]),
    ("finger", &["finger", "fingerorthese"]),
    ("hip", &["huefte", "hueftorthese"]),
    // Orthopedic devices
    ("orthosis", &["orthese", "orthesen"]),
    ("orthoses", &["orthese", "orthesen"]),
    ("orthotic", &["orthese", "orthopaedische"]),
    ("orthotics", &["orthese", "orthopaedische"]),
    ("ortho", &["orthopaedische", "orthese"]),
    ("brace", &["orthese", "stuetze", "bandage"]),
    ("splint", &["schiene"]),
    ("support", &["bandage", "stuetze"]),
    ("prosthesis", &["prothese"]),
    ("prosthetic", &["prothese"]),
    ("insole", &["schuheinlage", "einlage"]),
    ("shoe", &["schuh", "spezialschuhe"]),
    ("shoes", &["schuhe", "spezialschuhe"]),
    ("footwear", &["schuhe", "spezialschuhe"]),
    ("rehab", &["rehabilitation"]),
    // Catheters / cannulas
    ("catheter", &["katheter"]),
    ("cannula", &["kanuele"]),
    ("needle", &["nadel", "kanuele"]),
    ("syringe", &["spritze"]),
    // Wound care
    ("bandage", &["bandage", "binde", "verband"]),
    ("dressing", &["verband", "wundverband"]),
    ("compress", &["kompresse"]),
    ("gauze", &["gaze", "gazekompresse"]),
    ("plaster", &["pflaster"]),
    ("tape", &["tape"]),
    // Respiratory
    ("ventilator", &["beatmungsgeraet"]),
    ("nebulizer", &["vernebler"]),
    ("inhaler", &["inhalator"]),
    ("oxygen", &["sauerstoff"]),
    ("mask", &["maske"]),
    // Compression / stockings
    ("stocking", &["strumpf", "kompressionsstrumpf"]),
    ("compression", &["kompression", "kompressionsbandage"]),
    // Infusion / injection
    ("infusion", &["infusion", "infusionsset"]),
    ("injection", &["injektion"]),
    ("pump", &["pumpe"]),
    // Ankle/foot
    ("ankle-foot", &["sprunggelenk", "fussorthese", "unterschenkel"]),
    ("scoliosis", &["skoliose", "rumpf", "orthesen"]),
    ("scoli", &["skoliose", "rumpf", "orthesen"]),
    ("tlso", &["rumpf", "orthesen", "thorakolumbal"]),
    ("sacroiliac", &["iliosakral"]),
    // Glucose monitoring
    ("glucose", &["glukose", "blutzucker"]),
    ("monitoring", &["ueberwachung", "monitoring"]),
    ("continuous", &["kontinuierlich"]),
    ("sensor", &["sensor", "sensoren"]),
    // Nebulizer / aerosol
    ("nebulizer", &["vernebler", "inhalationsgeraet", "aerosol"]),
    ("nebuliser", &["vernebler", "inhalationsgeraet", "aerosol"]),
    ("aerosol", &["aerosol", "vernebler"]),
    ("mesh", &["netz"]),
    // Condoms
    ("condom", &["kondom", "praservativ"]),
    // Stimulation / TENS
    ("stimulator", &["stimulator", "stimulationsgeraet"]),
    ("stimulation", &["stimulation"]),
    ("electrode", &["elektrode"]),
    ("transcranial", &["transkraniell"]),
    // Cotton / wound care basics
    ("cotton", &["watte", "baumwolle"]),
    // Auto-injector
    ("injector", &["injektor", "injektionshilfe"]),
    ("auto-injector", &["injektionshilfe", "pen"]),
    // Elbow
    ("cuff", &["manschette", "bandage"]),
    // General
    ("glove", &["handschuh"]),
    ("wheelchair", &["rollstuhl"]),
    ("walker", &["gehwagen", "rollator"]),
    ("crutch", &["kruecke", "gehstuetze"]),
    ("crutches", &["kruecken", "gehstuetzen"]),
    ("stabilisation", &["stabilisation"]),
    ("stabilization", &["stabilisation"]),
];

/// Enrich text with German translations of English medical terms.
/// Appends German equivalents when English terms are found, improving
/// matching against German MiGeL keywords.
pub fn enrich_with_german(text: &str) -> String {
    let lower = text.to_lowercase();
    let words: Vec<&str> = lower.split_whitespace().collect();
    let clean_words: Vec<String> = words
        .iter()
        .map(|w| w.trim_matches(|c: char| !c.is_alphanumeric()).to_string())
        .collect();
    let mut additions: Vec<&str> = Vec::new();

    for &(en_term, de_terms) in EN_DE_MEDICAL_TERMS {
        if clean_words.iter().any(|w| w == en_term) {
            for de in de_terms {
                additions.push(de);
            }
        }
    }

    // Context-aware mappings: certain word combinations map to specific terms
    let has = |term: &str| clean_words.iter().any(|w| w == term);
    // "ortho" + "rehab" together → orthopedic rehabilitation shoes
    if has("ortho") && has("rehab") {
        additions.push("spezialschuhe");
    }

    if additions.is_empty() {
        text.to_string()
    } else {
        format!("{} {}", text, additions.join(" "))
    }
}

/// Product-level negative keywords: if a product's combined text contains any of
/// these terms, it should NOT match MiGeL items in certain code ranges.
/// Format: (MiGeL code prefix, exclusion keyword).
/// These prevent interventional/surgical devices from matching patient-facing MiGeL items.
const NEGATIVE_KEYWORDS: &[(&str, &str)] = &[
    // Dwelling catheters (15.11) should NOT match interventional devices
    ("15.11", "stent"),
    ("15.11", "endotracheal"),
    ("15.11", "endotracheale"),
    ("15.11", "dilatation"),
    ("15.11", "dilator"),
    ("15.11", "angiograph"),
    ("15.11", "angioplast"),
    ("15.11", "vascular"),
    ("15.11", "vasculaire"),
    ("15.11", "vascolare"),
    ("15.11", "esophageal"),
    ("15.11", "pyloric"),
    ("15.11", "colonic"),
    ("15.11", "tubus"),
    // Venous/safety cannulas should NOT match cardiac/interventional/GI catheters
    ("03.07.09.05", "launcher"),
    ("03.07.09.05", "guiding"),
    ("03.07.09.05", "mapping"),
    ("03.07.09.05", "ablation"),
    ("03.07.09.05", "diagnostic"),
    ("03.07.09.05", "angiograph"),
    ("03.07.09.05", "thermocouple"),
    ("03.07.09.05", "ercp"),
    ("03.07.09.05", "ureteral"),
    ("03.07.09.05", "pta"),
    ("03.07.09.05", "dilatation"),
    ("03.07.09.13", "ureteral"),
    ("03.07.09.13", "ercp"),
    ("03.07.09.13", "pta"),
    ("03.07.09.13", "dilatation"),
    ("03.07.09.13", "whistle"),
    // Katheterventil (15.13.01) should NOT match interventional catheters
    ("15.13.01", "pta"),
    ("15.13.01", "dilatation"),
    ("15.13.01", "angiograph"),
    ("15.13.01", "balloon"),
    ("15.13.01", "stent"),
    // Drawing-up cannulas (03.07.09.09, 03.07.09.10) should NOT match suction devices
    ("03.07.09.09", "saugansatz"),
    ("03.07.09.09", "saugkanuele"),
    ("03.07.09.09", "yankauer"),
    ("03.07.09.09", "frazier"),
    ("03.07.09.09", "absaugkath"),
    ("03.07.09.09", "schraube"),
    ("03.07.09.10", "saugansatz"),
    ("03.07.09.10", "yankauer"),
    ("03.07.09.10", "frazier"),
    // Bladder catheters (15.10) should NOT match scalpels or needles
    ("15.10", "skalpell"),
    ("15.10", "scalpel"),
    ("15.10", "bistouri"),
    ("15.10", "bisturi"),
    ("15.10", "nadel"),
    ("15.10", "electrode"),
    ("15.10", "elektrode"),
    // Catheter handles (15.13.06) should NOT match catheters or surgical handles
    ("15.13.06", "frauenkatheter"),
    ("15.13.06", "nelaton"),
    ("15.13.06", "ballonkatheter"),
    ("15.13.06", "absaugkatheter"),
    ("15.13.06", "ventrikelkatheter"),
    ("15.13.06", "verweilkatheter"),
    ("15.13.06", "ureteral"),
    ("15.13.06", "drainage"),
    ("15.13.06", "angiokatheter"),
    ("15.13.06", "malecot"),
    ("15.13.06", "whistle"),
    ("15.13.06", "raspel"),
    ("15.13.06", "koax"),
    ("15.13.06", "metagl"),
    ("15.13.06", "open-end"),
    // Safety butterfly needles (03.07.09.14) should NOT match electrodes/needle holders
    ("03.07.09.14", "nadelhalter"),
    ("03.07.09.14", "elektrode"),
    ("03.07.09.14", "electrode"),
    ("03.07.09.14", "aspiration"),
    // Vaginal pessary (15.30) should NOT match specula (different device)
    ("15.30", "spekula"),
    ("15.30", "speculum"),
    // Drawing-up cannulas should NOT match gauze/tupfer/suction products
    ("03.07.09.09", "tupfer"),
    ("03.07.09.09", "tampon"),
    ("03.07.09.09", "gaze"),
    ("03.07.09.09", "garza"),
    ("03.07.09.09", "saugset"),
    ("03.07.09.10", "tupfer"),
    ("03.07.09.10", "tampon"),
    ("03.07.09.10", "gaze"),
    ("03.07.09.10", "saugset"),
    // Cervikalstütze (22.12) should NOT match drapes, surgical tools
    ("22.12", "abdecktuch"),
    ("22.12", "schraube"),
    ("22.12", "platte"),
    // Einweg Pinzette (99.31.05) should NOT match connectors
    ("99.31.05", "konnektor"),
    ("99.31.05", "connector"),
    // --- Orthesis body-part exclusions ---
    // Hand-Orthesen (23.21) should NOT match other body parts
    ("23.21", "patella"),
    ("23.21", "rotula"),
    ("23.21", "knie"),
    ("23.21", "genou"),
    ("23.21", "ginocchio"),
    ("23.21", "hueft"),
    ("23.21", "hanche"),
    ("23.21", "anca"),
    ("23.21", "sprunggelenk"),
    ("23.21", "cheville"),
    ("23.21", "caviglia"),
    ("23.21", "malleo"),
    ("23.21", "schulter"),
    ("23.21", "epaule"),
    ("23.21", "spalla"),
    ("23.21", "humerus"),
    // Knie-Orthesen (23.04) should NOT match other body parts
    ("23.04", "handgelenk"),
    ("23.04", "poignet"),
    ("23.04", "polso"),
    ("23.04", "daumen"),
    ("23.04", "pouce"),
    ("23.04", "pollice"),
    ("23.04", "finger"),
    ("23.04", "doigt"),
    ("23.04", "dito"),
    ("23.04", "sprunggelenk"),
    ("23.04", "cheville"),
    ("23.04", "caviglia"),
    ("23.04", "malleo"),
    ("23.04", "schulter"),
    ("23.04", "epaule"),
    ("23.04", "spalla"),
    ("23.04", "hueft"),
    ("23.04", "hanche"),
    ("23.04", "anca"),
    // Sprunggelenks-Orthesen (23.02) should NOT match other body parts
    ("23.02", "knie"),
    ("23.02", "genou"),
    ("23.02", "ginocchio"),
    ("23.02", "handgelenk"),
    ("23.02", "poignet"),
    ("23.02", "polso"),
    ("23.02", "schulter"),
    ("23.02", "epaule"),
    ("23.02", "spalla"),
    // Schulter-Orthesen (23.25) should NOT match other body parts
    ("23.25", "knie"),
    ("23.25", "genou"),
    ("23.25", "ginocchio"),
    ("23.25", "sprunggelenk"),
    ("23.25", "cheville"),
    ("23.25", "caviglia"),
    ("23.25", "handgelenk"),
    ("23.25", "poignet"),
    ("23.25", "polso"),
    // --- Compress type exclusions ---
    // Augenkompressen (35.01.12) should NOT match general gauze/compresses
    ("35.01.12", "gazekompresse"),
    ("35.01.12", "saugkompresse"),
    ("35.01.12", "vlieskompresse"),
    ("35.01.12", "salbenkompresse"),
    // --- Katheterventil (15.13.01) should NOT match catheters themselves ---
    ("15.13.01", "ureteral"),
    ("15.13.01", "frauenkatheter"),
    ("15.13.01", "tiemann"),
    ("15.13.01", "drainage"),
    ("15.13.01", "pushing"),
    ("15.13.01", "angiokatheter"),
    ("15.13.01", "urinalkondom"),
    ("15.13.01", "perifix"),
    ("15.13.01", "trokarkatheter"),
    ("15.13.01", "malecot"),
    ("15.13.01", "open-end"),
    ("15.13.01", "cone tip"),
    // --- Infusions-Set (99.30.06) should NOT match culture media or pumps ---
    ("99.30.06", "bouillon"),
    ("99.30.06", "infusomat"),
    ("99.30.06", "agar"),
    ("99.30.06", "sabouraud"),
    // --- Schlauchverbände (35.01.08) should NOT match other dressing types ---
    ("35.01.08", "folienverband"),
    ("35.01.08", "schaumverband"),
    ("35.01.08", "calciumalginat"),
    ("35.01.08", "alginat"),
    ("35.01.08", "hydrokolloid"),
    ("35.01.08", "hydropolymer"),
    ("35.01.08", "wundversorgungs"),
    ("35.01.08", "wundauflage"),
    ("35.01.08", "spruehverband"),
    ("35.01.08", "spray"),
    ("35.01.08", "hydrofaser"),
    ("35.01.08", "wundfueller"),
    // --- Wegwerfspritze (03.07.10.15) should NOT match suction devices ---
    ("03.07.10.15", "saugansatz"),
    ("03.07.10.15", "yankauer"),
    // --- Knie-Orthesen (23.04) should NOT match net bandages or wrong body parts ---
    ("23.04", "netzschlauchverband"),
    ("23.04", "humerus"),
    ("23.04", "omero"),
    // --- Bladder catheters (15.10) should NOT match drainage systems ---
    ("15.10", "wunddrainage"),
    ("15.10", "redon"),
    // --- Transfer-Set (03.07.09.20) should NOT match wound dressings or covers ---
    ("03.07.09.20", "mepilex"),
    ("03.07.09.20", "rollbrett"),
    ("03.07.09.20", "bezug"),
    // --- Infusions-Set should NOT match infusion catheters ---
    ("99.30.06", "cragg"),
    ("99.30.06", "mcnamara"),
    // --- Patellasehnenband (22.04) should NOT match incontinence products ---
    ("22.04", "inkontinenz"),
    ("22.04", "tena"),
    ("22.04", "einlage"),
    // --- Katheterverschluss (15.13.01.00) should NOT match surgical instruments ---
    ("15.13.01.00", "knochenspreizzange"),
    ("15.13.01.00", "rippenhalterung"),
    ("15.13.01.00", "verschluss-halbring"),
    ("15.13.01.00", "fixationsplatte"),
    // --- Handgriff für Katheter should NOT match surgical electrode handles or catheters ---
    ("15.13.06", "elektrode"),
    ("15.13.06", "electrode"),
    ("15.13.06", "kippschalter"),
    ("15.13.06", "tiemann"),
    ("15.13.06", "careflow"),
    ("15.13.06", "bicakcilar"),
    // --- Dreiweghahn (03.07.02.01) should NOT match industrial taps ---
    ("03.07.02.01", "kanister"),
    // --- Schlauchverbände should NOT match wound change sets ---
    ("35.01.08", "verbandwechselset"),
    // --- Entnahmespike (03.07.09.18) should NOT match urine bags ---
    ("03.07.09.18", "urinbeutel"),
    ("03.07.09.18", "beinbeutel"),
    // --- Spüllösung (99.11) should NOT match implant components ---
    ("99.11", "schaft"),
    ("99.11", "tige"),
    ("99.11", "konus"),
    ("99.11", "prothese"),
    // --- Infusions-Set should NOT match bottle holders/warmers ---
    ("99.30.06", "flaschenhalterung"),
    ("99.30.06", "halterung"),
    // --- Aufziehkanüle (03.07.09.09) should NOT match bone instruments ---
    ("03.07.09.09", "knochen"),
    ("03.07.09.09", "zange"),
    // --- Heft-/Fixier-Pflaster (35.01.09) should NOT match retractors ---
    ("35.01.09", "retraktor"),
    ("35.01.09", "ecarteur"),
    ("35.01.09", "divaricatore"),
    // --- Bladder catheters should NOT match drills or other surgical tools ---
    ("15.10", "bohrer"),
    ("15.10", "foret"),
    ("15.10", "fresa"),
    // --- Spüllösung should NOT match X-ray templates or test components ---
    ("99.11", "roentgenschablone"),
    ("99.11", "testschaft"),
    // --- Einweg Pinzette should NOT match plastic sheets ---
    ("99.31.05", "unterlage"),
    ("99.31.05", "plastikunterlage"),
    // --- Brillen/Kontaktlinsen should NOT match nasal cannulas ---
    ("25.01", "nasenbrille"),
    ("25.01", "sauerstoff"),
    // --- Schlauchverbände should NOT match wound contact layers, wound gels, or adhesive dressings ---
    ("35.01.08", "wundkontaktschicht"),
    ("35.01.08", "wunddistanzgitter"),
    ("35.01.08", "wundgel"),
    ("35.01.08", "wundverband"),
    ("35.01.08", "cosmopor"),
    // --- Ständer/Infusionsständer should NOT match feeding tubes ---
    ("03.07.08", "ernaehrungssonde"),
    ("03.07.08", "nasoenteral"),
    // --- Spezialschuhe (26.01) should NOT match beds or dental products ---
    ("26.01", "bett"),
    ("26.01", "rahmen"),
    ("26.01", "pflegebett"),
    // --- Hand-Orthesen (23.21) should NOT match dental products ---
    ("23.21", "gum"),   // dental brand GUM ≠ hand orthosis
];

/// Normalize German umlauts so ALL-CAPS text (e.g. ABSAUGGERAETE) matches
/// proper text (e.g. Absauggeräte).
/// First applies Unicode NFC normalization to handle combining characters
/// (e.g., e + combining accent → precomposed é).
pub fn normalize_german(text: &str) -> String {
    let text: String = text.nfc().collect();
    text.replace('ä', "ae")
        .replace('ö', "oe")
        .replace('ü', "ue")
        .replace('ß', "ss")
        .replace('Ä', "Ae")
        .replace('Ö', "Oe")
        .replace('Ü', "Ue")
        .replace('é', "e").replace('É', "E")
        .replace('è', "e").replace('È', "E")
        .replace('ê', "e").replace('Ê', "E")
        .replace('à', "a").replace('À', "A")
        .replace('â', "a").replace('Â', "A")
        .replace('ù', "u").replace('Ù', "U")
        .replace('û', "u").replace('Û', "U")
        .replace('ô', "o").replace('Ô', "O")
        .replace('î', "i").replace('Î', "I")
        .replace('ç', "c").replace('Ç', "C")
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

            // Category hierarchy keywords (from parent categories, >= 8 chars)
            // e.g., "Injektions- und Infusionsmaterialien" → ["injektions", "infusionsmaterialien"]
            // Only long, specific terms to avoid generic matches
            let cat_text = category_texts.iter()
                .filter(|t| !t.is_empty())
                .cloned()
                .collect::<Vec<_>>()
                .join(" ");
            let category_de = extract_keywords_from(&cat_text, 8);

            // All keywords: full Bezeichnung text (all lines) + Limitation text + category
            // for broader candidate finding via the inverted index.
            let mut all_kw = extract_keywords_full(&bezeichnung);
            if !limitation.is_empty() {
                let lim_kw = extract_keywords_full(&limitation);
                all_kw.extend(lim_kw);
            }
            all_kw.extend(category_de.clone());
            all_kw.sort();
            all_kw.dedup();

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
                category_de,
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

/// Pre-built search index: inverted keyword→items map + Aho-Corasick automaton.
pub struct MigelSearchIndex {
    /// Aho-Corasick automaton built from all keywords + truncated variants
    automaton: AhoCorasick,
    /// Maps automaton pattern ID → set of MigelItem indices
    pattern_items: Vec<Vec<usize>>,
    /// IDF weights: keyword → log(N/df) where N=total items, df=items containing keyword
    pub idf_weights: HashMap<String, f64>,
}

/// Build an Aho-Corasick search index for fast candidate finding.
pub fn build_search_index(items: &[MigelItem]) -> MigelSearchIndex {
    // Build inverted index: keyword → item indices
    let mut keyword_to_items: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, item) in items.iter().enumerate() {
        for kw in &item.all_keywords {
            keyword_to_items.entry(kw.clone()).or_default().push(i);
        }
    }

    // Compute IDF weights: log(N / df) for each keyword, capped to prevent extremes
    // Keywords appearing in fewer items get higher weight (more discriminating)
    // Cap at 3.0 to prevent very rare keywords from dominating the score
    let n = items.len() as f64;
    let mut idf_weights: HashMap<String, f64> = HashMap::new();
    for (keyword, item_indices) in &keyword_to_items {
        let df = item_indices.len() as f64;
        let idf = (n / df).ln().max(0.1).min(5.0);
        idf_weights.insert(keyword.clone(), idf);
    }

    // Build pattern → item indices map, merging truncated variants
    // A pattern string can map to items from multiple keywords
    let mut pattern_to_items: HashMap<String, HashSet<usize>> = HashMap::new();

    for (keyword, item_indices) in &keyword_to_items {
        // Full keyword pattern
        pattern_to_items
            .entry(keyword.clone())
            .or_default()
            .extend(item_indices);

        // Truncated variant for fuzzy matching (>= 7 chars)
        if keyword.len() >= 7 {
            let trunc = keyword[..keyword.len() - 1].to_string();
            pattern_to_items
                .entry(trunc)
                .or_default()
                .extend(item_indices);
        }
    }

    // Convert to ordered vectors for AC automaton
    let mut patterns: Vec<String> = Vec::new();
    let mut pattern_items: Vec<Vec<usize>> = Vec::new();

    for (pattern, items_set) in pattern_to_items {
        patterns.push(pattern);
        pattern_items.push(items_set.into_iter().collect());
    }

    let automaton = AhoCorasick::builder()
        .start_kind(StartKind::Unanchored)
        .build(&patterns)
        .expect("Failed to build Aho-Corasick automaton");

    MigelSearchIndex {
        automaton,
        pattern_items,
        idf_weights,
    }
}

/// Split text into words (split on non-alphanumeric characters).
fn split_words(text: &str) -> Vec<&str> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|w| !w.is_empty())
        .collect()
}

/// Common German compound word components that can be extracted as prefixes.
/// Maps prefix → minimum remaining length (to prevent false splits).
const COMPOUND_PREFIXES: &[(&str, usize)] = &[
    ("blasen", 6),       // Blasenkatheter → blasen + katheter
    ("frauen", 6),       // Frauenkatheter → frauen + katheter
    ("verweil", 6),      // Verweilkatheter → verweil + katheter
    ("einmal", 6),       // Einmalblasenkatheter → einmal + blasenkatheter
    ("sicherheits", 5),  // Sicherheitskanüle → sicherheits + kanüle
    ("absaug", 6),       // Absaugkatheter → absaug + katheter
    ("infusions", 5),    // Infusionsschlauch → infusions + schlauch
    ("kompressions", 5), // Kompressionsbinde → kompressions + binde
    ("verbindungs", 5),  // Verbindungsschlauch → verbindungs + schlauch
    ("wund", 5),         // Wundverband → wund + verband
    ("augen", 6),        // Augenkompresse → augen + kompresse
    ("saug", 6),         // Saugkompresse → saug + kompresse
    ("ballon", 6),       // Ballonkatheter → ballon + katheter
];

/// Try to decompose a German compound word into known prefix + remainder.
/// Returns the prefix if found and the remainder is long enough.
fn decompose_compound(word: &str) -> Option<(&str, &str)> {
    for &(prefix, min_rest) in COMPOUND_PREFIXES {
        if word.len() > prefix.len() + min_rest && word.starts_with(prefix) {
            return Some((prefix, &word[prefix.len()..]));
        }
    }
    None
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
        // Suffix match in German compound words (keyword must be tail of compound)
        if suffix && word.len() > keyword.len() + 2 && word.ends_with(keyword) {
            return true;
        }
        // Compound prefix decomposition: check if word decomposes into
        // a known prefix + keyword remainder (e.g., "blasenkatheter" → "blasen" + "katheter")
        // The PREFIX must match the keyword, and the REMAINDER must also be a known
        // compound component (prevents false decomposition of arbitrary words)
        if suffix {
            if let Some((prefix, remainder)) = decompose_compound(word) {
                if prefix == keyword {
                    // Verify remainder is also a meaningful medical term (>= 6 chars)
                    if remainder.len() >= 6 {
                        return true;
                    }
                }
            }
        }
    }
    if fuzzy && keyword.len() >= 6 {
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

/// Compute keyword overlap score using word-level matching.
/// Returns (score, max_matched_keyword_len, matched_count, idf_score).
/// - score: length-weighted ratio (used for threshold decisions)
/// - idf_score: IDF-weighted ratio (used for ranking among passing candidates)
fn keyword_score(
    text_words: &[&str],
    keywords: &[String],
    suffix: bool,
    fuzzy: bool,
    idf: &HashMap<String, f64>,
) -> (f64, usize, usize, f64) {
    let total_len: f64 = keywords.iter().map(|k| k.len() as f64).sum();
    let total_idf: f64 = keywords.iter().map(|k| {
        let idf_w = idf.get(k.as_str()).copied().unwrap_or(1.0);
        k.len() as f64 * idf_w
    }).sum();
    if total_len == 0.0 {
        return (0.0, 0, 0, 0.0);
    }
    let mut matched_len = 0.0;
    let mut matched_idf = 0.0;
    let mut max_matched_len = 0;
    let mut matched_count = 0;
    for kw in keywords {
        if word_match(text_words, kw, suffix, fuzzy) {
            let idf_w = idf.get(kw.as_str()).copied().unwrap_or(1.0);
            matched_len += kw.len() as f64;
            matched_idf += kw.len() as f64 * idf_w;
            matched_count += 1;
            if kw.len() > max_matched_len {
                max_matched_len = kw.len();
            }
        }
    }
    let score = matched_len / total_len;
    let idf_score = if total_idf > 0.0 { matched_idf / total_idf } else { 0.0 };
    (score, max_matched_len, matched_count, idf_score)
}

/// Product keyword patterns that indicate an interventional/surgical device
/// which should NOT match any MiGeL code. These are checked against the
/// combined DE+FR+IT text.
const UNIVERSAL_EXCLUSIONS: &[&[&str]] = &[
    // PTA balloon dilatation catheters (interventional cardiology)
    &["pta", "balloon"],
    &["pta", "ballonnet"],
    &["pta", "palloncino"],
    // Stent delivery systems
    &["stent", "balloon"],
    &["stent", "expandable"],
    // Angiographic/diagnostic catheters
    &["angiograph", "catheter"],
    &["angiograph", "catetere"],
    // Ablation catheters
    &["ablation", "catheter"],
    &["ablation", "katheter"],
    // ERCP (endoscopy) catheters
    &["ercp"],
    // Mapping catheters (cardiac electrophysiology)
    &["mapping", "catheter"],
    // Guiding/launcher catheters (interventional)
    &["launcher"],
    &["guiding", "catheter"],
    // Ureteral catheters (specialty urology, not MiGeL patient devices)
    &["ureteral"],
    &["uretrale"],
    // Angiographic catheters
    &["angiokatheter"],
    &["angiodyn"],
    // Drainage catheter sets (surgical)
    &["malecot"],
    &["drainage", "catheter"],
    // Pushing/delivery catheters
    &["pushing", "catheter"],
    // Whistle tip catheters (urology specialty)
    &["whistle", "tip"],
    // Cone tip catheters
    &["cone", "tip", "catheter"],
    // Closure/endovascular catheters
    &["closurefast"],
    &["endovascular"],
    // Thermocouple catheters (cardiac ablation)
    &["thermocouple"],
    // Transjugular/renal specialty catheters
    &["transjugular"],
    // Cardiac navigation/mapping catheters (electrophysiology)
    &["navistar"],
    // Infusion warmers (not infusion sets)
    &["infusion", "warmer"],
    // Surgical gloves (not MiGeL patient devices)
    &["surgical", "gloves"],
    &["surgical", "glove"],
];

/// Check if a product is universally excluded from all MiGeL matching.
fn is_universally_excluded(combined_text: &str) -> bool {
    for pattern in UNIVERSAL_EXCLUSIONS {
        if pattern.iter().all(|kw| combined_text.contains(kw)) {
            return true;
        }
    }
    false
}

/// Check if a product is excluded from matching a specific MiGeL item.
fn is_excluded_by_negative_keywords(combined_text: &str, migel_code: &str) -> bool {
    for &(code_prefix, exclusion_kw) in NEGATIVE_KEYWORDS {
        if migel_code.starts_with(code_prefix) && combined_text.contains(exclusion_kw) {
            return true;
        }
    }
    false
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
    search_index: &MigelSearchIndex,
) -> Option<&'a MigelItem> {
    // Enrich with German translations of English medical terms before normalizing
    let de_enriched = enrich_with_german(&format!("{} {}", desc_de, brand));
    let de_lower = normalize_german(&de_enriched).to_lowercase();
    let fr_lower = normalize_german(&format!("{} {}", desc_fr, brand)).to_lowercase();
    let it_lower = normalize_german(&format!("{} {}", desc_it, brand)).to_lowercase();

    // If all language fields are identical, the product likely has English-only text.
    // Skip FR/IT scoring to prevent cross-language false positives (e.g., English
    // "catheter" matching FR keyword "cathéter" for a completely different device type).
    let fr_is_distinct = desc_fr != desc_de;
    let it_is_distinct = desc_it != desc_de;

    // Combined text only for candidate finding (broad pre-filter)
    let combined = format!("{} {} {}", de_lower, fr_lower, it_lower);

    // Pre-split text into words for word-level matching in scoring
    let de_words = split_words(&de_lower);
    let fr_words = split_words(&fr_lower);
    let it_words = split_words(&it_lower);

    // Step 0: Check universal exclusions (interventional/surgical devices)
    if is_universally_excluded(&combined) {
        return None;
    }

    // Step 1: Find candidate items via Aho-Corasick automaton (single overlapping scan)
    let mut candidates: HashSet<usize> = HashSet::new();
    let input = Input::new(&combined);
    for mat in search_index.automaton.find_overlapping_iter(input) {
        for &idx in &search_index.pattern_items[mat.pattern().as_usize()] {
            candidates.insert(idx);
        }
    }

    // Step 2: Score each candidate using WORD-LEVEL matching against per-language text
    // DE uses fuzzy word matching (handles German plural/case: Orthese/Orthesen)
    // FR/IT use exact word matching only
    // Secondary keywords from additional lines count as bonus matches
    let mut passing: Vec<(usize, f64, usize, f64)> = candidates
        .iter()
        .filter_map(|&idx| {
            let item = &migel_items[idx];
            // Primary scores (first-line keywords)
            // Skip FR/IT scoring if the product has identical text in all fields
            // Check negative keywords before scoring
            if is_excluded_by_negative_keywords(&combined, &item.position_nr) {
                return None; // filtered out; tracked via passing count vs candidate count
            }

            let idf = &search_index.idf_weights;
            let (score_de, max_len_de, count_de, idf_de) = keyword_score(&de_words, &item.keywords_de, true, true, idf);
            let (score_fr, max_len_fr, count_fr, idf_fr) = if fr_is_distinct {
                keyword_score(&fr_words, &item.keywords_fr, false, false, idf)
            } else {
                (0.0, 0, 0, 0.0)
            };
            let (score_it, max_len_it, count_it, idf_it) = if it_is_distinct {
                keyword_score(&it_words, &item.keywords_it, false, false, idf)
            } else {
                (0.0, 0, 0, 0.0)
            };

            // Secondary bonus matches: only count if at least 1 primary keyword matched
            let (_, sec_max_de, sec_count_de, _) = if count_de > 0 {
                keyword_score(&de_words, &item.secondary_de, true, true, idf)
            } else {
                (0.0, 0, 0, 0.0)
            };
            let (_, sec_max_fr, sec_count_fr, _) = if count_fr > 0 && fr_is_distinct {
                keyword_score(&fr_words, &item.secondary_fr, false, false, idf)
            } else {
                (0.0, 0, 0, 0.0)
            };
            let (_, sec_max_it, sec_count_it, _) = if count_it > 0 && it_is_distinct {
                keyword_score(&it_words, &item.secondary_it, false, false, idf)
            } else {
                (0.0, 0, 0, 0.0)
            };

            // Category hierarchy bonus (DE only): boosts IDF ranking but does NOT
            // count toward the match count threshold (to prevent generic category
            // terms from pushing weak matches over the threshold)
            let (_, cat_max_de, _, cat_idf_de) = if count_de > 0 {
                keyword_score(&de_words, &item.category_de, true, true, idf)
            } else {
                (0.0, 0, 0, 0.0)
            };

            // Total count = primary + secondary (category NOT included in count)
            let total_de = count_de + sec_count_de;
            let total_fr = count_fr + sec_count_fr;
            let total_it = count_it + sec_count_it;
            let max_de = max_len_de.max(sec_max_de).max(cat_max_de);
            let max_fr = max_len_fr.max(sec_max_fr);
            let max_it = max_len_it.max(sec_max_it);

            // Pick the best-scoring language (by primary score for threshold)
            let (best_score, best_max_len, best_count) = [
                (score_de, max_de, total_de),
                (score_fr, max_fr, total_fr),
                (score_it, max_it, total_it),
            ]
                .iter()
                .copied()
                .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal))
                .unwrap_or((0.0, 0, 0));

            // Best IDF score across languages (for ranking among passing candidates)
            // Category IDF bonus rewards matches where product text aligns with
            // the MiGeL item's parent category (e.g., "Injektions-" for needle items)
            let best_idf = idf_de.max(idf_fr).max(idf_it) + cat_idf_de * 0.5;

            // Bidirectional bonus: reward matches where the matched keyword(s)
            // cover a large fraction of the product's significant words.
            // A product named "Katheterventil" matching MiGeL "Katheterventil" gets
            // high coverage (1.0), while a 30-word surgical instrument description
            // matching one keyword gets low coverage (~0.03).
            let significant_words = de_words.iter()
                .filter(|w| w.len() >= 4)
                .count()
                .max(1) as f64;
            let coverage = best_count as f64 / significant_words;
            let best_idf = best_idf + coverage * 0.3;

            // Phrase matching bonus: if the MiGeL Bezeichnung (first line) appears
            // as a substring in the product text, it's a very strong signal.
            let bez_lower = normalize_german(&item.bezeichnung).to_lowercase();
            let phrase_bonus = if bez_lower.len() >= 8 && de_lower.contains(&bez_lower) {
                1.0 // strong boost for exact phrase match
            } else {
                0.0
            };
            let best_idf = best_idf + phrase_bonus;

            // DE significant word count (for length penalty on verbose descriptions)
            let de_sig_words = de_words.iter().filter(|w| w.len() >= 4).count();

            // Match criteria (length-based score for stable thresholds):
            // - 2+ matched keywords: score >= 0.3, max keyword len >= 6
            // - 1 matched keyword: score >= 0.5, keyword len >= 8
            // - Very long DE descriptions (15+ significant words) with single keyword:
            //   require higher score (>= 0.7) to reduce random keyword overlap in
            //   verbose surgical instrument descriptions
            let passes = if best_count >= 2 {
                best_score >= 0.3 && best_max_len >= 6
            } else if de_sig_words >= 15 {
                best_score >= 0.7 && best_max_len >= 8
            } else {
                best_score >= 0.5 && best_max_len >= 8
            };

            if passes {
                // Use IDF score for ranking (prefers matches on rare, specific keywords)
                Some((idx, best_idf, best_max_len, best_score))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    // Sort by IDF score descending, then max_len descending
    passing.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(b.2.cmp(&a.2))
    });

    // Return the best-ranked candidate
    passing.first().map(|&(idx, _, _, _)| &migel_items[idx])
}
