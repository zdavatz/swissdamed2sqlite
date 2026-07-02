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
    "der",
    "die",
    "das",
    "den",
    "dem",
    "des",
    "ein",
    "eine",
    "eines",
    "einem",
    "einen",
    "einer",
    "fuer",
    "mit",
    "von",
    "und",
    "oder",
    "bei",
    "auf",
    "nach",
    "ueber",
    "unter",
    "aus",
    "bis",
    "pro",
    "als",
    "inkl",
    "exkl",
    "max",
    "min",
    "per",
    "zur",
    "zum",
    "ins",
    "vom",
    "ohne",
    "auch",
    "sich",
    "noch",
    "wenn",
    "muss",
    "darf",
    "resp",
    "bzw",
    // German generic terms (too common in both MiGeL and products)
    "kauf",
    "miete",
    "tag",
    "jahr",
    "monate",
    "stueck",
    "set",
    "alle",
    "nur",
    "wird",
    "ist",
    "kann",
    "sind",
    "werden",
    "wurde",
    "hat",
    "haben",
    "steril",
    "unsteril",
    "sterile",
    "non", // too common across all medical products
    "diverse",
    "divers",
    "diversi", // MiGeL catch-all qualifier
    "gross",
    "klein",
    "lang",
    "kurz", // size/length descriptors
    "position",
    "definierte",
    "einstellbare", // MiGeL qualifiers
    "laenge",
    "breite",
    "hoehe",
    "durchmesser", // dimensions
    "links",
    "rechts", // left/right
    // French
    "les",
    "des",
    "pour",
    "avec",
    "par",
    "une",
    "dans",
    "sur",
    "qui",
    "que",
    "achat",
    "location",
    "piece",
    "sans",
    "usage",
    "unique",
    "jetable",
    "securite",
    "securise",
    // Generic French company-name tokens: companyName is appended into the
    // product text as "brand", so these would otherwise act as MiGeL keywords
    // (e.g. "fabrication"+"medicaux" from a manufacturer name matching
    // 17.02 "Serienfertigung"/"Med."). All real matches go via other keywords.
    "fabrication",
    "medicaux",
    "paramedicaux",
    "produits",
    "conception",
    "largeur",
    "longueur",
    "hauteur",
    "diametre", // dimensions — match across all products
    "gauche",
    "droite",
    "droit", // left/right — match across all products
    // Italian
    "acquisto",
    "noleggio",
    "pezzo",
    "senza",
    "monouso",
    "perdere",
    "sicurezza",
    "larghezza",
    "lunghezza",
    "altezza",
    "diametro", // dimensions
    "sinistra",
    "destra", // left/right
    // English
    "the",
    "for",
    "and",
    "with",
    "per",
    // Generic medical/product terms that match too broadly at word level
    "material",
    "produkt",
    "products",
    "product",
    "medical",
    "device",
    "system",
    "systeme",
    "systems",
    "geraet",
    "geraete",
    "appareil",
    // Cross-type medical terms (used for both screws/stockings/catheters/etc.)
    "compression",
    "compressione",
    "kompression",
    "verlaengerung",
    "extension",
    "estensione",
    "prolongation",
    "silikon",
    "silicone",
    // Generic surgical instrument terms (match across many unrelated instrument types)
    "ecarteur",
    "divaricatore",
    "retraktor",
    // Shape/form descriptors (match across unrelated product types)
    "tubolare",
    "tubulaire",
    "tubular",
    // Generic anatomical terms (match across surgical vs. orthopedic/support devices)
    "addominale",
    "abdominale",
    "abdominal",
    "cervicale",
    "cervical",
    "zervikal",
    // Generic functional terms (match across surgical/orthopedic devices)
    "sostegno",
    "soutien",
    "support",
    "stuetze",
    // Generic material/property terms (match across bandages, gauze, tape, etc.)
    "elastique",
    "elastico",
    "elastic", // FR/IT/EN "elastic" — too generic cross-language
    // NOTE: "elastisch" (DE) intentionally NOT stop-worded — needed for "Tape elastisch" matching
    "stumpf",
    "mousse", // "blunt" — matches across cannulas, retractors, screws
    // Generic body part / anatomy terms (too broad when used alone)
    "smussa",
    "smusso", // IT "blunt" — matches across cannulas, screws, retractors
    // Generic device type terms (too many subtypes to match reliably)
    "aiguille", // FR "needle" — matches all needle/cannula products
    "seringue", // FR "syringe" — matches all syringe types
    "siringa",  // IT "syringe" — matches all syringe types
];

/// Companies whose entire matched output is false positives (verified per company:
/// zero correct matches, pure non-MiGeL product lines). Shared by src/reports.rs
/// (CLI) and src/gui.rs (GUI) — single source of truth, exact-string matching on
/// `companyName`.
pub const EXCLUDED_COMPANIES: &[&str] = &[
    "Varian Medical Systems Inc",
    "Varian Medical Systems Inc.",
    "Sunstar Europe SA",
    // Diacor = patient-transfer furniture; SOMNOmedics = sleep-lab PSG sensors;
    // Accuratus = reusable surgical instruments; ATMOS = suction/ENT hardware;
    // CONCEPTION ET FABRICATION = dental products; iNOsystems = nitric-oxide
    // delivery hardware.
    "Diacor Inc",
    "SOMNOmedics AG",
    "Accuratus AG",
    "ATMOS MedizinTechnik GmbH & Co. KG",
    "CONCEPTION ET FABRICATION DE PRODUITS MEDICAUX ET PARAMEDICAUX",
    "iNOsystems SA",
    "Episurf Operations AB",
    "Aesculap AG",
    "Maquet Cardiopulmonary GmbH",
    "Philips Medizin Systeme Böblingen GmbH",
    "Invivo Corporation",
    "Invivo, a division of Philips Medical Systems",
    "Philips Healthcare (Suzhou) Co., Ltd.",
    "Philips Medical Systems DMC GmbH",
    "BEE Medic GmbH",
    "Medacta International SA",
    "Baitella AG",
    "Philips Medical Systems Nederland B.V.",
    // --- Jul 2026 audit additions (each verified 100% FP output, zero cross-
    // company collateral): ---
    "Dr. Jean Bausch GmbH & Co. KG", // dental articulating papers ('transfer' homonym); NOT Bausch & Lomb
    "Angelini Pharma Inc.", // ThermaCare heat wraps — proven code-hopper (23.03→23.10/23.04)
    "RFSU AB",              // contraceptive condoms ≠ 15.16 Urinal-Kondome
    "Braebon",              // sleep-lab sensors → 21.07.02 magnet
    "Lifemotion Medical Technology Co., Ltd.", // sleep-lab sensors → 21.07.02 magnet
    "Itamar Medical Ltd.",  // sleep-lab PAT sensors → 21.07.02 magnet
    "Maquet Critical Care AB", // ECMO/ICU sensors → 21.07.02 magnet
    "Becton Dickinson Infusion Therapy Systems Inc.", // IV cannulas; 'Infusion' in company name itself triggers
    "MANI, INC.",            // vitrectomy trocars — hop chain 03.07→99.30.06
    "Steeper Group Ltd",     // custom cosmetic prostheses = SVOT/OSM tariff, not keyword-assignable
    "Oertli-Instrumente AG", // phaco/vitrectomy consoles
    "Alpha-Bio Tec Ltd",     // dental abutments ('shoulder'/'transfer' homonyms)
    "Fesia Technology S.L.", // FES neurorehab garments — no MiGeL position
    "SAM Medical Products",  // emergency-trauma pelvic binders/splints
    "Dongguan Jiuhui Industrial Limited [EN]", // factory insoles — 26.01.01 requires individual fabrication
    "Cordis US Corp.",                         // vascular closure — hop 15.11→15.13 foreclosed
    "Hilotherm GmbH",                          // cold-therapy cuff holders
    "medK GmbH", // cath-lab inflation devices (hemostasis band ≠ 17.15 Kompressionsbandage)
    "Silony Medical GmbH", // spinal-surgery instruments ('VERTICALE Cervical' homonym)
    "BionIT Labs S.r.l.", // myoelectric prosthesis parts ('wrist' homonym)
];

/// Hard gates on structured UDI metadata: in-vitro diagnostics and Class III
/// (implants / interventional) devices are as a class never self-applied MiGeL
/// aids. Applied inside `find_best_migel_match` AFTER the curated forced
/// matches, so a verified pin (e.g. Omnipod 5 patch pumps, which are CLASS_III
/// closed-loop systems) overrides the coarse gate, while the whole IVD/Class-III
/// corpus stays immune to heuristic keyword drift.
pub fn is_metadata_excluded(device_type: &str, risk_class: &str) -> bool {
    device_type == "IVDR" || device_type == "IVDD" || risk_class == "CLASS_III"
}

/// English-to-German medical term dictionary for matching products with English-only
/// descriptions against German MiGeL keywords. When an English term is found in the
/// product text, its German equivalents are appended to improve matching.
const EN_DE_MEDICAL_TERMS: &[(&str, &[&str])] = &[
    // Body parts / anatomical regions
    (
        "cervical",
        &["cervikalstuetze", "halskrause", "halswirbelsaeule"],
    ),
    (
        "lumbar",
        &[
            "lumbal",
            "lendenwirbelsaeule",
            "lumbalstuetze",
            "orthese",
            "stabilisierung",
        ],
    ),
    ("lumbo", &["lumbal", "lendenwirbelsaeule", "lumbalstuetze"]),
    ("sacral", &["lendenwirbelsaeule", "lumbalstuetze"]),
    ("abdominal", &["leib", "leibbandage", "rumpf", "bandage"]),
    ("thoracic", &["thorakal", "brustwirbelsaeule"]),
    ("spinal", &["wirbelsaeule", "spinal"]),
    ("knee", &["knie", "knieorthese", "kniebandage"]),
    ("ankle", &["sprunggelenk", "sprunggelenksorthese"]),
    ("wrist", &["handgelenk", "handgelenkorthese"]),
    ("shoulder", &["schulter", "schulterorthese"]),
    (
        "clavicle",
        &["schluesselbein", "schluesselbeinbandage", "rucksackverband"],
    ),
    ("sling", &["schulterorthese"]),
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
    ("absorbent", &["superabsorber"]),
    ("superabsorbent", &["superabsorber"]),
    ("gelling", &["gelierend", "faserverband"]),
    ("hydrofiber", &["faserverband", "gelierend"]),
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
    (
        "ankle-foot",
        &["sprunggelenk", "fussorthese", "unterschenkel"],
    ),
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
    // Pulse oximeter / spirometer / peak flow → MiGeL 21.01
    (
        "oximeter",
        &["pulsoxymeter", "sauerstoffsaettigung", "pulsmonitor"],
    ),
    ("oximetry", &["pulsoxymeter", "sauerstoffsaettigung"]),
    ("spirometer", &["spirometriegeraet", "spirometrie"]),
    ("spirometry", &["spirometriegeraet", "spirometrie"]),
    // Condoms
    ("condom", &["kondom", "praservativ"]),
    // Stimulation / TENS
    ("stimulator", &["stimulator", "stimulationsgeraet"]),
    ("stimulation", &["stimulation"]),
    ("electrode", &["elektrode"]),
    ("electrodes", &["elektrode", "elektroden"]),
    (
        "neurostimulation",
        &["elektrode", "stimulation", "neuromuskular", "modulation"],
    ),
    ("transcutaneous", &["transkutan", "perkutan"]),
    (
        "tens",
        &["elektrode", "stimulation", "neuromuskular", "modulation"],
    ),
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
    ("stabilisation", &["stabilisierung", "stabilisation"]),
    ("stabilization", &["stabilisierung", "stabilisation"]),
    ("immobilisation", &["immobilisierung", "immobilisation"]),
    ("immobilization", &["immobilisierung", "immobilisation"]),
    ("immobiliser", &["immobilisierung", "orthese"]),
    ("immobilizer", &["immobilisierung", "orthese"]),
    ("mobilisation", &["mobilisierung", "mobilisation"]),
    ("mobilization", &["mobilisierung", "mobilisation"]),
    // "Heavy Inco" garments (ESSITY TENA) → 15.01 aufsaugende Inkontinenzhilfen.
    // Exact-word gate: never fires on "Incorporated"/"incontinence". Pushing
    // "schwere" biases IDF toward 15.01.02 (schwere Inkontinenz) matching "Heavy".
    (
        "inco",
        &["inkontinenz", "aufsaugende", "hilfsmittel", "schwere"],
    ),
    // Respironics InnoSpire nebulizers / LiteTouch aerosol masks (brand-exclusive
    // tokens; the devices carry no "nebulizer"/"aerosol" text of their own).
    ("innospire", &["vernebler", "aerosol"]),
    ("litetouch", &["maske", "aerosol"]),
    // Petrolatum(-impregnated) gauze → 35.01.02 Imprägnierte Wundkompresse.
    // Do NOT trigger on "paraffin" (histology-lab FPs: Sakura, VWR).
    (
        "petrolatum",
        &["impraegnierte", "wundkompresse", "nichtklebend"],
    ),
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
    let any_contains = |substr: &str| clean_words.iter().any(|w| w.contains(substr));
    // "ortho" + "rehab" together → orthopedic rehabilitation shoes
    if has("ortho") && has("rehab") {
        additions.push("spezialschuhe");
    }
    // Fecal incontinence insert/plug → MiGeL 15.40 Analtampon
    if has("fecal") && (has("incontinence") || has("insert") || has("plug")) {
        additions.push("analtampon");
    }
    // Arm compression sleeves → MiGeL 17.02.01.11.1 (Armkompressionsstrumpf KKL2, Serie)
    // Push the supporting descriptor keywords so the score crosses the threshold;
    // IDF ranking will pick the Armkompressionsstrumpf position over Waden/Schenkel
    // because "armkompressionsstrumpf" appears in only 3 MiGeL entries.
    if (has("arm") || any_contains("armsleeve")) && (has("sleeve") || has("sleeves")) {
        additions.push("armkompressionsstrumpf");
        additions.push("kompressionsklasse");
        additions.push("rundgestrickt");
        additions.push("serienfertigung");
    }
    // Ulcer (venous leg ulcer) compression system → MiGeL 17.05.01.00.1
    // (Unterschenkel-Kompressionsstrumpf-System für Ulcus cruris)
    if has("ulcer") || any_contains("ulcus") {
        additions.push("unterschenkel");
        additions.push("kompressionsstrumpf");
        additions.push("ulcus");
        additions.push("cruris");
        additions.push("system");
    }
    // Silicone foam wound dressing → MiGeL 35.05.03 (Hydropolymere)
    if (has("foam") || any_contains("schaumstoff"))
        && (has("dressing") || has("wound") || has("verband"))
    {
        additions.push("hydropolymere");
        additions.push("polyurethan");
        additions.push("schaumverband");
    }
    // Spine orthoses: thoracic/spinal/spine + orthotic context → MiGeL 22.13/22.15
    // (Brustwirbelsäulen-/Wirbelsäulen-Orthese). Only when an orthotic context word
    // is present to avoid matching X-ray AI / imaging products that say "thoracic".
    let spine_orthotic_ctx = has("orthese")
        || has("orthosis")
        || has("orthoses")
        || has("orthotic")
        || has("orthotics")
        || has("brace")
        || has("braceid")
        || has("immobiliser")
        || has("immobilizer")
        || has("tlso")
        || has("scoli")
        || has("scoliosis");
    if (has("thoracic") || has("thoraco")) && spine_orthotic_ctx {
        additions.push("brustwirbelsaeule");
        additions.push("thorax");
        additions.push("orthese");
    }
    if (has("spinal") || has("spine")) && spine_orthotic_ctx {
        additions.push("wirbelsaeule");
        additions.push("orthese");
    }
    // Shoulder abduction cushions/slings → MiGeL 22.09.03.00.1
    // (Schulterabduktionsorthese / Schulterabduktionskissen)
    let shoulder_abduction = any_contains("abduktionskissen")
        || any_contains("schulterkissen")
        || any_contains("armabduktion")
        || any_contains("ruthnersling")
        || ((has("shoulder") || has("schulter"))
            && (has("abduction") || has("abduktion") || has("pillow")));
    if shoulder_abduction {
        additions.push("schulterabduktionskissen");
        additions.push("schulterabduktionsorthese");
        additions.push("schultergeurtel");
        additions.push("orthese");
        additions.push("entlastung");
    }
    // Shoulder immobiliser → MiGeL 22.09.01.00.1 (Schultergürtel-Orthese zur Immobilisierung, Gilchrist)
    if (has("shoulder") || has("schulter")) && (has("immobiliser") || has("immobilizer")) {
        additions.push("schultergeurtel");
        additions.push("immobilisierung");
        additions.push("gilchrist");
    }
    // Knee orthosis context → MiGeL 22.04.xx (Kniegelenk-Orthese).
    // Only when there are orthotic context words present — avoids matching
    // pharma/OTC products like ThermaCare heat patches.
    let orthotic_context = has("orthese")
        || has("orthosis")
        || has("orthoses")
        || has("orthotic")
        || has("orthotics")
        || has("immobiliser")
        || has("immobilizer")
        || has("brace")
        || has("braceid")
        || any_contains("braceid");
    if has("knee") && orthotic_context {
        additions.push("kniegelenk");
    }
    if has("hip") && orthotic_context {
        additions.push("hueftgelenk");
    }
    // Compression garments named "... garment" → region-specific compression
    // codes (17.15 by mass / 05.11 trunk). Requires BOTH 'garment' and a region
    // word so generic apparel cannot trigger it.
    // Region detection uses "<region> garment" BIGRAMS with precedence
    // leg > arm/hand > face/head/neck > ear: Macom's deviceName "Leg, Arm and
    // Ear Garments" contributes bare 'arm'/'leg' words AND the 'ear garment'
    // bigram to EVERY row, so bare has() region words mis-route (all 116 LA001
    // rows took the arm branch) and ear must rank lowest. The tradeName carries
    // the row's true region as a bigram ("Leg Garment" / "Arm Garment" / ...).
    // Note: "Full Body Garment" rows never reach scoring — no full-body position
    // exists in MiGeL; they are killed by the ["full body","garment"] universal
    // exclusion.
    if has("garment") {
        if has("body") || has("trunk") || has("torso") || has("abdominal") {
            // Route torso garments to the serial Leib-/Rumpf-Bandage (05.11.10),
            // consistent with the already-matched abdominal garments. Push
            // "bandage" (a 05.11 keyword) NOT "kompressionsbandage", so serial
            // garments are not pulled into the custom-made (nach Mass) 17.15.04.
            additions.push("leib");
            additions.push("rumpf");
            additions.push("bandage");
        } else if lower.contains("leg garment") {
            additions.push("bein");
            additions.push("kompressionsbandage");
        } else if lower.contains("arm garment") || lower.contains("hand garment") {
            additions.push("armkompressionsstrumpf");
            additions.push("kompressionsbandage");
        } else if lower.contains("face garment")
            || lower.contains("head garment")
            || lower.contains("neck garment")
            || lower.contains("chin garment")
        {
            additions.push("kopf");
            additions.push("hals");
            additions.push("kompressionsbandage");
        } else if lower.contains("ear garment") {
            // Ear compression garments belong to the Kopf/Hals region family.
            additions.push("kopf");
            additions.push("hals");
            additions.push("kompressionsbandage");
        }
    }
    // German orthosis aliases (independent of the English gate):
    // Halskrawatte/Halskrause → Cervikalstütze (22.12); Gilchrist → shoulder
    // immobiliser belt (22.09). Note the correct normalized spelling
    // "schulterguertel" (Schultergürtel → ü becomes ue).
    if any_contains("halskrawatte") || any_contains("halskrause") {
        additions.push("cervikalstuetze");
    }
    if any_contains("gilchrist") {
        additions.push("schulterguertel");
        additions.push("immobilisierung");
        additions.push("gilchrist");
    }
    // Aerosol/nebuliser accessory stem: catch "aerosol*" (aerosoltherapy) and
    // any "nebuli*" form not in the fixed table. The VWR aerosol-fixative FP is
    // independently blocked by the ("14.01","fixative") negative keyword.
    if clean_words.iter().any(|w| w.starts_with("aerosol")) || any_contains("nebuli") {
        additions.push("vernebler");
        additions.push("aerosol");
    }
    // Ostomy / stoma → Material für Stoma- und Fistelversorgung (29.01).
    // "stomabandage" covers German Stomabandagen (Achim Ruthner); Gürtel are
    // explicitly listed in 29.01.01, and the 05.11 Limitation itself redirects
    // stoma carriers here. Do NOT use bare "stoma" (substring-hits "Stomach").
    if any_contains("ostomy")
        || has("colostomy")
        || has("urostomy")
        || has("ileostomy")
        || any_contains("stomabandage")
    {
        additions.push("stoma");
        additions.push("fistelversorgung");
    }
    // Urine / secretion drainage bags → Bein-/Bettbeutel (15.14 / 15.15) plus
    // their accessory positions. Requires a "bag" token to keep IVD urine
    // analyzers/reagents out. Rows with no region token default to Bettbeutel
    // (15.15.x) — verified: all such rows are urine-drainage products.
    // "leg bag" as a bigram is urology-exclusive, so accessory rows that never
    // say "urine" ("Leg Bag Sleeve", "Leg Bag Straps") still qualify.
    let urine_bag = (has("urine") || has("urinary")) && has("bag");
    let leg_bag = lower.contains("leg bag") || (urine_bag && has("leg"));
    if urine_bag || leg_bag {
        additions.push("sekret");
        if has("bed") {
            additions.push("bettbeutel");
        }
        if leg_bag {
            additions.push("beinbeutel");
            if has("sleeve") {
                additions.push("beinbeuteltasche"); // 15.14.99.01.1
            }
            if has("strap") || has("straps") {
                additions.push("haltebaender"); // 15.14.99.02.1 Haltebänder für Urinbeutel
                additions.push("urinbeutel");
            }
        }
        if has("hanger") || has("hangers") {
            additions.push("halterung"); // 15.15.99.01.1 Halterung/Befestigung für Bettbeutel
            additions.push("befestigung");
            additions.push("bettbeutel");
        }
        if !has("bed") && !leg_bag && !has("hanger") && !has("hangers") {
            additions.push("bettbeutel");
        }
    }
    // Insulin pen needles → 03.07.09.16.1 Penkanülen (safety variants may land
    // on 03.07.09.15.1 Sicherheits-Penkanülen — also correct).
    if has("pen") && (has("needle") || has("needles")) {
        additions.push("penkanuelen");
    }
    // Insulin disposable syringes → 03.07.10.10.1. Push only the insulin-specific
    // compound (split_words turns it into insulin + wegwerfspritzen); bare
    // "wegwerfspritzen" would risk drifting to the generic 03.07.10.15.1.
    if has("insulin") && (has("syringe") || has("syringes")) {
        additions.push("insulin-wegwerfspritzen");
    }
    // Retail absorbent incontinence products → 15.01. The !fecal guard is
    // mandatory: fecal-incontinence inserts belong to 15.40 Analtampon (see the
    // dedicated rule above) and must not be dragged into 15.01.
    // The German branches (any_contains: "Inkontinenzeinlagen" compounds,
    // HYGA; "Windelhosen"/"Vorlage", TZMO Seni — both tokens verified
    // TZMO-exclusive corpus-wide) need the same pushes — a lone
    // compound-decomposed "inkontinenz" keyword stays under the
    // single-keyword score threshold. The ch.03.07/22/23 "vorlage" negative
    // keywords remain as fences against the historical orthosis hops.
    if (has("incontinence")
        || any_contains("inkontinenz")
        || any_contains("windelhose")
        || any_contains("vorlage"))
        && !has("fecal")
    {
        additions.push("inkontinenz");
        additions.push("aufsaugende");
        additions.push("hilfsmittel");
    }
    // HANS HEPP first-aid refill plasters (+ ZCC Pflaster-Strip) → 35.01.10
    // Schnellverbände mit zentralem Wundkissen. Compound tokens only — bare
    // "pflaster" would hit Wärmepflaster and plaster-case rows.
    if any_contains("pflastersortiment")
        || any_contains("pflaster-sortiment")
        || any_contains("pflasterstrip")
        || any_contains("pflaster-strip")
        || any_contains("kinderpflaster")
        || any_contains("schnellverband")
    {
        additions.push("schnellverbaende");
        additions.push("wundkissen");
        additions.push("vlies");
    }
    // HANS HEPP plain Wundkompressen → 35.01.01 Falt-/Vlieskompressen.
    // ("wundkompressen" verified single-company corpus-wide; the impregnated/
    // coated 35.01.02 family stays reachable for "beschichtet" products via
    // its own keywords.)
    if any_contains("wundkompressen") {
        additions.push("falt");
        additions.push("vlieskompressen");
    }
    // CGM stragglers whose text says "Continuous/Flash Glucose Monitoring" but
    // never "sensor" (SIBIONICS) or that are the reader unit (Abbott Libre).
    // The gate is tight (glucose AND monitoring AND continuous|flash) so the
    // pushed "sensoren" cannot re-open the 21.07.02 magnet for non-CGM devices.
    if has("glucose") && has("monitoring") && (has("continuous") || has("flash")) {
        if has("reader") {
            additions.push("lesegeraet"); // 21.07.01.00.1
        } else {
            additions.push("sensoren"); // 21.07.02.00.1
            additions.push("glukose");
        }
    }
    // Breast pumps → 01.01 Milchpumpen (electric vs manual split).
    if has("breast") && (has("pump") || has("pumps")) {
        if has("electric") || has("electrical") {
            additions.push("einzelmilchpumpe"); // 01.01.02.00.1
            additions.push("elektrisch");
        } else {
            additions.push("milchpumpe"); // 01.01.01.00.1
            additions.push("handbetrieben");
        }
    }
    // Blood-ketone test strips → 21.03.01.03.1 Reagenzträger für
    // Blutketonbestimmungen. Control solutions are fenced off by the
    // ("21.03","control solution") negative keyword.
    if has("ketone") && (has("blood") || has("strip") || has("strips")) {
        additions.push("blutketonbestimmungen");
        additions.push("reagenztraeger");
    }
    // Simple arm slings → 05.10 Armtraggurten (the exact MiGeL position).
    // Device texts carry singular "Armtraggurt" or EN "arm sling"; the MiGeL
    // keyword is the plural "Armtraggurten" which word-level matching never
    // fires on. Without this, the generic sling→schulterorthese enrichment
    // mis-codes them to 23.10 Rumpf-Orthesen. armtraggurten's high IDF
    // (3 MiGeL items) outranks the generic schulterorthese candidates.
    if any_contains("armtraggurt")
        || ((has("arm") || any_contains("forearm")) && (has("sling") || has("slings")))
    {
        additions.push("armtraggurten");
    }
    // Cast / post-OP / offloading shoes sit on 26.01.04 "Spezialschuhe für
    // Verbände", not "... für Orthesen" (.01). The 'shoe' EN_DE entry already
    // pushes "spezialschuhe"; adding "verbaende" makes the Verbände sub-position
    // outrank the Orthesen one. Trigger tokens are Span-Link/Thuasne-exclusive.
    if (has("shoe") || has("shoes"))
        && (has("cast")
            || has("post-op")
            || has("postop")
            || any_contains("offloading")
            || any_contains("podo-med"))
    {
        additions.push("verbaende");
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
    // Unterschenkel-Orthesen (23.03 Massorthesen) should NOT match compression
    // stockings (chapter 17), arm products, or wrist products.
    ("23.03", "kompressionsstrumpf"),
    ("23.03", "armkompressionsstrumpf"),
    ("23.03", "ulcus"),
    ("23.03", "ulcer"),
    ("23.03", "handgelenk"),
    ("23.03", "poignet"),
    ("23.03", "polso"),
    ("23.03", "schulter"),
    ("23.03", "epaule"),
    ("23.03", "spalla"),
    ("23.03", "thermacare"),
    // Lumbar/Wirbelsäulen-Orthesen (22.13/22.14/22.15) should NOT match
    // CSF drainage catheters, lumbar punction needles, or AI imaging products.
    ("22.13", "catheter"),
    ("22.13", "katheter"),
    ("22.13", "drainage"),
    ("22.13", "punction"),
    ("22.13", "x-ray"),
    ("22.13", "ai "),
    ("22.13", "lunit"),
    ("22.14", "catheter"),
    ("22.14", "katheter"),
    ("22.14", "drainage"),
    ("22.14", "punction"),
    ("22.15", "catheter"),
    ("22.15", "katheter"),
    ("22.15", "drainage"),
    ("22.15", "punction"),
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
    // Transfer-Set should NOT match dental impression copings (Nobel Biocare etc.)
    ("03.07.09.20", "impression"),
    ("03.07.09.20", "coping"),
    ("03.07.09.20", "abutment"),
    ("03.07.09.20", "branemark"),
    ("03.07.09.20", "brånemark"),
    ("03.07.09.20", "implant"),
    ("03.07.09.20", "abformpfosten"),
    ("03.07.09.20", "model tools"),
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
    ("23.21", "gum"), // dental brand GUM ≠ hand orthosis
    // --- Arm-Kompressionsbandage (17.15) should NOT match torso compression
    // bras or powered sequential-compression (DVT) pumps. The unconditional
    // `compression`->`kompressionsbandage` enrichment is the only signal; a bra
    // is a torso garment and an SCD pump is apparatus, not a custom arm bandage.
    // A compression bra / sequential-compression (DVT) pump is not a
    // measured-textile Kompressionsbandage. Block at chapter level (17 and 05)
    // so the `compression`->`kompressionsbandage` enrichment cannot relocate it
    // to another wrong sub-code (e.g. Arm 17.15.03 or Hüft 05.06.02). Phrase
    // form avoids colliding with the substring "bra" inside "brace"/"abrasion".
    ("17", "compression bra"),
    ("05", "compression bra"),
    ("17", "sequential compression"),
    ("05", "sequential compression"),
    // --- Transfer-Set (03.07.09.20) should NOT match patient-transfer furniture
    // (hoverboards/stretchers/transfer chairs) or dental tray-transfer copings.
    // The single generic keyword 'transfer' otherwise meets the threshold.
    ("03.07.09.20", "hoverboard"),
    ("03.07.09.20", "stretcher"),
    ("03.07.09.20", "blower"),
    ("03.07.09.20", "zephyr"),
    ("03.07.09.20", "fauteuil"),
    ("03.07.09.20", "disque"),
    ("03.07.09.20", "cadre"),
    ("03.07.09.20", "tray"),
    ("03.07.09.20", "prosthetic"),
    // --- Insulin pen WITHOUT cannula (03.05.03) should NOT match pen needles ---
    ("03.05.03", "needle"),
    ("03.05.03", "pennadel"),
    ("03.05.03", "nadel"),
    // --- Orthosis chapter 23 should NOT match blood-pressure monitors or the
    // AGFA 'Ortho' radiography screen-film (wrist/ortho enrichment collisions) ---
    ("23", "blood pressure"),
    ("23", "blutdruck"),
    // General rule: catheters, CPAP brands and orthodontics are never orthoses.
    // Scoped to the orthosis chapters (22/23) so a blocked sub-code FP cannot
    // relocate to a neighbouring orthosis position (e.g. SOPHYSA CSF catheter,
    // Air Liquide Respireo CPAP elbow, Ormco orthodontic pad).
    ("22", "catheter"),
    ("22", "katheter"),
    ("23", "catheter"),
    ("23", "katheter"),
    ("22", "respireo"),
    ("23", "respireo"),
    ("22", "ormco"),
    ("23", "ormco"),
    // --- Cervikalstütze (22.12) should NOT match dental cervical matrices,
    // orthodontic headgear, spinal torque-limiters or neurosurgical skull clamps ---
    ("22.12", "matrices"),
    ("22.12", "matryce"),
    ("22.12", "torque"),
    ("22.12", "doro"),
    ("22.12", "headrest"),
    ("22.12", "britegear"),
    ("22.12", "ormco"),
    // --- Orthosis chapter 23 should NOT match surgical levers/spreaders
    // (DE compound stems) or CPAP/NO breathing-circuit 'elbow' connectors ---
    ("23", "spreizzange"),
    ("23", "schulterhebel"),
    ("23", "huefthebel"),
    ("23.23", "respireo"),
    ("23.23", "anti-asphyxia"),
    ("23.23", "vented"),
    // --- Alginate, steril wound dressing (35.05.06) should NOT match dental
    // impression alginate (bare ingredient word 'alginate' collision) ---
    ("35.05.06", "dental"),
    ("35.05.06", "impression"),
    // --- Superabsorber, steril (35.05.05) is a sterile WOUND dressing. Incontinence
    // underwear (Hartmann MoliCare etc.) is absorbent but belongs to MiGeL ch. 15
    // (Inkontinenzhilfen), not sterile wound care — the 'absorbent' enrichment leaks. ---
    ("35.05", "incontinence"),
    ("35.05", "underwear"),
    ("35.05", "molicare"),
    // --- MRI radiofrequency coils (Invivo, Philips, Shenzhen RF Tech) name body
    // parts ("SENSE Knee Coil", "8CH FOOT ANKLE COIL", "HD 8CH WRIST ARRAY") and
    // leak into orthosis chapters 22/23 via the body-part enrichment. No genuine
    // orthosis is a "coil" or an "array" (MRI channel-count arrays). ---
    ("23", "coil"),
    ("22", "coil"),
    ("23", "array"),
    ("22", "array"),
    // --- Sensoren (21.07.02) is for diabetic continuous-glucose sensors (Medtronic
    // Guardian, Abbott FreeStyle Libre). Patient-monitor sensors — capnography
    // (CO2/flow/Capnostat), pulse-oximetry (SpO2), temperature — are NOT MiGeL. ---
    ("21.07.02", "co2"),
    ("21.07.02", "spo2"),
    ("21.07.02", "flow sensor"),
    ("21.07.02", "capnostat"),
    ("21.07.02", "mainstream"),
    ("21.07.02", "temperature"),
    // --- Smaller targeted collisions ---
    ("15.16", "male condom"), // contraceptive condoms ≠ urinal condoms
    ("15.16", "non-medicated"),
    ("09.03", "external"), // standalone AEDs ≠ wearable defibrillator vest
    ("09.03", "monitor"),
    ("09.03", "paper"),      // defibrillator recording paper ≠ defib vest
    ("09.03", "heartstart"), // Philips HeartStart AED ≠ wearable defib vest
    ("09.03", "implantable cardioverter"), // Boston Sci ICD ≠ wearable defib vest
    ("09.03", "resynchronization"), // Boston Sci CRT-D ≠ wearable defib vest
    ("21.01", "dreamstation"), // CPAP modem/wifi accessory w/ oximetry ≠ pulse oximeter
    ("21.07.02", "respiratory effort"), // Pro-Tech/Respironics PSG sensors ≠ CGM sensor
    ("21.07.02", "piezo"),   // piezo respiratory-effort sensor ≠ CGM sensor
    ("21.07.02", "emg"),     // Edan EMG/stimulation monitor sensor ≠ CGM sensor
    ("21.07.02", "nmt"),     // Edan neuromuscular-transmission sensor ≠ CGM sensor
    ("21.07.02", "stimulation"), // Edan EMG/stimulation sensor ≠ CGM sensor
    ("23", "oximeter"),      // Edan finger/pulse oximeter ≠ finger orthosis (ch.23)
    ("22", "spo2"),          // MIPM SpO2 finger adapter ≠ finger orthosis (hops 23->22.06)
    ("23", "spo2"),          // MIPM SpO2 finger adapter ≠ finger orthosis (ch.23)
    ("23", "table"),         // Pivotal powered treatment table ≠ shoulder orthosis (ch.23)
    ("03.07", "windel"),     // TZMO Seni diaper "geschlossenes System" ≠ closed infusion system
    ("03.07", "vorlage"), // TZMO Seni incontinence Vorlage (Klettverschluss) ≠ infusion-tube fixation
    ("22", "vorlage"), // TZMO Seni incontinence brief ≠ orthosis (hops 03.07->22.04); ch.15 Vorlagen kept
    ("23", "vorlage"), // TZMO Seni incontinence brief ≠ orthosis; ch.15 Vorlagen kept
    ("35.05", "bettschutz"), // TZMO Seni bed underpad ≠ sterile wound superabsorber
    ("05.11", "abdominal belt"), // fetal-monitoring CTG belt ≠ Leib-/Rumpf-Bandage
    ("35.06", "plating"), // histology silver-plating kit ≠ silver alginate dressing
    ("05.14", "catheter"), // SOPHYSA lumbar catheter ≠ Lumbal-Bandage
    ("05.14", "katheter"),
    ("14.01", "fixative"),    // VWR aerosol fixative ≠ Vernebler
    ("99.30.06", "staender"), // infusion stands/holders ≠ Schlitzkompresse-Set
    ("99.30.06", "halter"),
    ("03.07", "trocar"), // surgical trocar kits ≠ infusion sets/stands
    ("99.30.06", "bottle"),
    ("99.30.06", "cuff"),
    ("22.03", "cadre"), // walking frame ≠ Fussheber-Orthese
    ("22.03", "marche"),
    ("01.03", "connection hose"), // suction connection hoses ≠ Spülschlauch
    // --- Jul 2026 audit additions ---
    // Margomed gravity IV administration sets ≠ 99.30.06.02.1 (that code requires
    // a complete sterile home-infusion kit). Deliberately scoped narrow: the
    // desired hop target IS 03.07.01.* (Infusionsschlauch mit Tropfenregler).
    ("99.30.06", "margomed"),
    // Bare FR "gaze" (plain square gauze) ≠ 35.01.12 Augenkompressen (eye-shape
    // only). Hop to 35.01.01 Falt-/Vlieskompressen is the correct family.
    ("35.01.12", "gaze"),
    // Knitted finger sleeves (IVF Hartmann Tricot Fingerling) are a genuine
    // MiGeL device at 35.01.14.11.1 "Fingerlinge Stoff/Leder" — never an
    // orthosis. Block the whole 22/23 hop-target family; 35.01.14 stays open.
    ("22", "fingerling"),
    ("23", "fingerling"),
    // Adhesive surgical dressings ≠ 35.07 medical-honey codes (require >60%
    // Honig). Hop to 35.01.10 Schnellverbände is the correct family.
    ("35.07", "chirurgical"),
    ("35.07", "surgical"),
    ("35.07", "chirurgisch"),
    // DIN-61634 fixation bandages ≠ 17.30 Kurzzug/Langzug compression bandages
    // (dimensions don't match). Correct home is the elastic fixation bandage
    // family 35.01.07.01-.07 "Elastische (Ideal-)Binden, Fixation gedehnt" —
    // block the kohäsiv sub-series (.20-.25) too, whose shorter keyword list
    // otherwise outranks it (a plain Fixierbinde is not cohesive).
    ("17.30", "fixierbinde"),
    ("35.01.07.2", "fixierbinde"),
    // Blister plasters ≠ 35.05 Superabsorber (leak in via the
    // absorbent→superabsorber enrichment, which must stay — Huizhou Foryou
    // superabsorbent dressings depend on it).
    ("35.05", "plaster"),
    ("35.05", "pflaster"),
    // Blood-ketone CONTROL solutions ≠ 21.03 Reagenzträger (test strips only).
    ("21.03", "control solution"),
    // Plain square gauze ≠ 35.01.05 Stillkompressen (second hop after the
    // 35.01.12 Augenkompressen block; correct home is 35.01.01 Faltkompressen).
    ("35.01.05", "gaze"),
    // Tricot (knitted fabric) finger sleeves belong to 35.01.14.11 Fingerlinge
    // Stoff/Leder — the .10 Gummi and .12 Netz siblings' shorter keyword lists
    // otherwise outrank it on the shared "fingerlinge" keyword.
    ("35.01.14.10", "tricot"),
    ("35.01.14.12", "tricot"),
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
        .replace('é', "e")
        .replace('É', "E")
        .replace('è', "e")
        .replace('È', "E")
        .replace('ê', "e")
        .replace('Ê', "E")
        .replace('à', "a")
        .replace('À', "A")
        .replace('â', "a")
        .replace('Â', "A")
        .replace('ù', "u")
        .replace('Ù', "U")
        .replace('û', "u")
        .replace('Û', "U")
        .replace('ô', "o")
        .replace('Ô', "O")
        .replace('î', "i")
        .replace('Î', "I")
        .replace('ç', "c")
        .replace('Ç', "C")
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
                    category_texts[i] = bezeichnung.lines().next().unwrap_or("").trim().to_string();
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
            let cat_text = category_texts
                .iter()
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
pub fn build_search_index(
    items: &[MigelItem],
) -> Result<MigelSearchIndex, Box<dyn std::error::Error>> {
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
        .build(&patterns)?;

    Ok(MigelSearchIndex {
        automaton,
        pattern_items,
        idf_weights,
    })
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
    // Body-part orthosis compounds: German products name the body part as a
    // compound prefix (Knieschiene → knie + schiene), which the suffix path
    // does not catch. min-remainder 6 ("schiene"=7) prevents false splits.
    ("knie", 6),          // Knieschiene → knie + schiene
    ("ellenbogen", 6),    // Ellenbogenschiene → ellenbogen + schiene
    ("sprunggelenk", 6),  // Sprunggelenkorthese → sprunggelenk + orthese
    ("unterschenkel", 6), // Unterschenkelorthese → unterschenkel + orthese
    ("finger", 6),        // Fingerschiene → finger + schiene
    ("inkontinenz", 6),   // Inkontinenzeinlage → inkontinenz + einlage (all
                          // 'inkontinenz' compounds DB-wide are genuine products)
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
    let total_idf: f64 = keywords
        .iter()
        .map(|k| {
            let idf_w = idf.get(k.as_str()).copied().unwrap_or(1.0);
            k.len() as f64 * idf_w
        })
        .sum();
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
    let idf_score = if total_idf > 0.0 {
        matched_idf / total_idf
    } else {
        0.0
    };
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
    // AGFA = radiography/imaging vendor only; never a MiGeL patient aid. The
    // `ortho`/`screen film` text otherwise collides with orthosis codes.
    &["agfa"],
    // CSF / neurosurgical drainage catheters (interventional, not patient aids).
    &["lumbar", "catheter"],
    &["ventricular", "catheter"],
    // Respironics CPAP/ventilation mask connectors: a "nebulizing/bronchoscopy
    // elbow" is a tube fitting on an oro-nasal mask, never a MiGeL device. The bare
    // word "elbow" otherwise hops across orthosis chapters (22/23) and "nebulizing"
    // collides with nebulizers (ch.14); the AND-pair pins it precisely.
    &["nebulizing", "elbow"],
    &["bronchoscopy", "elbow"],
    // Laboratory histology/microbiology staining reagents (Merck: Lugol's solution
    // for Gram staining, Lactophenol blue for staining fungi) are IVD lab solutions,
    // never a MiGeL blocker/irrigation solution. "staining" never denotes a MiGeL device.
    &["staining"],
    // --- Jul 2026 audit additions ---
    // Macom "Full Body Garment" post-liposuction suits: no Ganzkörper position
    // exists anywhere in MiGeL; the garment region routing would otherwise pull
    // them into 05.11 Leib-/Rumpf-Bandage. Region-specific garments unaffected.
    &["full body", "garment"],
    // OBA foam positioning aids ≠ 17.30.15 Pelotte (Schaumstoff homonym). OBA
    // itself stays unexcluded — it has genuine nebulizer matches.
    &["positionierungshilfe", "schaumstoff"],
    // Cervical/lumbar traction devices (Aspen ComforTrac): MiGeL has zero
    // traction positions; a chapter-scoped block would hop via the body-part
    // enrichment. ('extraction' bleed only touches surgical rows — harmless.)
    &["traction"],
    // Phlebotomy chairs ('Entnahmestuhl' shares the 'entnahme' compound
    // fragment with 03.07 Entnahmespike).
    &["entnahmestuhl"],
    // Reusable hot/cold gel compresses: genuine MiGeL 16.01 positions exist,
    // but the matcher has no ch.16 path today and the FR 'cheville' keyword
    // mis-routes them to 05.02. LIFT this exclusion if a 16.01 recall rule is
    // ever added.
    &["hot", "cold"],
    // Orthobroker LSO spare front panels ≠ 05.14 Lumbal-Bandage. The 'lumbo'
    // AND-term protects Aspen's 9 correct "COLLAR FRONT PANEL" rows.
    &["front panel", "lumbo"],
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

/// Accessory stop-list for the Respironics home-ventilator forced matches:
/// circuits, tubing, humidifiers, stands etc. are accessories, not the
/// rentable 14.12.02 base unit.
const VENT_ACCESSORY_STOPS: &[&str] = &[
    "cable",
    "case",
    "bag",
    "stand",
    "battery",
    "mount",
    "power supply",
    "nurse call",
    "adapter",
    "adaptor",
    "circ",
    "tubing",
    "humidifier",
    "accessor",
    "filter",
    "mask",
];

/// Accessory stop-list for the Respironics PAP (CPAP/BiPAP) forced matches.
/// Must cover the same accessory classes as the ventilator list: a row vetoed
/// by a specific rule (e.g. "bipap a40" + circuit) falls through to the later
/// bare-"bipap" rule, so the stop tokens have to hold there too. "circ" covers
/// circuit/circuits and the abbreviated "Disp Circ".
const PAP_ACCESSORY_STOPS: &[&str] = &[
    "cable",
    "cord",
    "stand",
    "battery",
    "modem",
    "wi-fi",
    "wifi",
    "case",
    "serial",
    "spo2",
    "link",
    "circ",
    "tubing",
    "humidifier",
    "accessor",
    "filter",
    "mask",
];

/// Curated recall rules: (all_of, none_of, position_nr).
/// If a product's RAW combined text (pre-enrichment, normalized + lowercased,
/// including the company name) contains every `all_of` substring and none of
/// the `none_of` substrings, the row is pinned directly to `position_nr`,
/// bypassing keyword scoring. First matching rule wins, so order matters
/// (e.g. "bipap a30/a40" ventilators before the bare "bipap" PAP rule).
///
/// This is the recall-side counterpart of UNIVERSAL_EXCLUSIONS: use it for
/// verified brand/category-exclusive tokens where the heuristic scorer cannot
/// reach the correct position — either because the MiGeL Bezeichnung is too
/// verbose (score dilution: "Krücken für Erwachsene, ergonomischer Griff"
/// leaves any single matched keyword under the 0.5 threshold) or because IDF
/// ranking would drift to a sibling code. Every trigger below was verified
/// against the full UDI corpus to hit only the intended rows.
const FORCED_MATCHES: &[(&[&str], &[&str], &str)] = &[
    // GCE MediSelect II / MediReg II medical-O2 cylinder regulators (brand-
    // exclusive tokens; GCE's MediConnect/Medimeter hospital-pipeline lines
    // don't carry them). Rental position incl. maintenance.
    (&["mediselect"], &[], "14.10.42.00.2"),
    (&["medireg"], &[], "14.10.42.00.2"),
    // German crutches ("Unterarmgehstützen", REBOTEC): MiGeL Bezeichnung says
    // only "Krücken", so no organic keyword path exists.
    (&["gehstuetze"], &[], "10.01.01.00.1"),
    // Corrective contact lenses → 25.01.01 Brillen/Kontaktlinsen (chapter 25
    // has no organic keyword path). Bigram/substring never matches accessories
    // (solutions, cases) — verified corpus-wide.
    (&["contact lens"], &[], "25.01.01.00.1"),
    (&["kontaktlinse"], &[], "25.01.01.00.1"),
    // Respironics home ventilators → 14.12.02 Heimbeatmungsgerät, Miete.
    // The Bezeichnung compound "Heimbeatmungsgerät" is unreachable via the
    // ventilator→beatmungsgeraet enrichment. MUST come before the PAP rules:
    // "BiPAP A30/A40" are ventilators, bare "bipap" is a PAP device.
    (&["trilogy"], VENT_ACCESSORY_STOPS, "14.12.02.00.2"),
    (&["garbin"], VENT_ACCESSORY_STOPS, "14.12.02.00.2"),
    (&["bipap a30"], VENT_ACCESSORY_STOPS, "14.12.02.00.2"),
    (&["bipap a40"], VENT_ACCESSORY_STOPS, "14.12.02.00.2"),
    (
        &["ventilator", "respironics"],
        VENT_ACCESSORY_STOPS,
        "14.12.02.00.2",
    ),
    // Respironics PAP devices. AutoSV = servo-ventilation (14.11.03); all other
    // brand-token rows are CPAP/BiPAP base units with humidification (14.11.02).
    // All trigger tokens verified Respironics-exclusive corpus-wide.
    (&["autosv"], PAP_ACCESSORY_STOPS, "14.11.03.00.2"),
    (&["dreamstation"], PAP_ACCESSORY_STOPS, "14.11.02.00.2"),
    (&["system one"], PAP_ACCESSORY_STOPS, "14.11.02.00.2"),
    (&["remstar"], PAP_ACCESSORY_STOPS, "14.11.02.00.2"),
    (&["dorma"], PAP_ACCESSORY_STOPS, "14.11.02.00.2"),
    (&["bipap"], PAP_ACCESSORY_STOPS, "14.11.02.00.2"),
    // MIR handheld spirometers → 21.01.15 Portables Spirometriegerät (score
    // dilution: "Portables Spirometriegerät (inkl. Mundstück)"). "spirometer"
    // as substring misses accessory rows (they say "spirometry"); the
    // "smart one" bigram is guarded against ostomy "one-piece" products.
    (&["spirobank"], &[], "21.01.15.00.1"),
    (&["spirometer"], &[], "21.01.15.00.1"),
    (&["smart one"], &["piece"], "21.01.15.00.1"),
    // Insulet Omnipod patch pumps → 03.02.01 Insulinpumpen-System (Bezeichnung
    // explicitly anticipates patch pumps; PodPals overlays never carry the token).
    (&["omnipod"], &[], "03.02.01.00.2"),
    // SIGVARIS Doff'N Donner donning aid → 17.12.01.01.1 Rollmanschetten.
    (&["doff"], &[], "17.12.01.01.1"),
    // --- IVF Hartmann DermaPlast retail line (audit §2b; all trigger tokens
    // verified single-company corpus-wide). Sizes aren't text-derivable, so
    // the audit's representative size position is pinned. Never bare
    // "dermaplast" (nasal sprays) or bare "conviva" (STE Pharma sea water). ---
    // Compress Gel/Plus/Protect → beschichtete Wundkompresse. Also corrects
    // the one Gel row that previously drifted to 99.30.03 (M-Plast).
    (&["dermaplast compress"], &[], "35.01.02.02.1"),
    // Sparablanc (transparent + textile) plaster spools → Heft-/Fixier-Pflaster.
    (&["sparablanc"], &[], "35.01.09.03.1"),
    // Combifix elastic cohesive gauze bandage → Gazebinden elastisch, kohäsiv.
    (&["combifix"], &[], "35.01.06.12.1"),
    // Coop Conviva Protect+ waterproof sterile dressing → Schnellverbände.
    (&["conviva protect"], &[], "35.01.10.12.1"),
    // --- HANS HEPP first-aid refills (audit §2b) ---
    // (Heft-)Pflasterspulen → Heft-/Fixier-Pflaster spools ("pflasterspule"
    // is a substring of "heftpflasterspule", one rule covers both).
    (&["pflasterspule"], &[], "35.01.09.03.1"),
    // --- SIGVARIS Inc. MAK wraps (audit §2b): 17.06 Medizinisch adaptives
    // Kompressionssystem is literally this product type. All trigger tokens
    // verified SIGVARIS-Inc.-exclusive (245 rows) and absent from the GTIN
    // override DB. Order matters: extender + Compreboot before the generic
    // Compreflex default. ---
    // Strap Extender → the Zubehör position (Extensionsbinde) names exactly this.
    (&["compreflex strap extender"], &[], "17.06.01.10.1"),
    // Compreboot → Fuss.
    (&["compreboot"], &[], "17.06.01.01.1"),
    // Coolflex Standard Calf → Wade (region provable from the name).
    (&["coolflex"], &["extender"], "17.06.01.02.1"),
    // Generic "Compreflex" rows carry zero body-region text (the line includes
    // arm/thigh/foot variants) → Wade as the accepted modal default. Worst
    // case = wrong sub-position inside the correct 17.06 chapter.
    (&["compreflex"], &["extender"], "17.06.01.02.1"),
    // --- Walker boots (audit §2b, maintainer decision 02.07.2026): prefab
    // ankle-immobilization walkers are FERTIGORTHESEN → 22.02.04 "Sprunggelenk-
    // Orthese zur Immobilisierung, definierte Position", NOT ch.23 MASSORTHESEN
    // (custom-made, SVOT/OSM tariff). Covers Span Link (Actimove/DonJoy/
    // Thuasne/b:joynz/ROM/Air Pump), Ruthner Smartwalker ("walker" is a
    // substring), and re-codes the Aspen TRAVERSE + Orthobroker BraceID rows
    // that previously sat in 23.02. Liners/wedges ride along with the system.
    // Guards: ABLE exoskeleton ("human motion"), GAUKE kit ("first aid"),
    // REBOTEC YANO-Walker ("yano") — plus defensive walking-frame tokens,
    // since US-English "walker" also means Gehwagen/Rollator.
    (
        &["walker"],
        &["human motion", "first aid", "yano", "gehwagen", "rollator", "gehgestell", "walking frame"],
        "22.02.04.00.1",
    ),
];

/// Check the curated forced-match rules against the raw (pre-enrichment)
/// combined text. Returns the pinned MiGeL item if a rule fires and its
/// position exists in the current XLSX (else falls through to the heuristic).
fn find_forced_match<'a>(
    raw_combined: &str,
    migel_items: &'a [MigelItem],
) -> Option<&'a MigelItem> {
    for &(all_of, none_of, position_nr) in FORCED_MATCHES {
        if all_of.iter().all(|t| raw_combined.contains(t))
            && !none_of.iter().any(|t| raw_combined.contains(t))
        {
            if let Some(item) = migel_items.iter().find(|m| m.position_nr == position_nr) {
                return Some(item);
            }
        }
    }
    None
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
    device_type: &str,
    risk_class: &str,
    migel_items: &'a [MigelItem],
    search_index: &MigelSearchIndex,
) -> Option<&'a MigelItem> {
    // Step -1: curated forced matches on the RAW text (pre-enrichment, so the
    // rules can't be triggered by enrichment side effects). Highest priority:
    // these are verified brand/category-exclusive pins and deliberately outrank
    // the metadata gate below (e.g. Omnipod 5 is CLASS_III yet genuine MiGeL).
    let raw_combined =
        normalize_german(&format!("{} {} {} {}", desc_de, desc_fr, desc_it, brand)).to_lowercase();
    if let Some(item) = find_forced_match(&raw_combined, migel_items) {
        return Some(item);
    }

    // Step -0.5: hard metadata gate — IVD and Class III devices never reach
    // the heuristic matcher, immunizing them against keyword drift.
    if is_metadata_excluded(device_type, risk_class) {
        return None;
    }

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
            let (score_de, max_len_de, count_de, idf_de) =
                keyword_score(&de_words, &item.keywords_de, true, true, idf);
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
            let significant_words = de_words.iter().filter(|w| w.len() >= 4).count().max(1) as f64;
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

    // Sort by IDF score descending, then max_len descending, then position_nr
    // ascending. The final tiebreak is essential for determinism: candidates
    // come from a HashSet, so exact score ties (common between sibling
    // positions like Kauf/Miete variants) would otherwise flip randomly
    // between runs, producing phantom diffs in the daily output.
    passing.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(b.2.cmp(&a.2))
            .then(
                migel_items[a.0]
                    .position_nr
                    .cmp(&migel_items[b.0].position_nr),
            )
    });

    // Return the best-ranked candidate
    passing.first().map(|&(idx, _, _, _)| &migel_items[idx])
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Golden-set regression test: 279 rows sampled from the audited &
    /// verified 02.07.2026 matcher output (2 exemplars per company × code
    /// family), plus every adversarially-confirmed false-positive cluster as
    /// expected-NONE, the excluded-company list, deliberate never-match
    /// exemplars (hearing aids, surgical, dental, imaging, IVD, stents, MRI
    /// coils), and the curated forced-match pins. Runs against the pinned
    /// MiGeL XLSX in tests/fixtures/ so BAG list updates can't shift results.
    ///
    /// GTIN-override-layer rows (SIGVARIS shop DB) are deliberately absent —
    /// they never reach `find_best_migel_match`.
    ///
    /// If this test fails after an intentional rule change: inspect every
    /// listed row, confirm each delta is intended, and regenerate the fixture
    /// from a verified run.
    #[test]
    fn golden_set() {
        let xlsx = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/migel.xlsx");
        let items = parse_migel_items(xlsx).expect("parse pinned MiGeL XLSX fixture");
        let index = build_search_index(&items).expect("build search index");
        let tsv = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/golden_set.tsv"
        ));

        let mut failures: Vec<String> = Vec::new();
        let mut total = 0;
        for line in tsv.lines().skip(1) {
            if line.trim().is_empty() {
                continue;
            }
            let f: Vec<&str> = line.split('\t').collect();
            assert!(f.len() >= 8, "malformed golden row: {}", line);
            let (de, fr, it, brand, dtype, risk, expected, note) =
                (f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[7]);
            total += 1;

            // Replicate the company-exclusion gate applied by run_migel/gui
            // before the matcher is invoked.
            let got_code = if EXCLUDED_COMPANIES.contains(&brand) {
                "NONE".to_string()
            } else {
                find_best_migel_match(de, fr, it, brand, dtype, risk, &items, &index)
                    .map(|m| m.position_nr.clone())
                    .unwrap_or_else(|| "NONE".to_string())
            };

            if got_code != expected {
                let snippet: String = de.chars().take(90).collect();
                failures.push(format!(
                    "[{}] brand={} expected={} got={} | {}",
                    note, brand, expected, got_code, snippet
                ));
            }
        }

        assert!(
            failures.is_empty(),
            "{} of {} golden rows failed:\n{}",
            failures.len(),
            total,
            failures.join("\n")
        );
    }
}
