//! X / Twitter image posting for swissdamed2sqlite.
//!
//! Posts a tweet with the MiGeL stats chart. Media upload uses the v2
//! `/2/media/upload` endpoint with OAuth 1.0a; the tweet itself is created via
//! the v2 `/2/tweets` endpoint, also OAuth 1.0a-signed.
//!
//! Credentials are read from `twitter_credentials.json` (cwd, then $HOME):
//!   {"consumer_key":"...","consumer_secret":"...","token":"...","secret":"..."}
//! If absent, falls back to the first profile in `~/.twurlrc` (the `twurl` CLI
//! config), reusing the same OAuth 1.0a key set.

use hmac::{Hmac, Mac};
use rusqlite::Connection;
use serde::Deserialize;
use sha1::Sha1;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha1 = Hmac<Sha1>;

#[derive(Deserialize, Clone)]
pub struct Creds {
    pub consumer_key: String,
    pub consumer_secret: String,
    pub token: String,
    pub secret: String,
}

fn find_file(name: &str) -> Option<PathBuf> {
    let cwd = PathBuf::from(name);
    if cwd.exists() {
        return Some(cwd);
    }
    std::env::var_os("HOME")
        .map(|h| PathBuf::from(h).join(name))
        .filter(|p| p.exists())
}

fn parse_twurlrc(text: &str) -> Option<Creds> {
    let field = |key: &str| -> Option<String> {
        text.lines()
            .map(str::trim)
            .find_map(|l| l.strip_prefix(key).and_then(|r| r.strip_prefix(':')))
            .map(|v| v.trim().trim_matches('"').to_string())
            .filter(|s| !s.is_empty())
    };
    Some(Creds {
        consumer_key: field("consumer_key")?,
        consumer_secret: field("consumer_secret")?,
        token: field("token")?,
        secret: field("secret")?,
    })
}

fn load_creds() -> Result<Creds, Box<dyn Error>> {
    if let Some(p) = find_file("twitter_credentials.json") {
        eprintln!("[twitter] Using credentials: {}", p.display());
        return Ok(serde_json::from_str(&fs::read_to_string(&p)?)?);
    }
    if let Some(home) = std::env::var_os("HOME") {
        let rc = PathBuf::from(home).join(".twurlrc");
        if rc.exists() {
            eprintln!("[twitter] Using credentials: {}", rc.display());
            return parse_twurlrc(&fs::read_to_string(&rc)?)
                .ok_or_else(|| "could not parse OAuth fields from ~/.twurlrc".into());
        }
    }
    Err("no Twitter credentials found (twitter_credentials.json or ~/.twurlrc)".into())
}

fn enc(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 2);
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

fn nonce() -> String {
    let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let pid = std::process::id();
    format!("{:x}{:x}{:x}", t.as_nanos(), pid, t.subsec_nanos())
}

fn auth_header(creds: &Creds, method: &str, url: &str) -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .to_string();
    let nonce = nonce();
    let mut params: Vec<(&str, &str)> = vec![
        ("oauth_consumer_key", &creds.consumer_key),
        ("oauth_nonce", &nonce),
        ("oauth_signature_method", "HMAC-SHA1"),
        ("oauth_timestamp", &ts),
        ("oauth_token", &creds.token),
        ("oauth_version", "1.0"),
    ];
    params.sort_by(|a, b| a.0.cmp(b.0));

    let param_str = params
        .iter()
        .map(|(k, v)| format!("{}={}", enc(k), enc(v)))
        .collect::<Vec<_>>()
        .join("&");
    let base = format!("{}&{}&{}", method, enc(url), enc(&param_str));
    let key = format!("{}&{}", enc(&creds.consumer_secret), enc(&creds.secret));

    let mut mac = HmacSha1::new_from_slice(key.as_bytes()).expect("hmac key");
    mac.update(base.as_bytes());
    let sig = {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
    };

    let mut header_params = params.clone();
    let sig_pair = ("oauth_signature", sig.as_str());
    header_params.push(sig_pair);
    header_params.sort_by(|a, b| a.0.cmp(b.0));
    let inner = header_params
        .iter()
        .map(|(k, v)| format!("{}=\"{}\"", enc(k), enc(v)))
        .collect::<Vec<_>>()
        .join(", ");
    format!("OAuth {}", inner)
}

fn format_thousands(n: i64) -> String {
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

/// Build a compact tweet caption (well under 280 chars) from the MiGeL DB.
fn build_caption(migel_db: &Path) -> String {
    let conn = match Connection::open(migel_db) {
        Ok(c) => c,
        Err(_) => return default_caption(),
    };
    let matched: i64 = conn
        .query_row("SELECT COUNT(*) FROM swissdamed", [], |r| r.get(0))
        .unwrap_or(0);
    let codes: i64 = conn
        .query_row(
            "SELECT COUNT(DISTINCT migel_code) FROM swissdamed",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);
    let companies: i64 = conn
        .query_row(
            "SELECT COUNT(DISTINCT companyName) FROM swissdamed",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);
    let total: i64 = conn
        .query_row(
            "SELECT value FROM meta WHERE key = 'total_products'",
            [],
            |r| r.get::<_, String>(0),
        )
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let pct = if total > 0 {
        format!("{:.1}%", matched as f64 / total as f64 * 100.0)
    } else {
        "—".to_string()
    };

    format!(
        "swissdamed × MiGeL daily snapshot: {} of {} UDI rows ({}) mapped · {} codes · {} manufacturers.\n\
         \n\
         Windows: https://apps.microsoft.com/detail/9mvmq21r4mkc?hl=de-DE&gl=CH\n\
         macOS: https://apps.apple.com/my/app/swissdamed2sqlite/id6762261366?mt=12\n\
         #MedTech #MiGeL #SwissDAMED #UDI",
        format_thousands(matched),
        format_thousands(total),
        pct,
        codes,
        companies,
    )
}

fn default_caption() -> String {
    "swissdamed × MiGeL — daily snapshot.\n\
     Windows: https://apps.microsoft.com/detail/9mvmq21r4mkc?hl=de-DE&gl=CH\n\
     macOS: https://apps.apple.com/my/app/swissdamed2sqlite/id6762261366?mt=12\n\
     #MedTech #MiGeL #SwissDAMED #UDI"
        .to_string()
}

fn upload_media(
    client: &reqwest::blocking::Client,
    creds: &Creds,
    png_path: &Path,
) -> Result<String, Box<dyn Error>> {
    let upload_url = "https://api.twitter.com/2/media/upload";
    let bytes = fs::read(png_path)?;
    eprintln!(
        "[twitter] Uploading {} ({:.1} KB)",
        png_path.display(),
        bytes.len() as f64 / 1024.0
    );
    let fname = png_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("image.png")
        .to_string();
    let form = reqwest::blocking::multipart::Form::new()
        .text("media_category", "tweet_image")
        .text("media_type", "image/png")
        .part(
            "media",
            reqwest::blocking::multipart::Part::bytes(bytes)
                .file_name(fname)
                .mime_str("image/png")?,
        );
    let resp = client
        .post(upload_url)
        .header("Authorization", auth_header(creds, "POST", upload_url))
        .multipart(form)
        .send()?;
    let status = resp.status();
    let text = resp.text()?;
    if !status.is_success() {
        return Err(format!("media upload failed ({status}): {text}").into());
    }
    let v: serde_json::Value = serde_json::from_str(&text)?;
    let media_id = v["data"]["id"]
        .as_str()
        .or_else(|| v["media_id_string"].as_str())
        .or_else(|| v["id"].as_str())
        .ok_or_else(|| format!("no media id in upload response: {text}"))?
        .to_string();
    eprintln!("[twitter] media_id: {media_id}");
    Ok(media_id)
}

fn create_tweet(
    client: &reqwest::blocking::Client,
    creds: &Creds,
    caption: &str,
    media_ids: &[String],
) -> Result<String, Box<dyn Error>> {
    let tweets_url = "https://api.twitter.com/2/tweets";
    let body = serde_json::json!({
        "text": caption,
        "media": { "media_ids": media_ids }
    });
    let resp = client
        .post(tweets_url)
        .header("Authorization", auth_header(creds, "POST", tweets_url))
        .header("Content-Type", "application/json")
        .json(&body)
        .send()?;
    let status = resp.status();
    let text = resp.text()?;
    if !status.is_success() {
        return Err(format!("create tweet failed ({status}): {text}").into());
    }
    let v: serde_json::Value = serde_json::from_str(&text)?;
    let id = v["data"]["id"]
        .as_str()
        .ok_or_else(|| format!("no tweet id in response: {text}"))?
        .to_string();
    eprintln!("[twitter] Published: https://x.com/i/web/status/{id}");
    Ok(id)
}

/// Upload `png_path` and post a tweet with it, using a caption derived from
/// the MiGeL SQLite DB. Returns the tweet URL.
pub fn publish_image(png_path: &Path, migel_db: &Path) -> Result<String, Box<dyn Error>> {
    let creds = load_creds()?;
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(180))
        .build()?;
    let media_id = upload_media(&client, &creds, png_path)?;
    let caption = build_caption(migel_db);
    let id = create_tweet(&client, &creds, &caption, &[media_id])?;
    Ok(format!("https://x.com/i/web/status/{id}"))
}
