//! LinkedIn image publishing (Images API + Posts API).
//!
//! Reuses the same `linkedin_credentials.json` / `linkedin_token.json`
//! files as `li_push_rs` (lookup order: cwd, then $HOME).
//!
//! Token refresh follows the same flow as li_push_rs: if the saved token
//! has a `refresh_token`, swap it for a fresh `access_token` before posting
//! and persist the new token back to disk.

use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

const LINKEDIN_VERSION: &str = "202603";

#[derive(Serialize, Deserialize)]
struct Credentials {
    client_id: String,
    client_secret: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct Token {
    access_token: String,
    #[serde(default)]
    refresh_token: String,
    #[serde(default)]
    person_id: String,
    #[serde(default)]
    expires_in: u64,
}

fn find_file(name: &str) -> Option<PathBuf> {
    let cwd = PathBuf::from(name);
    if cwd.exists() {
        return Some(cwd);
    }
    if let Some(home) = std::env::var_os("HOME") {
        let p = PathBuf::from(home).join(name);
        if p.exists() {
            return Some(p);
        }
    }
    None
}

fn load_credentials() -> Result<(PathBuf, Credentials), Box<dyn Error>> {
    let path = find_file("linkedin_credentials.json")
        .ok_or("linkedin_credentials.json not found (looked in cwd and $HOME)")?;
    let data = fs::read_to_string(&path)?;
    let creds = serde_json::from_str(&data)?;
    Ok((path, creds))
}

fn load_token() -> Result<(PathBuf, Token), Box<dyn Error>> {
    let path = find_file("linkedin_token.json").ok_or(
        "linkedin_token.json not found (looked in cwd and $HOME). Run li_push --auth first.",
    )?;
    let data = fs::read_to_string(&path)?;
    let token: Token = serde_json::from_str(&data)?;
    Ok((path, token))
}

fn save_token(path: &Path, token: &Token) -> Result<(), Box<dyn Error>> {
    fs::write(path, serde_json::to_string_pretty(token)?)?;
    Ok(())
}

fn refresh_token(
    client: &reqwest::blocking::Client,
    creds: &Credentials,
    token: &Token,
    token_path: &Path,
) -> Token {
    if token.refresh_token.is_empty() {
        return token.clone();
    }
    eprintln!("[linkedin] Refreshing access token...");
    let body = format!(
        "grant_type=refresh_token&refresh_token={}&client_id={}&client_secret={}",
        token.refresh_token, creds.client_id, creds.client_secret
    );
    let resp = client
        .post("https://www.linkedin.com/oauth/v2/accessToken")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send();
    let Ok(resp) = resp else {
        eprintln!("[linkedin] Refresh request failed; using existing token");
        return token.clone();
    };
    let Ok(text) = resp.text() else {
        return token.clone();
    };
    let Ok(data): Result<serde_json::Value, _> = serde_json::from_str(&text) else {
        return token.clone();
    };
    let Some(at) = data["access_token"].as_str().filter(|s| !s.is_empty()) else {
        eprintln!("[linkedin] Refresh response had no access_token; using existing");
        return token.clone();
    };
    let new_token = Token {
        access_token: at.to_string(),
        refresh_token: data["refresh_token"]
            .as_str()
            .unwrap_or(&token.refresh_token)
            .to_string(),
        person_id: token.person_id.clone(),
        expires_in: data["expires_in"].as_u64().unwrap_or(0),
    };
    if let Err(e) = save_token(token_path, &new_token) {
        eprintln!("[linkedin] Could not persist refreshed token: {}", e);
    } else {
        eprintln!("[linkedin] Token refreshed");
    }
    new_token
}

/// Read summary numbers from the latest MiGeL DB to build a post caption.
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
    let matched_fmt = format_thousands(matched);
    let total_fmt = format_thousands(total);
    format!(
        "swissdamed × MiGeL: {matched} of {total} UDI rows ({pct}) mapped to MiGeL — {codes} codes, {companies} manufacturers. Daily snapshot.\n\
         \n\
         Source: swissdamed.ch · github.com/zdavatz/swissdamed2sqlite\n\
         #MedTech #MiGeL #SwissDAMED #UDI",
        matched = matched_fmt,
        total = total_fmt,
        pct = pct,
        codes = codes,
        companies = companies,
    )
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

fn default_caption() -> String {
    "swissdamed × MiGeL — daily snapshot.\n\
     Source: swissdamed.ch · Generated with github.com/zdavatz/swissdamed2sqlite\n\
     #MedTech #MiGeL #SwissDAMED #UDI"
        .to_string()
}

/// Upload the given PNG to LinkedIn as an image post.
pub fn publish_image(png_path: &Path, migel_db: &Path) -> Result<String, Box<dyn Error>> {
    let (creds_path, creds) = load_credentials()?;
    eprintln!("[linkedin] Using credentials: {}", creds_path.display());
    let (token_path, token) = load_token()?;
    if token.person_id.is_empty() {
        return Err("linkedin_token.json has empty person_id (run li_push --auth)".into());
    }

    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .build()?;
    let token = refresh_token(&client, &creds, &token, &token_path);

    let owner = format!("urn:li:person:{}", token.person_id);
    let auth = format!("Bearer {}", token.access_token);
    let bytes = fs::read(png_path)?;
    eprintln!(
        "[linkedin] Uploading {} ({:.1} KB)",
        png_path.display(),
        bytes.len() as f64 / 1024.0
    );

    // Step 1 — initialize image upload
    let init_body = serde_json::json!({
        "initializeUploadRequest": { "owner": owner }
    });
    let init_resp = client
        .post("https://api.linkedin.com/rest/images?action=initializeUpload")
        .header("Authorization", &auth)
        .header("Content-Type", "application/json")
        .header("LinkedIn-Version", LINKEDIN_VERSION)
        .header("X-Restli-Protocol-Version", "2.0.0")
        .json(&init_body)
        .send()?;
    let status = init_resp.status();
    let text = init_resp.text()?;
    if !status.is_success() {
        return Err(format!("initializeUpload failed ({}): {}", status, text).into());
    }
    let init: serde_json::Value = serde_json::from_str(&text)?;
    let value = &init["value"];
    let image_urn = value["image"]
        .as_str()
        .ok_or_else(|| format!("no image URN in initializeUpload response: {}", text))?
        .to_string();
    let upload_url = value["uploadUrl"]
        .as_str()
        .ok_or_else(|| format!("no uploadUrl in initializeUpload response: {}", text))?
        .to_string();
    eprintln!("[linkedin] Image URN: {}", image_urn);

    // Step 2 — PUT the bytes
    let put_resp = client
        .put(&upload_url)
        .header("Authorization", &auth)
        .header("Content-Type", "application/octet-stream")
        .body(bytes)
        .send()?;
    let put_status = put_resp.status();
    if !put_status.is_success() {
        let body = put_resp.text().unwrap_or_default();
        return Err(format!("image PUT failed ({}): {}", put_status, body).into());
    }
    eprintln!("[linkedin] Image bytes uploaded");

    // Step 3 — create post
    let caption = build_caption(migel_db);
    let post_body = serde_json::json!({
        "author": owner,
        "commentary": caption,
        "visibility": "PUBLIC",
        "distribution": {
            "feedDistribution": "MAIN_FEED",
            "targetEntities": [],
            "thirdPartyDistributionChannels": []
        },
        "content": {
            "media": {
                "title": "swissdamed MiGeL Matching Stats",
                "id": image_urn
            }
        },
        "lifecycleState": "PUBLISHED",
        "isReshareDisabledByAuthor": false
    });
    let post_resp = client
        .post("https://api.linkedin.com/rest/posts")
        .header("Authorization", &auth)
        .header("Content-Type", "application/json")
        .header("LinkedIn-Version", LINKEDIN_VERSION)
        .header("X-Restli-Protocol-Version", "2.0.0")
        .json(&post_body)
        .send()?;
    let post_status = post_resp.status();
    let post_headers = post_resp.headers().clone();
    let post_text = post_resp.text().unwrap_or_default();
    if !post_status.is_success() {
        return Err(format!("create post failed ({}): {}", post_status, post_text).into());
    }
    let post_id = post_headers
        .get("x-restli-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("(unknown)")
        .to_string();
    let post_url = format!("https://www.linkedin.com/feed/update/{}/", post_id);
    eprintln!("[linkedin] Published: {}", post_url);
    Ok(post_url)
}
