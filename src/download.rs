use serde_json::Value;
use std::fs;
use std::path::PathBuf;

const BROWSER_USER_AGENT: &str =
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36";

/// Create a reqwest blocking client with cookie store and browser-like User-Agent.
pub fn http_client() -> Result<reqwest::blocking::Client, Box<dyn std::error::Error>> {
    Ok(reqwest::blocking::Client::builder()
        .cookie_store(true)
        .user_agent(BROWSER_USER_AGENT)
        .build()?)
}

pub fn download_all_pages(page_size: u32) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
    download_all_pages_from(
        "https://swissdamed.ch/public/udi/basic-udis",
        "UDI",
        page_size,
    )
}

pub fn download_all_pages_from(
    base_url: &str,
    label: &str,
    page_size: u32,
) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
    let client = http_client()?;

    let mut all_values: Vec<Value> = Vec::new();
    let mut page: u32 = 0;

    loop {
        let url = format!("{}?page={}&size={}", base_url, page, page_size);
        eprintln!("[{}] Fetching page {} ...", label, page);

        let resp = client
            .post(&url)
            .header("Accept", "application/json, text/plain, */*")
            .header("Content-Type", "application/json")
            .body("{}")
            .send()?;

        if !resp.status().is_success() {
            return Err(format!("HTTP error: {} for page {}", resp.status(), page).into());
        }

        let body: Value = resp.json()?;

        let values = body
            .get("values")
            .and_then(|v| v.as_array())
            .ok_or("Response missing 'values' array")?;

        if values.is_empty() {
            break;
        }

        let count = values.len();
        all_values.extend(values.iter().cloned());
        eprintln!(
            "[{}]   got {} items (total so far: {})",
            label,
            count,
            all_values.len()
        );

        if (count as u32) < page_size {
            break;
        }

        page += 1;
    }

    eprintln!(
        "[{}] Download complete: {} items total.",
        label,
        all_values.len()
    );
    Ok(all_values)
}

pub fn load_json_file(path: &PathBuf) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
    let content = fs::read_to_string(path)?;
    let parsed: Value = serde_json::from_str(&content)?;

    if let Some(arr) = parsed.get("values").and_then(|v| v.as_array()) {
        Ok(arr.clone())
    } else if let Some(arr) = parsed.as_array() {
        Ok(arr.clone())
    } else {
        Err("JSON must contain a 'values' array or be a top-level array".into())
    }
}
