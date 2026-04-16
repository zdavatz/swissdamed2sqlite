use serde_json::Value;
use std::fs;
use std::process::Command;

use crate::{Args, Config, resolve_setting};

// --- P12 key extraction ---

fn extract_pem_from_p12(p12_path: &str) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("openssl")
        .args([
            "pkcs12", "-in", p12_path, "-nocerts", "-nodes", "-passin",
            "pass:notasecret", "-legacy",
        ])
        .output();

    // Try with -legacy flag first (OpenSSL 3.x), fall back without it (LibreSSL/older)
    let output = match output {
        Ok(o) if o.status.success() => o,
        _ => Command::new("openssl")
            .args([
                "pkcs12", "-in", p12_path, "-nocerts", "-nodes", "-passin",
                "pass:notasecret",
            ])
            .output()?,
    };

    if !output.status.success() {
        return Err(format!(
            "openssl pkcs12 failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }

    Ok(String::from_utf8(output.stdout)?)
}

// --- Shared Google JWT auth ---

/// Obtain a Google OAuth2 access token via service account JWT assertion.
fn get_google_access_token(
    pem_key: &str,
    service_email: &str,
    scope: &str,
    sub_email: Option<&str>,
) -> Result<String, Box<dyn std::error::Error>> {
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use serde::Serialize;

    #[derive(Serialize)]
    struct Claims {
        iss: String,
        scope: String,
        aud: String,
        exp: u64,
        iat: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        sub: Option<String>,
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();

    let claims = Claims {
        iss: service_email.to_string(),
        scope: scope.to_string(),
        aud: "https://oauth2.googleapis.com/token".to_string(),
        iat: now,
        exp: now + 3600,
        sub: sub_email.map(|s| s.to_string()),
    };

    let header = Header::new(Algorithm::RS256);
    let key = EncodingKey::from_rsa_pem(pem_key.as_bytes())?;
    let jwt = encode(&header, &claims, &key)?;

    let client = reqwest::blocking::Client::new();
    let resp = client
        .post("https://oauth2.googleapis.com/token")
        .form(&[
            ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
            ("assertion", &jwt),
        ])
        .send()?;

    let body: Value = resp.json()?;
    body.get("access_token")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| format!("No access_token in response: {}", body).into())
}

/// Resolve Google service account credentials from CLI args / config file,
/// extract PEM, and return (pem, email) for token requests.
fn resolve_google_credentials(
    args: &Args,
) -> Result<(String, String), Box<dyn std::error::Error>> {
    let config = Config::load();
    let gdrive_key = resolve_setting(&args.gdrive_key, &config.gdrive_key, "gdrive-key")?;
    let gdrive_email = resolve_setting(&args.gdrive_email, &config.gdrive_email, "gdrive-email")?;
    let pem = extract_pem_from_p12(&gdrive_key)?;
    Ok((pem, gdrive_email))
}

// --- Google Drive upload ---

fn upload_to_gdrive(
    access_token: &str,
    file_path: &str,
    folder_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let file_name = std::path::Path::new(file_path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(file_path);

    let file_content = fs::read(file_path)?;

    let boundary = "swissdamed2sqlite_boundary";
    let metadata = serde_json::json!({
        "name": file_name,
        "parents": [folder_id]
    });

    let mut body = Vec::new();
    body.extend_from_slice(format!("--{}\r\n", boundary).as_bytes());
    body.extend_from_slice(b"Content-Type: application/json; charset=UTF-8\r\n\r\n");
    body.extend_from_slice(metadata.to_string().as_bytes());
    body.extend_from_slice(format!("\r\n--{}\r\n", boundary).as_bytes());
    body.extend_from_slice(b"Content-Type: text/csv\r\n\r\n");
    body.extend_from_slice(&file_content);
    body.extend_from_slice(format!("\r\n--{}--\r\n", boundary).as_bytes());

    let client = reqwest::blocking::Client::new();
    let resp = client
        .post("https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&supportsAllDrives=true")
        .header("Authorization", format!("Bearer {}", access_token))
        .header(
            "Content-Type",
            format!("multipart/related; boundary={}", boundary),
        )
        .body(body)
        .send()?;

    if resp.status().is_success() {
        let result: Value = resp.json()?;
        let id = result.get("id").and_then(|v| v.as_str()).unwrap_or("?");
        eprintln!("Uploaded {} to Google Drive (id: {})", file_name, id);
    } else {
        let status = resp.status();
        let err_body = resp.text().unwrap_or_default();
        return Err(format!("Google Drive upload failed ({}): {}", status, err_body).into());
    }

    Ok(())
}

pub fn gdrive_upload_csv(args: &Args, csv_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    if args.gdrive_sub.is_none() {
        return Err(
            "--gdrive requires --gdrive-sub <email> to impersonate a Google Workspace user".into(),
        );
    }
    let config = Config::load();
    let gdrive_folder = resolve_setting(&args.gdrive_folder, &config.gdrive_folder, "gdrive-folder")?;
    let (pem, email) = resolve_google_credentials(args)?;
    eprintln!("Uploading {} to Google Drive...", csv_path);
    let token = get_google_access_token(
        &pem,
        &email,
        "https://www.googleapis.com/auth/drive.file",
        args.gdrive_sub.as_deref(),
    )?;
    upload_to_gdrive(&token, csv_path, &gdrive_folder)?;
    Ok(())
}

// --- Gmail send with attachment ---

pub fn send_email_with_attachment(
    args: &Args,
    csv_path: &str,
    to_email: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use base64::Engine;
    let engine = base64::engine::general_purpose::STANDARD;
    let url_engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;

    let sub_email = args
        .gdrive_sub
        .as_deref()
        .ok_or("--mailto requires --gdrive-sub <email> to send from")?;

    eprintln!("Sending {} via email to {} ...", csv_path, to_email);

    let (pem, email) = resolve_google_credentials(args)?;
    let token = get_google_access_token(
        &pem,
        &email,
        "https://www.googleapis.com/auth/gmail.send",
        Some(sub_email),
    )?;

    let file_name = std::path::Path::new(csv_path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(csv_path);

    let file_content = fs::read(csv_path)?;
    let encoded_attachment = engine.encode(&file_content);

    let boundary = "swissdamed2sqlite_email_boundary";
    let subject_raw = args
        .mail_subject
        .clone()
        .unwrap_or_else(|| format!("swissdamed2sqlite: {}", file_name));
    let subject = if subject_raw.is_ascii() {
        subject_raw
    } else {
        format!("=?UTF-8?B?{}?=", engine.encode(subject_raw.as_bytes()))
    };

    let raw_email = format!(
        "From: {from}\r\n\
         To: {to}\r\n\
         Subject: {subject}\r\n\
         MIME-Version: 1.0\r\n\
         Content-Type: multipart/mixed; boundary=\"{boundary}\"\r\n\
         \r\n\
         --{boundary}\r\n\
         Content-Type: text/plain; charset=\"UTF-8\"\r\n\
         \r\n\
         CSV file attached: {file_name}\r\n\
         \r\n\
         --{boundary}\r\n\
         Content-Type: text/csv; name=\"{file_name}\"\r\n\
         Content-Disposition: attachment; filename=\"{file_name}\"\r\n\
         Content-Transfer-Encoding: base64\r\n\
         \r\n\
         {attachment}\r\n\
         --{boundary}--\r\n",
        from = sub_email,
        to = to_email,
        subject = subject,
        boundary = boundary,
        file_name = file_name,
        attachment = encoded_attachment,
    );

    let encoded_message = url_engine.encode(raw_email.as_bytes());

    let body = serde_json::json!({ "raw": encoded_message });

    let client = reqwest::blocking::Client::new();
    let resp = client
        .post("https://www.googleapis.com/gmail/v1/users/me/messages/send")
        .header("Authorization", format!("Bearer {}", token))
        .header("Content-Type", "application/json")
        .body(body.to_string())
        .send()?;

    if resp.status().is_success() {
        let result: Value = resp.json()?;
        let id = result.get("id").and_then(|v| v.as_str()).unwrap_or("?");
        eprintln!("Email sent to {} (message id: {})", to_email, id);
    } else {
        let status = resp.status();
        let err_body = resp.text().unwrap_or_default();
        return Err(format!("Gmail send failed ({}): {}", status, err_body).into());
    }

    Ok(())
}
