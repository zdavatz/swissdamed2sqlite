use serde_json::Value;
use std::fs;
use std::process::Command;

use crate::{resolve_setting, Args, Config};

// --- P12 key extraction ---

fn extract_pem_from_p12(p12_path: &str) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("openssl")
        .args([
            "pkcs12",
            "-in",
            p12_path,
            "-nocerts",
            "-nodes",
            "-passin",
            "pass:notasecret",
            "-legacy",
        ])
        .output();

    // Try with -legacy flag first (OpenSSL 3.x), fall back without it (LibreSSL/older)
    let output = match output {
        Ok(o) if o.status.success() => o,
        _ => Command::new("openssl")
            .args([
                "pkcs12",
                "-in",
                p12_path,
                "-nocerts",
                "-nodes",
                "-passin",
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
fn resolve_google_credentials(args: &Args) -> Result<(String, String), Box<dyn std::error::Error>> {
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
    let gdrive_folder =
        resolve_setting(&args.gdrive_folder, &config.gdrive_folder, "gdrive-folder")?;
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

    // Sanitize values interpolated into MIME headers to prevent header injection
    let sanitize_header = |s: &str| s.replace(['\r', '\n'], "");
    let safe_file_name = sanitize_header(file_name);
    let safe_to = sanitize_header(to_email);
    let safe_from = sanitize_header(sub_email);

    let file_content = fs::read(csv_path)?;
    let encoded_attachment = engine.encode(&file_content);

    let boundary = "swissdamed2sqlite_email_boundary";
    let subject_raw = args
        .mail_subject
        .clone()
        .unwrap_or_else(|| format!("swissdamed2sqlite: {}", safe_file_name));
    let subject = sanitize_header(&if subject_raw.is_ascii() {
        subject_raw
    } else {
        format!("=?UTF-8?B?{}?=", engine.encode(subject_raw.as_bytes()))
    });

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
        from = safe_from,
        to = safe_to,
        subject = subject,
        boundary = boundary,
        file_name = safe_file_name,
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

// --- Google Drive / Gmail read access ---
//
// The write paths above (upload, send) cover the daily reports. These read paths
// cover the other direction: pulling a partner's spreadsheet out of Drive, or
// finding the mail they sent it with. Both were throwaway Python at first; they
// live here so the next run is a command rather than a rebuild.
//
// Every call goes through the same service-account JWT as the write paths.
// Reading a Workspace mailbox additionally needs `--gdrive-sub` (domain-wide
// delegation): without a `sub` claim the token is the service account's own
// identity, which has no mailbox at all.

/// Scope for reading Drive files the service account can see.
const SCOPE_DRIVE_READ: &str = "https://www.googleapis.com/auth/drive.readonly";
/// Scope for reading a delegated user's mailbox.
const SCOPE_GMAIL_READ: &str = "https://www.googleapis.com/auth/gmail.readonly";

/// GET `url` with a bearer token, returning the body bytes on 2xx.
/// Errors carry the response body — Google explains refusals there (a wrong
/// `sub`, a missing domain-wide-delegation scope, a file that was never shared),
/// and swallowing it turns a fixable problem into a bare status code.
fn google_get(url: &str, token: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    // Drive downloads run to tens of megabytes; reqwest's default timeout cuts
    // those off mid-body and surfaces as an opaque "error decoding response body".
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(900))
        .build()?;
    let resp = client
        .get(url)
        .header("Authorization", format!("Bearer {token}"))
        .send()?;
    let status = resp.status();
    let body = resp.bytes()?;
    if !status.is_success() {
        return Err(format!(
            "{status}: {}",
            String::from_utf8_lossy(&body).chars().take(400).collect::<String>()
        )
        .into());
    }
    Ok(body.to_vec())
}

/// Percent-encode a query string for use in a URL. Gmail queries carry spaces,
/// colons and `@`, all of which must survive the round trip; pulling in a crate
/// for this one call would not earn its keep.
fn percent_encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

fn google_get_json(url: &str, token: &str) -> Result<Value, Box<dyn std::error::Error>> {
    Ok(serde_json::from_slice(&google_get(url, token)?)?)
}

/// Download a Drive file verbatim (`alt=media`) to `out_path`.
///
/// Note this is the *raw bytes* path: it works for uploaded files (an .xlsx a
/// partner put in Drive) but NOT for native Google Docs/Sheets, which have no
/// byte stream and must be exported instead. `supportsAllDrives` is set so files
/// living in a shared drive resolve rather than 404.
pub fn drive_download(
    args: &Args,
    file_id: &str,
    out_path: &std::path::Path,
) -> Result<u64, Box<dyn std::error::Error>> {
    let (pem, email) = resolve_google_credentials(args)?;
    let token = get_google_access_token(
        &pem,
        &email,
        SCOPE_DRIVE_READ,
        args.gdrive_sub.as_deref(),
    )?;

    let meta = google_get_json(
        &format!(
            "https://www.googleapis.com/drive/v3/files/{file_id}\
             ?supportsAllDrives=true&fields=name,mimeType,size"
        ),
        &token,
    )?;
    let name = meta.get("name").and_then(|v| v.as_str()).unwrap_or("?");
    let mime = meta.get("mimeType").and_then(|v| v.as_str()).unwrap_or("?");
    eprintln!("[gdrive] {name} ({mime})");
    if mime.starts_with("application/vnd.google-apps") {
        return Err(format!(
            "{name} is a native Google {mime} — it has no raw bytes to download. \
             Export it (File → Download) or use the export endpoint instead."
        )
        .into());
    }

    let bytes = google_get(
        &format!("https://www.googleapis.com/drive/v3/files/{file_id}?alt=media&supportsAllDrives=true"),
        &token,
    )?;
    fs::write(out_path, &bytes)?;
    eprintln!("[gdrive] saved {} ({} bytes)", out_path.display(), bytes.len());
    Ok(bytes.len() as u64)
}

/// One line of `--gmail-search` output.
pub struct MailSummary {
    pub id: String,
    pub date: String,
    pub from: String,
    pub subject: String,
}

/// Search the delegated mailbox with a Gmail query (`from:…`, `has:attachment`, …).
pub fn gmail_search(
    args: &Args,
    query: &str,
    max: u32,
) -> Result<Vec<MailSummary>, Box<dyn std::error::Error>> {
    let sub = args.gdrive_sub.as_deref().ok_or(
        "--gmail-search requires --gdrive-sub <email>: a service account has no mailbox of its own",
    )?;
    let (pem, email) = resolve_google_credentials(args)?;
    let token = get_google_access_token(&pem, &email, SCOPE_GMAIL_READ, Some(sub))?;

    let list = google_get_json(
        &format!(
            "https://gmail.googleapis.com/gmail/v1/users/me/messages\
             ?q={}&maxResults={max}",
            percent_encode(query)
        ),
        &token,
    )?;

    let mut out = Vec::new();
    for m in list.get("messages").and_then(|v| v.as_array()).unwrap_or(&vec![]) {
        let Some(id) = m.get("id").and_then(|v| v.as_str()) else { continue };
        let full = google_get_json(
            &format!(
                "https://gmail.googleapis.com/gmail/v1/users/me/messages/{id}\
                 ?format=metadata&metadataHeaders=From&metadataHeaders=Subject&metadataHeaders=Date"
            ),
            &token,
        )?;
        let header = |name: &str| -> String {
            full.get("payload")
                .and_then(|p| p.get("headers"))
                .and_then(|h| h.as_array())
                .and_then(|hs| {
                    hs.iter().find(|h| {
                        h.get("name").and_then(|n| n.as_str()) == Some(name)
                    })
                })
                .and_then(|h| h.get("value"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        };
        out.push(MailSummary {
            id: id.to_string(),
            date: header("Date"),
            from: header("From"),
            subject: header("Subject"),
        });
    }
    Ok(out)
}

/// Save every attachment of one message into `out_dir`, returning their paths.
///
/// Gmail nests parts arbitrarily deep (a forwarded mail with an attached mail
/// with an attachment), so the payload tree is walked rather than only its top
/// level — otherwise attachments quietly go missing.
pub fn gmail_attachments(
    args: &Args,
    msg_id: &str,
    out_dir: &std::path::Path,
) -> Result<Vec<std::path::PathBuf>, Box<dyn std::error::Error>> {
    use base64::Engine;
    let sub = args.gdrive_sub.as_deref().ok_or(
        "--gmail-attachments requires --gdrive-sub <email>: a service account has no mailbox of its own",
    )?;
    let (pem, email) = resolve_google_credentials(args)?;
    let token = get_google_access_token(&pem, &email, SCOPE_GMAIL_READ, Some(sub))?;

    let msg = google_get_json(
        &format!("https://gmail.googleapis.com/gmail/v1/users/me/messages/{msg_id}?format=full"),
        &token,
    )?;

    let mut found = Vec::new();
    collect_attachment_parts(msg.get("payload"), &mut found);
    if found.is_empty() {
        eprintln!("[gmail] message {msg_id} has no attachments");
        return Ok(Vec::new());
    }

    fs::create_dir_all(out_dir)?;
    let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let mut written = Vec::new();
    for (filename, att_id) in found {
        let body = google_get_json(
            &format!(
                "https://gmail.googleapis.com/gmail/v1/users/me/messages/{msg_id}/attachments/{att_id}"
            ),
            &token,
        )?;
        let data = body
            .get("data")
            .and_then(|v| v.as_str())
            .ok_or("attachment response carried no data")?;
        // Gmail uses base64url, and pads inconsistently — strip padding so the
        // no-pad decoder accepts both shapes.
        let bytes = engine.decode(data.trim_end_matches('='))?;
        let path = out_dir.join(sanitize_filename(&filename));
        fs::write(&path, &bytes)?;
        eprintln!("[gmail] saved {} ({} bytes)", path.display(), bytes.len());
        written.push(path);
    }
    Ok(written)
}

/// Walk a Gmail payload tree, collecting `(filename, attachmentId)` pairs.
fn collect_attachment_parts(part: Option<&Value>, out: &mut Vec<(String, String)>) {
    let Some(part) = part else { return };
    let filename = part.get("filename").and_then(|v| v.as_str()).unwrap_or("");
    let att_id = part
        .get("body")
        .and_then(|b| b.get("attachmentId"))
        .and_then(|v| v.as_str());
    if !filename.is_empty() {
        if let Some(id) = att_id {
            out.push((filename.to_string(), id.to_string()));
        }
    }
    if let Some(parts) = part.get("parts").and_then(|v| v.as_array()) {
        for p in parts {
            collect_attachment_parts(Some(p), out);
        }
    }
}

/// Reduce an attachment name to something safe to write into `out_dir`.
/// A sender controls this string, so path separators and `..` must not survive —
/// otherwise a crafted attachment name could write outside the target directory.
fn sanitize_filename(name: &str) -> String {
    let base = name.rsplit(['/', '\\']).next().unwrap_or(name);
    let cleaned: String = base
        .chars()
        .map(|c| if c.is_control() { '_' } else { c })
        .collect();
    let trimmed = cleaned.trim().trim_matches('.').trim();
    if trimmed.is_empty() {
        "attachment.bin".to_string()
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attachment_names_cannot_escape_the_output_directory() {
        // The filename comes from the sender; a path traversal in it must not
        // let a mail write outside out_dir.
        assert_eq!(sanitize_filename("../../etc/passwd"), "passwd");
        assert_eq!(sanitize_filename("../../../evil.sh"), "evil.sh");
        assert_eq!(sanitize_filename("C:\\Windows\\evil.exe"), "evil.exe");
        assert_eq!(sanitize_filename("normal.xlsx"), "normal.xlsx");
        // A name that is only dots/space would otherwise yield an empty path.
        assert_eq!(sanitize_filename("  ..  "), "attachment.bin");
        assert_eq!(sanitize_filename(""), "attachment.bin");
    }

    #[test]
    fn gmail_query_characters_survive_encoding() {
        // A real query is `from:someone@example.com has:attachment` — the space,
        // colon and @ all have to be escaped or Gmail sees a different query.
        assert_eq!(
            percent_encode("from:a@b.ch has:attachment"),
            "from%3Aa%40b.ch%20has%3Aattachment"
        );
        assert_eq!(percent_encode("plain-Query_1.0~x"), "plain-Query_1.0~x");
    }

    #[test]
    fn attachments_are_collected_from_nested_parts() {
        // A forwarded mail nests the real attachment one level down; only
        // scanning the top-level parts would silently miss it.
        let payload = serde_json::json!({
            "filename": "",
            "parts": [
                {"filename": "", "body": {}},
                {"filename": "top.pdf", "body": {"attachmentId": "a1"}},
                {"filename": "", "parts": [
                    {"filename": "nested.xlsx", "body": {"attachmentId": "a2"}}
                ]}
            ]
        });
        let mut found = Vec::new();
        collect_attachment_parts(Some(&payload), &mut found);
        assert_eq!(
            found,
            vec![
                ("top.pdf".to_string(), "a1".to_string()),
                ("nested.xlsx".to_string(), "a2".to_string())
            ]
        );
    }

    #[test]
    fn a_part_without_an_attachment_id_is_skipped() {
        // Inline body parts carry a filename but no attachmentId; treating them
        // as attachments would produce a bogus fetch.
        let payload = serde_json::json!({"filename": "inline.txt", "body": {"size": 12}});
        let mut found = Vec::new();
        collect_attachment_parts(Some(&payload), &mut found);
        assert!(found.is_empty());
    }
}
