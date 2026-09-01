//! Locating the credential files on disk.
//!
//! These files used to be looked up in the current directory and `$HOME`, which
//! meant they sat unprotected next to the source tree (`linkedin_token.json`,
//! `twitter_credentials.json`, the Google service-account `.p12`) — one careless
//! `git add -A` or a shared screen away from exposure.
//!
//! They now belong in a dedicated, owner-only directory
//! (`~/.config/swissdamed2sqlite`, created with mode 0700). The old locations are
//! still searched, so installs that share files with `li_push_rs` keep working,
//! but they come last.
//!
//! Note this module only resolves *paths* — the files themselves stay plaintext.
//! Restrictive permissions keep them out of sight, they are not encryption.
//!
//! Lookup order:
//!   1. `$SWISSDAMED_CREDENTIALS_DIR` — explicit override (CI, containers, a vault mount)
//!   2. `~/.config/swissdamed2sqlite`
//!   3. the current directory     (legacy)
//!   4. `$HOME`                   (legacy)

use std::path::PathBuf;

/// The directory credential files belong in.
pub fn dir() -> Option<PathBuf> {
    // An exported-but-empty override must not win: `PathBuf::from("")` would
    // silently resolve every credential relative to the current directory.
    if let Some(d) = std::env::var_os("SWISSDAMED_CREDENTIALS_DIR") {
        if !d.is_empty() {
            return Some(PathBuf::from(d));
        }
    }
    let home = std::env::var_os("HOME")?;
    Some(PathBuf::from(home).join(".config").join("swissdamed2sqlite"))
}

/// Create the credential directory if missing, restricted to the owner.
/// Callers that only read may ignore the result.
pub fn ensure_dir() -> std::io::Result<PathBuf> {
    let dir = dir().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::NotFound, "no HOME and no override set")
    })?;
    std::fs::create_dir_all(&dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(dir)
}

/// Locate a credential file by name, preferring the protected directory and
/// falling back to the historical cwd/`$HOME` locations.
pub fn find(name: &str) -> Option<PathBuf> {
    if let Some(d) = dir() {
        let p = d.join(name);
        if p.exists() {
            return Some(p);
        }
    }
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

/// The places searched, so a "not found" error can tell the user where the file
/// is expected instead of leaving them to guess.
pub fn searched_locations() -> String {
    let mut parts = Vec::new();
    if let Some(d) = dir() {
        parts.push(d.display().to_string());
    }
    parts.push("current directory".to_string());
    parts.push("$HOME".to_string());
    parts.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `SWISSDAMED_CREDENTIALS_DIR` is process-global, and libtest runs these in
    /// parallel threads: without this lock one test's `set_var` lands between
    /// another's `set_var` and its assert, which made the suite flaky (observed
    /// 01.09.2026: the override test read the perm test's directory).
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn lock_env() -> std::sync::MutexGuard<'static, ()> {
        // A test that panicked while holding the lock poisons it; the guarded
        // state is just the env var, which every test sets before reading.
        ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[test]
    fn env_override_wins_over_the_default_config_dir() {
        // A deployment that mounts credentials elsewhere must not be forced into
        // ~/.config; the override is the whole point of the env var.
        let _guard = lock_env();
        let tmp = std::env::temp_dir().join("swissdamed_credentials_test");
        std::env::set_var("SWISSDAMED_CREDENTIALS_DIR", &tmp);
        assert_eq!(dir(), Some(tmp));
        std::env::remove_var("SWISSDAMED_CREDENTIALS_DIR");
    }

    #[test]
    fn an_empty_override_falls_back_rather_than_returning_an_empty_path() {
        // An unset-but-exported env var ("") must not resolve to "" — that would
        // make every credential path relative to the current directory.
        let _guard = lock_env();
        std::env::set_var("SWISSDAMED_CREDENTIALS_DIR", "");
        let d = dir();
        std::env::remove_var("SWISSDAMED_CREDENTIALS_DIR");
        assert!(
            d.map(|p| p.ends_with("swissdamed2sqlite")).unwrap_or(false),
            "empty override should fall through to ~/.config/swissdamed2sqlite"
        );
    }

    #[test]
    fn missing_file_is_reported_as_none() {
        let _guard = lock_env();
        assert!(find("definitely-not-a-real-credential-file.json").is_none());
    }

    #[cfg(unix)]
    #[test]
    fn ensure_dir_restricts_permissions_to_the_owner() {
        // The whole point of moving the files is that nobody else can read them;
        // a directory created with the default umask would defeat that.
        use std::os::unix::fs::PermissionsExt;
        let _guard = lock_env();
        let tmp = std::env::temp_dir().join("swissdamed_credentials_perm_test");
        let _ = std::fs::remove_dir_all(&tmp);
        std::env::set_var("SWISSDAMED_CREDENTIALS_DIR", &tmp);
        let made = ensure_dir().expect("should create the directory");
        std::env::remove_var("SWISSDAMED_CREDENTIALS_DIR");
        let mode = std::fs::metadata(&made).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700, "credential dir must be owner-only, got {mode:o}");
        let _ = std::fs::remove_dir_all(&tmp);
    }
}
