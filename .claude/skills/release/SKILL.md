---
name: release
description: Release & CI mechanics for swissdamed2sqlite — GitHub Actions build matrix (macOS universal + DMG + App Store, Windows portable + MSIX + Microsoft Store, Linux tar.gz + AppImage), code-signing/notarization certificates, the MAC_APP_DISTRIBUTION provisioning-profile requirement, and store submission. Use when tagging a release, editing .github/workflows/release.yml, or debugging signing/packaging/store-submission failures.
---

# Release & CI

Release/CI details migrated out of `CLAUDE.md` so they load only when doing release work. The `winit` build patch stays in `CLAUDE.md` (it is a cross-cutting build gotcha, not release-only).

## CI (`.github/workflows/ci.yml`)

Triggered on every push (non-`v*` tags) and pull request. Builds all three platforms in parallel (macOS universal, Linux, Windows) without signing, packaging, or releasing. Copies `config.sample.toml → config.toml` before building.

## Release (`.github/workflows/release.yml`)

Triggered by `git tag v* && git push --tags`. Builds for all platforms in parallel:
- **macOS**: universal binary (arm64 + x86_64), .app bundle with ICNS icon (generated from `assets/icon.iconset/` via `iconutil`), signed DMG (Developer ID), notarized, App Store .pkg (signed with Mac App Distribution + Mac Installer Distribution certs) uploaded via `xcrun altool` (iTMSTransporter fallback)
- **Windows**: portable ZIP, signed MSIX, Microsoft Store submission via Partner Center API (listings, pricing=Free, visibility=Public, publishMode=Immediate)
- **Linux**: tar.gz + AppImage
- **GitHub Release**: collects all artifacts via `softprops/action-gh-release`
- Version synced from git tag to Cargo.toml automatically

Platform configs: `build.rs` (Windows icon), `entitlements.plist` / `entitlements-appstore.plist` (macOS), `windows/AppxManifest.xml` + `windows/assets/` (MSIX/Store).

Store screenshots: `screenshots/windows/` (PNG, 1366x768+), `screenshots/macos/` (PNG, 1280x800 / 1440x900 / 2560x1600 / 2880x1800).

## macOS Signing Details

- DMG: signed with `Developer ID Application: ywesee GmbH` + `entitlements.plist`, notarized via `notarytool`
- App Store .pkg: re-signed with `Apple Distribution` / `Mac App Distribution` / `3rd Party Mac Developer Application` + `entitlements-appstore.plist`, packaged with `3rd Party Mac Developer Installer`
- Provisioning profile (`MACOS_PROVISIONING_PROFILE` secret) must use `MAC_APP_DISTRIBUTION` cert type (not `DISTRIBUTION`) to match the signing identity
- ICNS icon generated at build time from `assets/icon.iconset/` (contains 16x16 through 512x512@2x PNGs)
