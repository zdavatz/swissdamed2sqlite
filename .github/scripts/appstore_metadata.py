#!/usr/bin/env python3
"""
Create / submit the macOS App Store version for swissdamed2sqlite on
api.appstoreconnect.apple.com, with auto-release-after-approval.

Adapted from parados_rust's appstore_metadata.py. Uploading a .pkg (altool /
iTMSTransporter) only adds a build to App Store Connect — it does NOT create the
version record, attach the build, choose a release type, or submit for review.
Nothing did that automatically, so every swissdamed release uploaded a build
that then sat unreleased. This script closes that gap:

  1. finds — or CREATES — the editable MAC_OS appStoreVersion for this build,
     with releaseType = AFTER_APPROVAL  ("Automatically release this version"
     the moment Apple approves)                     <- the primary win
  2. sets "What's New" (per-locale, best-effort) when --whats-new is given
  3. with --submit: waits (bounded) for the uploaded build to finish
     processing, declares export compliance, attaches it, and submits the
     version for review

Deliberately does NOT overwrite the listing description / keywords / name /
subtitle / URLs — those carry over from the previously published version and
are maintained in App Store Connect, not here.

Failure is non-fatal: the calling workflow step suppresses errors so a build
that already uploaded isn't blocked by a metadata / timing hiccup. Whatever it
can't finish, the maintainer completes in App Store Connect (releaseType is
already set to auto-release).
"""

from __future__ import annotations

import argparse
import base64
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

API_BASE = "https://api.appstoreconnect.apple.com/v1"

# Shown as the App Store copyright line. Set only when CREATING a version so an
# existing hand-set value is never clobbered.
COPYRIGHT = "ywesee GmbH"

EDITABLE_VERSION_STATES = {
    "PREPARE_FOR_SUBMISSION", "DEVELOPER_REJECTED",
    "REJECTED", "METADATA_REJECTED", "INVALID_BINARY",
}


# ---------------------------------------------------------------------
# JWT signing — no PyJWT dependency on GitHub-hosted runners. Sign ES256
# with `cryptography` if present, else shell out to `openssl`.
# ---------------------------------------------------------------------
def jwt_token(key_id: str, issuer_id: str, key_path: Path) -> str:
    header = {"alg": "ES256", "kid": key_id, "typ": "JWT"}
    payload = {
        "iss": issuer_id,
        "iat": int(time.time()),
        "exp": int(time.time()) + 600,
        "aud": "appstoreconnect-v1",
    }

    def b64(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

    msg = (
        b64(json.dumps(header, separators=(",", ":")).encode())
        + "."
        + b64(json.dumps(payload, separators=(",", ":")).encode())
    )

    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import ec
        from cryptography.hazmat.primitives.asymmetric.utils import (
            decode_dss_signature,
        )

        pem = key_path.read_bytes()
        private_key = serialization.load_pem_private_key(pem, password=None)
        sig_der = private_key.sign(msg.encode(), ec.ECDSA(hashes.SHA256()))
        r, s = decode_dss_signature(sig_der)
        sig = r.to_bytes(32, "big") + s.to_bytes(32, "big")
        return msg + "." + b64(sig)
    except Exception as e:  # noqa: BLE001
        print(f"warning: cryptography unavailable ({e}); falling back to openssl",
              file=sys.stderr)
        import subprocess

        sig_der = subprocess.check_output(
            ["openssl", "dgst", "-sha256", "-sign", str(key_path)],
            input=msg.encode(),
        )
        # openssl emits DER — strip to raw r||s 64-byte form.
        i = 2
        if sig_der[i] != 0x02:
            raise RuntimeError("unexpected DER signature shape")
        lr = sig_der[i + 1]
        r = sig_der[i + 2 : i + 2 + lr].lstrip(b"\x00")
        i += 2 + lr
        if sig_der[i] != 0x02:
            raise RuntimeError("unexpected DER signature shape")
        ls = sig_der[i + 1]
        s = sig_der[i + 2 : i + 2 + ls].lstrip(b"\x00")
        sig = r.rjust(32, b"\x00") + s.rjust(32, b"\x00")
        return msg + "." + b64(sig)


def api(token: str, method: str, path: str, body: dict | None = None) -> dict:
    url = path if path.startswith("http") else f"{API_BASE}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        detail = e.read().decode(errors="replace")
        raise RuntimeError(f"{method} {path} → {e.code}\n{detail}") from None


def wait_for_build(token: str, app_id: str, version: str, timeout: int = 1800):
    """Poll until the MAC_OS build for `version` finishes processing.

    Apple takes seconds to ~20 minutes to move a freshly uploaded build from
    PROCESSING to VALID; it cannot be attached to a version before then.
    """
    deadline = time.time() + timeout
    while True:
        builds = api(token, "GET",
                     f"/builds?filter[app]={app_id}&limit=20&sort=-uploadedDate")
        state = None
        for b in builds.get("data", []):
            if b.get("attributes", {}).get("version") != version:
                continue
            pre = api(token, "GET",
                      f'/builds/{b["id"]}/preReleaseVersion').get("data")
            if not pre or pre.get("attributes", {}).get("platform") != "MAC_OS":
                continue
            state = b["attributes"].get("processingState")
            if state == "VALID":
                return b["id"]
            break
        if time.time() >= deadline:
            print(f"warning: build {version} not VALID after {timeout}s "
                  f"(last state: {state}) — leaving version unsubmitted",
                  file=sys.stderr)
            return None
        print(f"  build {version} state={state or 'not found yet'}; waiting 30s…",
              file=sys.stderr)
        time.sleep(30)


def ensure_export_compliance(token: str, build_id: str) -> None:
    """Answer the export-compliance question the review submission requires.

    swissdamed2sqlite only uses standard HTTPS/TLS (reqwest), which is exempt.
    Without this the review submission fails with
    ENTITY_ERROR.ATTRIBUTE.REQUIRED on 'usesNonExemptEncryption'.
    """
    b = api(token, "GET", f"/builds/{build_id}")["data"]
    if b.get("attributes", {}).get("usesNonExemptEncryption") is None:
        api(token, "PATCH", f"/builds/{build_id}", body={"data": {
            "id": build_id, "type": "builds",
            "attributes": {"usesNonExemptEncryption": False}}})
        print("  + export compliance declared (usesNonExemptEncryption=False)",
              file=sys.stderr)


def submit_for_review(token: str, app_id: str, version_id: str) -> None:
    subs = api(token, "GET", f"/apps/{app_id}/reviewSubmissions?limit=10")
    sid = next((s["id"] for s in subs.get("data", [])
                if s.get("attributes", {}).get("platform") == "MAC_OS"
                and s.get("attributes", {}).get("state")
                in {"READY_FOR_REVIEW", "UNRESOLVED_ISSUES"}), None)
    if sid is None:
        sid = api(token, "POST", "/reviewSubmissions", body={"data": {
            "type": "reviewSubmissions",
            "attributes": {"platform": "MAC_OS"},
            "relationships": {"app": {"data": {"type": "apps", "id": app_id}}},
        }})["data"]["id"]
        print(f"  + created reviewSubmission {sid}", file=sys.stderr)

    api(token, "POST", "/reviewSubmissionItems", body={"data": {
        "type": "reviewSubmissionItems",
        "relationships": {
            "reviewSubmission": {"data": {"type": "reviewSubmissions", "id": sid}},
            "appStoreVersion": {"data": {"type": "appStoreVersions", "id": version_id}},
        }}})
    state = api(token, "PATCH", f"/reviewSubmissions/{sid}", body={"data": {
        "id": sid, "type": "reviewSubmissions",
        "attributes": {"submitted": True}}})["data"]["attributes"].get("state")
    print(f"  + submitted for review — state: {state}", file=sys.stderr)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--app-id", required=True, help="App Store Connect numeric App ID")
    p.add_argument("--version", required=True, help="Version string, e.g. 0.1.9")
    p.add_argument("--key-id", required=True)
    p.add_argument("--issuer-id", required=True)
    p.add_argument("--key-file", required=True, type=Path)
    p.add_argument("--whats-new", default="",
                   help="'What's New' text (best-effort per-locale). Optional; "
                        "when empty the carried-over value is left as-is.")
    p.add_argument("--submit", action="store_true",
                   help="Attach the uploaded build and submit for review. Without "
                        "it the version is created but stays in "
                        "PREPARE_FOR_SUBMISSION.")
    args = p.parse_args()

    if not args.key_file.is_file():
        print(f"error: key file not found: {args.key_file}", file=sys.stderr)
        return 1

    whats_new = args.whats_new.strip()
    if len(whats_new) > 4000:  # Apple rejects whatsNew over 4000 chars
        whats_new = whats_new[:3960].rstrip() + "\n- ...and more."

    token = jwt_token(args.key_id, args.issuer_id, args.key_file)

    # 1. Find — or CREATE — the macOS App Store version for this build.
    versions = api(token, "GET",
                   f"/apps/{args.app_id}/appStoreVersions?filter[platform]=MAC_OS&limit=10")
    target_version = None
    for v in versions.get("data", []):
        attrs = v.get("attributes", {})
        if attrs.get("versionString") != args.version:
            continue
        state = attrs.get("appStoreState")
        if state in EDITABLE_VERSION_STATES:
            target_version = v
        else:
            print(f"macOS version {args.version} already exists in state "
                  f"{state} — nothing to do.", file=sys.stderr)
            return 0
        break

    if target_version is None:
        print(f"creating macOS appStoreVersion {args.version}", file=sys.stderr)
        try:
            target_version = api(token, "POST", "/appStoreVersions", body={"data": {
                "type": "appStoreVersions",
                "attributes": {
                    "platform": "MAC_OS",
                    "versionString": args.version,
                    "copyright": COPYRIGHT,
                    "releaseType": "AFTER_APPROVAL",
                },
                "relationships": {
                    "app": {"data": {"type": "apps", "id": args.app_id}}},
            }})["data"]
        except RuntimeError as e:
            # Apple allows only one pending version at a time.
            if "cannot create a new version" in str(e):
                pending = [
                    f'{v["attributes"]["versionString"]} '
                    f'({v["attributes"]["appStoreState"]})'
                    for v in versions.get("data", [])
                    if v["attributes"].get("appStoreState") != "READY_FOR_SALE"
                ]
                print(
                    f"error: cannot create macOS version {args.version} — another "
                    f"version is still pending: {', '.join(pending) or 'unknown'}.\n"
                    "       Release or withdraw it in App Store Connect, then "
                    "re-run. The build is already uploaded, so nothing is lost.",
                    file=sys.stderr,
                )
                return 1
            raise
    version_id = target_version["id"]

    # 2. Ensure the release type is AFTER_APPROVAL (auto-release after approval).
    api(token, "PATCH", f"/appStoreVersions/{version_id}", body={"data": {
        "type": "appStoreVersions", "id": version_id,
        "attributes": {"releaseType": "AFTER_APPROVAL"}}})
    print(f"macOS appStoreVersion {version_id} ({args.version}) → AFTER_APPROVAL",
          file=sys.stderr)

    # 3. Best-effort "What's New" per existing locale. Description / keywords /
    #    name are deliberately left as carried over from the prior version.
    if whats_new:
        locs = api(token, "GET",
                   f"/appStoreVersions/{version_id}/appStoreVersionLocalizations?limit=200")
        for loc in locs.get("data", []):
            loc_id = loc["id"]
            locale = loc.get("attributes", {}).get("locale", "?")
            try:
                api(token, "PATCH", f"/appStoreVersionLocalizations/{loc_id}",
                    body={"data": {"type": "appStoreVersionLocalizations",
                                   "id": loc_id,
                                   "attributes": {"whatsNew": whats_new}}})
                print(f"  + whatsNew updated for {locale}", file=sys.stderr)
            except RuntimeError as e:
                print(f"warning: whatsNew for {locale} failed: {e}", file=sys.stderr)

    # 4. Attach the uploaded build and submit for review.
    if args.submit:
        build_id = wait_for_build(token, args.app_id, args.version)
        if build_id is None:
            print("warning: no VALID macOS build to attach — version left in "
                  "PREPARE_FOR_SUBMISSION (auto-release is set; attach + submit "
                  "manually in App Store Connect).", file=sys.stderr)
        else:
            ensure_export_compliance(token, build_id)
            api(token, "PATCH",
                f"/appStoreVersions/{version_id}/relationships/build",
                body={"data": {"type": "builds", "id": build_id}})
            print(f"  + attached build {build_id}", file=sys.stderr)
            submit_for_review(token, args.app_id, version_id)
    else:
        print("--submit not given — version left in PREPARE_FOR_SUBMISSION",
              file=sys.stderr)

    print("App Store Connect version/submit complete.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
