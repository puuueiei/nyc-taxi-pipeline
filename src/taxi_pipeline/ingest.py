"""Stage 1 — Ingest: ดาวน์โหลดแบบ incremental + เก็บ manifest (size, sha256, เวลา)"""
from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path

import requests

from .config import FileSpec
from .runlog import atomic_write, now


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def load_manifest(state_dir: Path) -> dict:
    p = state_dir / "manifest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}


def save_manifest(state_dir: Path, manifest: dict) -> None:
    atomic_write(state_dir / "manifest.json", json.dumps(manifest, indent=2, sort_keys=True))


def _remote_size(session, url, timeout):
    r = session.head(url, timeout=timeout, allow_redirects=True)
    if r.status_code == 404 or r.status_code == 403:
        return None, r.status_code           # CloudFront ส่ง 403 เมื่อไฟล์ยังไม่ถูกเผยแพร่
    r.raise_for_status()
    cl = r.headers.get("Content-Length")
    return (int(cl) if cl else -1), r.status_code


def _download(session, url, dest: Path, timeout, retries) -> int:
    part = dest.with_suffix(dest.suffix + ".part")
    last = None
    for attempt in range(1, retries + 1):
        try:
            with session.get(url, stream=True, timeout=timeout) as r:
                r.raise_for_status()
                expected = int(r.headers.get("Content-Length", -1))
                n = 0
                with part.open("wb") as f:
                    for chunk in r.iter_content(1 << 20):
                        f.write(chunk)
                        n += len(chunk)
            if expected >= 0 and n != expected:
                raise IOError(f"truncated download: got {n} of {expected} bytes")
            part.replace(dest)
            return n
        except Exception as e:           # retry พร้อม backoff
            last = e
            part.unlink(missing_ok=True)
            if attempt < retries:
                time.sleep(2 ** attempt)
    raise RuntimeError(f"download failed after {retries} attempts: {last}")


def ingest_file(spec: FileSpec, cfg: dict, manifest: dict, session, offline=False, force=False) -> dict:
    """คืน dict สถานะ: downloaded | cached | missing | failed"""
    raw = cfg["paths"]["raw"]
    raw.mkdir(parents=True, exist_ok=True)
    dest = raw / spec.filename
    timeout = cfg["source"]["timeout_seconds"]
    retries = cfg["source"]["retries"]
    res = {"file": spec.filename, "taxi_type": spec.taxi_type, "month": spec.month, "url": spec.url}
    try:
        need = force or not dest.exists()
        if not need and not offline:
            remote, code = _remote_size(session, spec.url, timeout)
            if remote is not None and remote >= 0 and remote != dest.stat().st_size:
                need = True              # ไฟล์ต้นทางถูกแก้ไข/ขนาดไม่ตรง -> โหลดใหม่
        if need:
            if offline:
                return {**res, "status": "missing", "detail": "offline mode and file not on disk"}
            remote, code = _remote_size(session, spec.url, timeout)
            if remote is None:
                return {**res, "status": "missing", "detail": f"HTTP {code} (not published yet?)"}
            _download(session, spec.url, dest, timeout, retries)
            status = "downloaded"
        else:
            status = "cached"
        size = dest.stat().st_size
        prev = manifest.get(spec.filename, {})
        if status == "cached" and prev.get("size") == size and prev.get("sha256"):
            digest = prev["sha256"]
        else:
            digest = sha256_file(dest)
        manifest[spec.filename] = {
            "url": spec.url, "size": size, "sha256": digest,
            "fetched_at": now() if status == "downloaded" else prev.get("fetched_at", now()),
        }
        return {**res, "status": status, "size": size, "sha256": digest}
    except Exception as e:
        return {**res, "status": "failed", "detail": repr(e)}


def new_session():
    s = requests.Session()
    s.headers["User-Agent"] = "nyc-taxi-pipeline/2.0 (student project)"
    return s
