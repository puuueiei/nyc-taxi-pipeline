"""Stage 2 — Profile/Scan: อ่านเฉพาะ column pickup/dropoff แบบ batch แล้วนับจริง
(ต่างจาก baseline ที่เชื่อ metadata อย่างเดียว) ผลต่อไฟล์ cache ไว้ตาม sha256"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from . import SCAN_VERSION
from .runlog import atomic_write


def _cache_path(state_dir: Path, filename: str) -> Path:
    return state_dir / "file_results" / f"{filename}.json"


def _cache_key(sha: str) -> str:
    # ไม่ใส่ช่วงวันที่ใน key: ผลสแกนเก็บ "ทุกวัน" ของไฟล์ แล้วค่อยกรองช่วงทีหลัง (apply_window)
    # -> เปลี่ยน/ขยายช่วงวิเคราะห์ได้โดยไม่ต้องสแกนไฟล์เก่าใหม่
    return f"{SCAN_VERSION}|{sha}"


def scan_file(path: Path, pickup: str, dropoff: str, batch_rows: int = 1_000_000) -> dict:
    out = {"file": path.name, "ok": False, "error": None}
    try:
        pf = pq.ParquetFile(path)
    except Exception as e:
        out["error"] = f"cannot open parquet: {e!r}"
        return out
    names = pf.schema_arrow.names
    missing = [c for c in (pickup, dropoff) if c not in names]
    out.update(metadata_rows=pf.metadata.num_rows, n_columns=len(names), missing_columns=missing)
    if missing:
        out["error"] = f"missing required columns: {missing}"
        return out

    scanned = null_p = null_d = neg = 0
    min_p = max_p = None
    daily: dict[str, int] = {}
    for batch in pf.iter_batches(batch_size=batch_rows, columns=[pickup, dropoff]):
        p, d = batch.column(0), batch.column(1)
        scanned += len(p)
        null_p += p.null_count
        null_d += d.null_count
        pd_ = p.cast(pa.date32())
        neg += pc.sum(pc.less(d.cast(p.type), p).cast(pa.int64())).as_py() or 0
        mm = pc.min_max(p).as_py()
        if mm["min"] is not None:
            min_p = mm["min"] if min_p is None else min(min_p, mm["min"])
            max_p = mm["max"] if max_p is None else max(max_p, mm["max"])
        vc = pc.value_counts(pc.drop_null(pd_))
        for item in vc.to_pylist():
            k = item["values"].isoformat()
            daily[k] = daily.get(k, 0) + item["counts"]
    out.update(ok=True, scanned_rows=scanned, null_pickup=null_p, null_dropoff=null_d,
               negative_duration=neg, min_pickup=str(min_p), max_pickup=str(max_p), daily_all=daily)
    return out


def apply_window(scan: dict, start: dt.date, end: dt.date) -> dict:
    """กรอง daily_all ตามช่วงวิเคราะห์ -> in_window / out_of_window / daily"""
    if not scan.get("ok"):
        return scan
    lo, hi = start.isoformat(), end.isoformat()
    daily = {d: n for d, n in scan["daily_all"].items() if lo <= d <= hi}
    in_win = sum(daily.values())
    return {**scan, "daily": daily, "in_window": in_win,
            "out_of_window": sum(scan["daily_all"].values()) - in_win}


def scan_with_cache(path: Path, sha: str, spec, cfg: dict, force=False) -> tuple[dict, bool]:
    """คืน (ผล, ใช้ cache หรือไม่)"""
    state = cfg["paths"]["state"]
    cp = _cache_path(state, path.name)
    key = _cache_key(sha)
    if cp.exists() and not force:
        try:
            cached = json.loads(cp.read_text(encoding="utf-8"))
            if cached.get("_key") == key:
                return cached, True
        except Exception:
            pass
    res = scan_file(path, spec.pickup, spec.dropoff, cfg["scan"]["batch_rows"])
    res.update(_key=key, taxi_type=spec.taxi_type, month=spec.month, sha256=sha)
    if res["ok"]:
        atomic_write(cp, json.dumps(res))
    return res, False
