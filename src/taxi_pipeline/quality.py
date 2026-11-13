"""Stage 3 — Data Quality gate: ทุก check มี id, severity, ค่าที่วัดได้, เกณฑ์ และผล (PASS/WARN/FAIL)
FAIL = หยุดไม่ให้เผยแพร่ curated output"""
from __future__ import annotations

import pandas as pd

from .config import FileSpec


def _chk(check_id, scope, status, value, threshold, message):
    return {"check_id": check_id, "scope": scope, "status": status,
            "value": value, "threshold": threshold, "message": message}


def _pct(n, total):
    return round(100.0 * n / total, 4) if total else 0.0


def run_checks(specs: list[FileSpec], ingest_results: dict, scans: dict, cfg: dict) -> pd.DataFrame:
    th = cfg["quality_thresholds"]
    rows = []
    for s in specs:
        f = s.filename
        ing = ingest_results.get(f, {})
        # Q1 มีไฟล์
        ok_file = ing.get("status") in ("downloaded", "cached")
        rows.append(_chk("Q01_file_present", f, "PASS" if ok_file else "FAIL", ing.get("status"), "downloaded|cached",
                         ing.get("detail", "file available") or "file available"))
        sc = scans.get(f)
        if not ok_file or sc is None:
            continue
        # Q2 อ่าน parquet ได้ + column ครบ
        rows.append(_chk("Q02_readable_schema", f, "PASS" if sc["ok"] else "FAIL",
                         "ok" if sc["ok"] else sc.get("error"), "readable & required columns",
                         sc.get("error") or "parquet readable, pickup/dropoff present"))
        if not sc["ok"]:
            continue
        n = sc["scanned_rows"]
        # Q3 metadata == จำนวนแถวที่สแกนจริง
        ok = sc["metadata_rows"] == n
        rows.append(_chk("Q03_rowcount_matches_metadata", f, "PASS" if ok else "FAIL",
                         f"{sc['metadata_rows']} vs {n}", "equal", "metadata rows equal scanned rows"))
        # Q4 pickup null
        v = _pct(sc["null_pickup"], n)
        st = "FAIL" if v > th["null_pickup_fail_pct"] else "WARN" if v > th["null_pickup_warn_pct"] else "PASS"
        rows.append(_chk("Q04_null_pickup_pct", f, st, v, f"warn>{th['null_pickup_warn_pct']} fail>{th['null_pickup_fail_pct']}",
                         f"{sc['null_pickup']} null pickup rows"))
        # Q5 pickup นอกช่วงวิเคราะห์
        v = _pct(sc["out_of_window"], n)
        st = "FAIL" if v > th["out_of_window_fail_pct"] else "WARN" if v > th["out_of_window_warn_pct"] else "PASS"
        rows.append(_chk("Q05_out_of_window_pct", f, st, v, f"warn>{th['out_of_window_warn_pct']} fail>{th['out_of_window_fail_pct']}",
                         f"{sc['out_of_window']} rows pickup outside window (min={sc['min_pickup']}, max={sc['max_pickup']}) -> excluded"))
        # Q6 dropoff < pickup
        v = _pct(sc["negative_duration"], n)
        st = "WARN" if v > th["negative_duration_warn_pct"] else "PASS"
        rows.append(_chk("Q06_negative_duration_pct", f, st, v, f"warn>{th['negative_duration_warn_pct']}",
                         f"{sc['negative_duration']} rows dropoff<pickup (kept: เราวัดจำนวน trip ไม่ใช่เวลา)"))
        # Q7 มี trip ในช่วงจริง
        rows.append(_chk("Q07_has_in_window_rows", f, "PASS" if sc["in_window"] > 0 else "FAIL",
                         sc["in_window"], ">0", "file contributes rows to window"))
        # Q8 daily รวมตรง in_window
        ok = sum(sc["daily"].values()) == sc["in_window"]
        rows.append(_chk("Q08_daily_sum_equals_in_window", f, "PASS" if ok else "FAIL",
                         f"{sum(sc['daily'].values())} vs {sc['in_window']}", "equal", "daily aggregation reconciles"))
    # ระดับชุดข้อมูล: ครบทุก type × month
    got = {(s.key, s.month) for s in specs if scans.get(s.filename, {}).get("ok")}
    exp = {(s.key, s.month) for s in specs}
    rows.append(_chk("Q09_coverage_complete", "ALL", "PASS" if got == exp else "FAIL",
                     f"{len(got)}/{len(exp)}", "all type x month", f"missing: {sorted(exp - got)}" if got != exp else "complete"))
    return pd.DataFrame(rows)


def gate_passed(dq: pd.DataFrame) -> bool:
    return not (dq["status"] == "FAIL").any()
