"""Orchestrator + CLI.   python -m taxi_pipeline run [--offline] [--force]"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .analyze import analyze, make_charts, write_results_md
from .config import expected_files, load_config
from .curate import build_curated, write_curated
from .ingest import ingest_file, load_manifest, new_session, save_manifest
from .quality import gate_passed, run_checks
from .runlog import RunLog, atomic_write
from .scan import apply_window, scan_with_cache


def run(cfg: dict, offline=False, force=False, demo=False) -> int:
    P = cfg["paths"]
    log = RunLog(P["logs"], P["reports"])
    specs = expected_files(cfg)
    manifest = load_manifest(P["state"])
    ingest_results, scans, cache_hits = {}, {}, 0
    log.event("pipeline", "START", expected_files=len(specs), offline=offline, force=force)
    try:
        # 1) INGEST
        with log.stage("ingest") as st:
            session = None if offline else new_session()
            for s in specs:
                r = ingest_file(s, cfg, manifest, session, offline=offline, force=force)
                ingest_results[s.filename] = r
                log.event("ingest", r["status"].upper(), file=s.filename)
            save_manifest(P["state"], manifest)
            counts = {k: sum(r["status"] == k for r in ingest_results.values())
                      for k in ("downloaded", "cached", "missing", "failed")}
            st.summary = counts

        # 2) SCAN
        with log.stage("scan") as st:
            for s in specs:
                if ingest_results[s.filename]["status"] not in ("downloaded", "cached"):
                    continue
                res, hit = scan_with_cache(P["raw"] / s.filename, manifest[s.filename]["sha256"], s, cfg, force)
                w = cfg["analysis_window"]
                res = apply_window(res, w["start"], w["end"])
                scans[s.filename] = res
                cache_hits += hit
                log.event("scan", "CACHED" if hit else "SCANNED", file=s.filename,
                          rows=res.get("scanned_rows"), in_window=res.get("in_window"))
            st.summary = {"scanned": len(scans) - cache_hits, "reused_from_cache": cache_hits}

        # 3) QUALITY GATE
        with log.stage("quality") as st:
            dq = run_checks(specs, ingest_results, scans, cfg)
            P["reports"].mkdir(parents=True, exist_ok=True)
            dq.to_csv(P["reports"] / "data_quality_report.csv", index=False)
            atomic_write(P["reports"] / "data_quality_report.json", dq.to_json(orient="records", indent=2))
            passed = gate_passed(dq)
            st.summary = {"PASS": int((dq.status == "PASS").sum()), "WARN": int((dq.status == "WARN").sum()),
                          "FAIL": int((dq.status == "FAIL").sum()), "gate": "OPEN" if passed else "CLOSED"}
        if not passed:
            log.event("quality", "GATE_CLOSED", detail="FAIL checks -> curated output NOT published")
            log.finish("FAILED_QUALITY_GATE", failed_checks=dq[dq.status == "FAIL"][["check_id", "scope"]].to_dict("records"))
            return 2

        # 4) CURATE
        with log.stage("curate") as st:
            daily, monthly, lineage = build_curated(specs, scans, ingest_results, manifest)
            st.summary = {"files": len(write_curated(P["curated"], daily, monthly, lineage)),
                          "daily_rows": len(daily)}

        # 5) ANALYZE
        with log.stage("analyze") as st:
            res = analyze(daily, lineage)
            make_charts(daily, monthly, res["ranking"], P["reports"] / "figures", "(DEMO)" if demo else "")
            write_results_md(P["reports"] / "results.md", res, cfg["analysis_window"], dq, log.run_id, demo)
            res["ranking"].to_csv(P["reports"] / "ranking.csv", index=False)
            res["comparison"].to_csv(P["reports"] / "baseline_comparison.csv", index=False)
            st.summary = {"winner": res["winner"], "winner_trips": res["winner_trips"]}

        # 6) RECONCILE (ผลสุดท้าย = ผลรวมจากแหล่งต้นทางโดยตรง)
        with log.stage("reconcile") as st:
            assert int(daily["trips"].sum()) == int(lineage["in_window_rows"].sum()), "daily != lineage"
            assert int(monthly["trips_in_window"].sum()) == int(res["ranking"]["total_trips"].sum()), "monthly != ranking"
            st.summary = {"total_trips": int(daily["trips"].sum()), "reconciled": True}

        log.finish("SUCCESS", winner=res["winner"], total_trips=int(daily["trips"].sum()),
                   dq_warnings=int((dq.status == "WARN").sum()), demo_data=demo)
        return 0
    except Exception as e:
        log.finish("ERROR", error=repr(e))
        raise


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="taxi_pipeline")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="รัน pipeline ทั้งเส้น (idempotent)")
    r.add_argument("--config", default="config/pipeline.yaml")
    r.add_argument("--offline", action="store_true", help="ไม่ใช้เน็ต ใช้ไฟล์ใน data/raw เท่านั้น")
    r.add_argument("--force", action="store_true", help="ดาวน์โหลด/สแกนใหม่ทั้งหมด")
    s = sub.add_parser("status", help="แสดง reports/status.json ล่าสุด")
    s.add_argument("--config", default="config/pipeline.yaml")
    d = sub.add_parser("demo", help="รันกับข้อมูลสังเคราะห์ขนาดเล็ก (ทดสอบว่า pipeline ทำงาน)")
    d.add_argument("--dir", default="demo_workspace")
    a = ap.parse_args(argv)
    if a.cmd == "status":
        cfg = load_config(a.config)
        print((cfg["paths"]["reports"] / "status.json").read_text(encoding="utf-8"))
        return 0
    if a.cmd == "demo":
        from .demo import setup_demo
        cfg = setup_demo(Path(a.dir))
        return run(cfg, offline=True, demo=True)
    return run(load_config(a.config), offline=a.offline, force=a.force)


if __name__ == "__main__":
    sys.exit(main())
