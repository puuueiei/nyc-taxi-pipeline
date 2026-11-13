import datetime as dt
import functools
import http.server
import json
import threading
from pathlib import Path

import pandas as pd
import pytest

from taxi_pipeline.config import expected_files, load_config
from taxi_pipeline.demo import make_file, setup_demo
from taxi_pipeline.pipeline import run

CFG = Path(__file__).resolve().parents[1] / "config" / "pipeline.yaml"


@pytest.fixture
def cfg(tmp_path):
    return setup_demo(tmp_path / "ws", CFG)


def status(cfg):
    return json.loads((cfg["paths"]["reports"] / "status.json").read_text())


def test_end_to_end_and_independent_count(cfg):
    assert run(cfg, offline=True, demo=True) == 0
    daily = pd.read_parquet(cfg["paths"]["curated"] / "trips_daily.parquet")
    # นับซ้ำแบบตรงๆ ด้วย pandas เพื่อยืนยันว่า logic ของ pipeline ถูก
    w = cfg["analysis_window"]
    expect = {}
    for s in expected_files(cfg):
        df = pd.read_parquet(cfg["paths"]["raw"] / s.filename, columns=[s.pickup])
        d = df[s.pickup].dropna().dt.date
        expect[s.taxi_type] = expect.get(s.taxi_type, 0) + int(((d >= w["start"]) & (d <= w["end"])).sum())
    got = daily.groupby("taxi_type")["trips"].sum().to_dict()
    assert got == expect
    assert status(cfg)["overall_status"] == "SUCCESS"


def test_rerun_is_idempotent_and_uses_cache(cfg):
    run(cfg, offline=True, demo=True)
    first = pd.read_csv(cfg["paths"]["curated"] / "trips_daily.csv")
    run(cfg, offline=True, demo=True)
    second = pd.read_csv(cfg["paths"]["curated"] / "trips_daily.csv")
    pd.testing.assert_frame_equal(first, second)
    assert status(cfg)["stages"]["scan"]["reused_from_cache"] == 24


def test_quality_gate_blocks_corrupt_file(cfg):
    run(cfg, offline=True, demo=True)
    (cfg["paths"]["curated"] / "trips_daily.csv").unlink()
    bad = cfg["paths"]["raw"] / "green_tripdata_2024-03.parquet"
    bad.write_bytes(b"not a parquet file")
    assert run(cfg, offline=True, demo=True) == 2
    st = status(cfg)
    assert st["overall_status"] == "FAILED_QUALITY_GATE"
    assert not (cfg["paths"]["curated"] / "trips_daily.csv").exists()   # ไม่เผยแพร่ผลเมื่อ gate ปิด
    dq = pd.read_csv(cfg["paths"]["reports"] / "data_quality_report.csv")
    assert ((dq.status == "FAIL") & dq.scope.eq("green_tripdata_2024-03.parquet")).any()


def test_missing_file_fails_coverage(cfg):
    (cfg["paths"]["raw"] / "yellow_tripdata_2024-02.parquet").unlink()
    assert run(cfg, offline=True, demo=True) == 2
    dq = pd.read_csv(cfg["paths"]["reports"] / "data_quality_report.csv")
    assert (dq[dq.check_id == "Q09_coverage_complete"].status == "FAIL").all()


def test_out_of_window_rows_are_excluded(cfg):
    run(cfg, offline=True, demo=True)
    cmp_ = pd.read_csv(cfg["paths"]["reports"] / "baseline_comparison.csv")
    assert (cmp_["excluded_vs_baseline"] > 0).all()   # demo ใส่วันที่เพี้ยนไว้ทุกไฟล์


def test_incremental_update_processes_only_new_month(cfg):
    """เพิ่มเดือนใหม่ใน config -> ดาวน์โหลด/สแกนเฉพาะไฟล์ใหม่ (ผ่าน HTTP server จริงบนเครื่อง)"""
    # สร้างไฟล์ต้นทาง 7 เดือนใน 'server dir'
    server_dir = cfg["paths"]["raw"].parent / "server"
    cfg["analysis_window"]["end"] = dt.date(2024, 7, 31)
    for s in expected_files(cfg):
        make_file(s, server_dir / s.filename, 500, seed=hash(s.filename) % 1000)
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(server_dir))
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    try:
        cfg["source"]["base_url"] = f"http://127.0.0.1:{httpd.server_address[1]}"
        for p in cfg["paths"]["raw"].glob("*.parquet"):
            p.unlink()                               # เริ่มจาก raw ว่าง
        cfg["analysis_window"]["end"] = dt.date(2024, 6, 30)
        assert run(cfg) == 0
        assert status(cfg)["stages"]["ingest"]["downloaded"] == 24
        assert run(cfg) == 0                         # รันซ้ำ: ไม่โหลดอะไรเพิ่ม
        st = status(cfg)["stages"]
        assert st["ingest"]["downloaded"] == 0 and st["ingest"]["cached"] == 24
        cfg["analysis_window"]["end"] = dt.date(2024, 7, 31)    # มีข้อมูลใหม่ (กรกฎาคม)
        assert run(cfg) == 0
        st = status(cfg)["stages"]
        assert st["ingest"]["downloaded"] == 4 and st["ingest"]["cached"] == 24
        assert st["scan"]["scanned"] == 4 and st["scan"]["reused_from_cache"] == 24
        monthly = pd.read_parquet(cfg["paths"]["curated"] / "trips_monthly.parquet")
        assert "2024-07" in set(monthly.pickup_month)
    finally:
        httpd.shutdown()


def test_unpublished_month_reported_not_crash(cfg):
    cfg["analysis_window"]["end"] = dt.date(2024, 7, 31)   # offline + ไม่มีไฟล์ก.ค.
    assert run(cfg, offline=True, demo=True) == 2
    assert status(cfg)["overall_status"] == "FAILED_QUALITY_GATE"
