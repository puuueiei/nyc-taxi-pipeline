"""Stage 4 — Curate/Integrate: รวม 4 schema -> ตารางมาตรฐานเดียว (taxi_type, pickup_date, trips)
แล้วสร้างตารางสรุปรายเดือน + lineage ของไฟล์ต้นทาง เขียนเป็น parquet+csv (atomic)"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd

from .config import FileSpec


def build_curated(specs: list[FileSpec], scans: dict, ingest_results: dict, manifest: dict):
    daily_rows, lineage_rows, month_rows = [], [], []
    for s in specs:
        sc = scans[s.filename]
        for d, n in sc["daily"].items():
            daily_rows.append({"taxi_type": s.taxi_type, "pickup_date": dt.date.fromisoformat(d), "trips": n})
        m = manifest.get(s.filename, {})
        lineage_rows.append({
            "taxi_type": s.taxi_type, "file_month": s.month, "file": s.filename, "source_url": s.url,
            "size_bytes": m.get("size"), "sha256": m.get("sha256"), "fetched_at": m.get("fetched_at"),
            "metadata_rows": sc["metadata_rows"], "in_window_rows": sc["in_window"],
            "out_of_window_rows": sc["out_of_window"], "null_pickup_rows": sc["null_pickup"],
        })
    daily = (pd.DataFrame(daily_rows).groupby(["taxi_type", "pickup_date"], as_index=False)["trips"].sum()
             .sort_values(["taxi_type", "pickup_date"]).reset_index(drop=True))
    daily["pickup_month"] = pd.to_datetime(daily["pickup_date"]).dt.strftime("%Y-%m")
    monthly = (daily.groupby(["taxi_type", "pickup_month"], as_index=False)["trips"].sum()
               .rename(columns={"trips": "trips_in_window"}))
    lineage = pd.DataFrame(lineage_rows).sort_values(["taxi_type", "file_month"]).reset_index(drop=True)
    return daily[["taxi_type", "pickup_date", "pickup_month", "trips"]], monthly, lineage


def write_curated(out_dir: Path, daily, monthly, lineage) -> list[str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for name, df in (("trips_daily", daily), ("trips_monthly", monthly), ("file_lineage", lineage)):
        for ext in ("parquet", "csv"):
            p = out_dir / f"{name}.{ext}"
            tmp = p.with_suffix(p.suffix + ".tmp")
            (df.to_parquet(tmp, index=False) if ext == "parquet" else df.to_csv(tmp, index=False))
            tmp.replace(p)
            written.append(str(p.name))
    return written
