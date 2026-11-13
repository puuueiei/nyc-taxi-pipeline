"""โหลด config และสร้างรายการไฟล์ที่ต้องมี (expected files)"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path

import yaml


def load_config(path: str | Path, root: str | Path | None = None) -> dict:
    path = Path(path)
    cfg = yaml.safe_load(path.read_text(encoding="utf-8"))
    cfg["_root"] = Path(root) if root else path.resolve().parent.parent
    for k, v in cfg["paths"].items():
        cfg["paths"][k] = cfg["_root"] / v
    w = cfg["analysis_window"]
    w["start"] = _d(w["start"])
    w["end"] = _d(w["end"])
    if w["start"] > w["end"]:
        raise ValueError("analysis_window.start must be <= end")
    return cfg


def _d(x) -> dt.date:
    return x if isinstance(x, dt.date) else dt.date.fromisoformat(str(x))


def months_in_window(start: dt.date, end: dt.date) -> list[str]:
    out, y, m = [], start.year, start.month
    while (y, m) <= (end.year, end.month):
        out.append(f"{y}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


@dataclass(frozen=True)
class FileSpec:
    key: str            # yellow / green / fhv / fhvhv
    taxi_type: str      # ชื่อแสดงผล
    month: str          # YYYY-MM
    filename: str
    url: str
    pickup: str
    dropoff: str


def expected_files(cfg: dict) -> list[FileSpec]:
    w = cfg["analysis_window"]
    base = cfg["source"]["base_url"].rstrip("/")
    specs = []
    for key, t in cfg["taxi_types"].items():
        for month in months_in_window(w["start"], w["end"]):
            fn = f"{t['prefix']}_{month}.parquet"
            specs.append(FileSpec(key, t["display"], month, fn, f"{base}/{fn}",
                                  t["pickup"], t["dropoff"]))
    return specs
