"""ข้อมูลสังเคราะห์ที่ใช้ schema เดียวกับ TLC (ชื่อ column ต่างกันตาม type) ไว้ทดสอบ pipeline
ใส่ความผิดปกติจงใจ: pickup นอกช่วง, pickup เป็น null, dropoff < pickup"""
from __future__ import annotations

import shutil
from pathlib import Path

import numpy as np
import pandas as pd

from .config import expected_files, load_config

SIZES = {"yellow": 3000, "green": 60, "fhv": 900, "fhvhv": 12000}


def make_file(spec, path: Path, n: int, seed: int, with_issues=True):
    rng = np.random.default_rng(seed)
    y, m = map(int, spec.month.split("-"))
    start = pd.Timestamp(year=y, month=m, day=1)
    days = start.days_in_month
    pick = start + pd.to_timedelta(rng.integers(0, days * 86400, n), unit="s")
    drop = pick + pd.to_timedelta(rng.integers(60, 3600, n), unit="s")
    pick, drop = pd.Series(pick).astype("datetime64[us]"), pd.Series(drop).astype("datetime64[us]")
    if with_issues:
        k = max(1, n // 200)
        pick.iloc[:k] = pd.Timestamp("2002-12-31 10:00")            # วันที่เพี้ยน
        drop.iloc[k:2 * k] = drop.iloc[k:2 * k] - pd.Timedelta(days=1)  # dropoff < pickup
        if spec.key == "fhv":
            pick.iloc[3 * k:4 * k] = pd.NaT                         # pickup ว่าง
    df = pd.DataFrame({spec.pickup: pick, spec.dropoff: drop, "other_col": rng.integers(0, 5, n)})
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False, row_group_size=2000)


def setup_demo(root: Path, config_path="config/pipeline.yaml") -> dict:
    root = Path(root)
    if root.exists():
        shutil.rmtree(root)
    cfg = load_config(config_path, root=root)
    for k, v in cfg["paths"].items():
        cfg["paths"][k] = root / Path(v).name
    cfg["scan"]["batch_rows"] = 2500
    for i, s in enumerate(expected_files(cfg)):
        make_file(s, cfg["paths"]["raw"] / s.filename, SIZES[s.key], seed=i)
    return cfg
