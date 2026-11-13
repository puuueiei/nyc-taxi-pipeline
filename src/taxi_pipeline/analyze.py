"""Stage 5 — Analyze: ตอบโจทย์ 'most used' จาก curated data + เทียบกับวิธี baseline (นับจาก metadata)"""
from __future__ import annotations

from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd


def rank(df: pd.DataFrame, col: str) -> pd.DataFrame:
    r = df.sort_values(col, ascending=False).reset_index(drop=True)
    r.insert(0, "rank", r.index + 1)
    r["share_percent"] = (r[col] / r[col].sum() * 100).round(2)
    return r


def analyze(daily: pd.DataFrame, lineage: pd.DataFrame) -> dict:
    curated = rank(daily.groupby("taxi_type", as_index=False)["trips"].sum()
                   .rename(columns={"trips": "total_trips"}), "total_trips")
    baseline = rank(lineage.groupby("taxi_type", as_index=False)["metadata_rows"].sum()
                    .rename(columns={"metadata_rows": "baseline_total_trips"}), "baseline_total_trips")
    cmp_ = curated.merge(baseline[["taxi_type", "baseline_total_trips"]], on="taxi_type")
    cmp_["excluded_vs_baseline"] = cmp_["baseline_total_trips"] - cmp_["total_trips"]
    cmp_["excluded_pct"] = (cmp_["excluded_vs_baseline"] / cmp_["baseline_total_trips"] * 100).round(4)
    top, second = curated.iloc[0], curated.iloc[1]
    return {
        "ranking": curated, "comparison": cmp_,
        "winner": top["taxi_type"], "winner_trips": int(top["total_trips"]), "winner_share": float(top["share_percent"]),
        "margin_x": round(top["total_trips"] / second["total_trips"], 2), "runner_up": second["taxi_type"],
        "same_winner_as_baseline": top["taxi_type"] == baseline.iloc[0]["taxi_type"],
    }


def make_charts(daily: pd.DataFrame, monthly: pd.DataFrame, ranking: pd.DataFrame, out_dir: Path, tag: str = ""):
    out_dir.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(8, 4))
    r = ranking.sort_values("total_trips")
    ax.barh(r["taxi_type"], r["total_trips"])
    for y, v in enumerate(r["total_trips"]):
        ax.text(v, y, f" {v:,}", va="center", fontsize=8)
    ax.set_xlabel("Trips in window (pickup date)"); ax.set_title(f"Most used taxi type {tag}".strip())
    fig.tight_layout(); fig.savefig(out_dir / "ranking.png", dpi=150); plt.close(fig)

    piv = monthly.pivot(index="pickup_month", columns="taxi_type", values="trips_in_window")
    fig, ax = plt.subplots(figsize=(8, 4))
    piv.plot(ax=ax, marker="o", logy=True)
    ax.set_ylabel("Trips (log scale)"); ax.set_title(f"Monthly trips by taxi type {tag}".strip())
    fig.tight_layout(); fig.savefig(out_dir / "monthly_trend.png", dpi=150); plt.close(fig)


def write_results_md(path: Path, res: dict, window, dq: pd.DataFrame, run_id: str, demo: bool = False):
    c = res["comparison"]
    n_warn, n_fail, n_pass = (int((dq["status"] == s).sum()) for s in ("WARN", "FAIL", "PASS"))
    lines = []
    if demo:
        lines += ["> ⚠️ **DEMO DATA (synthetic) — ตัวเลขนี้ไม่ใช่ผลจริงของ NYC TLC**", ""]
    lines += [
        "# Result — Which taxi type was the most used?", "",
        f"- Window (pickup date): **{window['start']} → {window['end']}**  | run_id: `{run_id}`",
        "- Metric: จำนวน trip records ที่มี pickup date อยู่ในช่วง (ไม่ใช่รายได้/ระยะทาง/ผู้โดยสาร)", "",
        f"## Answer: **{res['winner']}**",
        f"{res['winner_trips']:,} trips = {res['winner_share']}% of all four types; "
        f"{res['margin_x']}× the runner-up ({res['runner_up']}).", "",
        "## Ranking & comparison with Submission 1 method (metadata row count)", "",
        c.to_markdown(index=False), "",
        f"Same winner as baseline method: **{res['same_winner_as_baseline']}**", "",
        f"## Data quality: {n_pass} PASS / {n_warn} WARN / {n_fail} FAIL "
        "(ดูรายละเอียดที่ `reports/data_quality_report.csv`)", "",
        "![ranking](figures/ranking.png)", "", "![trend](figures/monthly_trend.png)", ""]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")
