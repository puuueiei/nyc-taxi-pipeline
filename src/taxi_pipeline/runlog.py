"""บันทึกหลักฐานการรัน: logs/run_<id>.jsonl (ทุก event) + reports/status.json (สรุป)"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)   # เขียนแล้วค่อย rename: ไฟล์ไม่ค้างครึ่งๆ กลางๆ ถ้า crash


class RunLog:
    def __init__(self, logs_dir: Path, reports_dir: Path):
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.path = Path(logs_dir) / f"run_{self.run_id}.jsonl"
        self.status_path = Path(reports_dir) / "status.json"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.stages: dict[str, dict] = {}
        self.started = now()

    def event(self, stage: str, status: str, **detail) -> None:
        rec = {"ts": now(), "run_id": self.run_id, "stage": stage, "status": status, **detail}
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
        print(f"[{rec['ts']}] {stage:<10} {status:<8} "
              + " ".join(f"{k}={v}" for k, v in detail.items() if not isinstance(v, (dict, list))))

    class _Stage:
        def __init__(self, log, name):
            self.log, self.name = log, name

        def __enter__(self):
            self.t0 = time.time()
            self.log.event(self.name, "START")
            return self

        def __exit__(self, et, ev, tb):
            secs = round(time.time() - self.t0, 2)
            if et:
                self.log.stages[self.name] = {"status": "FAILED", "seconds": secs, "error": repr(ev)}
                self.log.event(self.name, "FAILED", seconds=secs, error=repr(ev))
            else:
                extra = getattr(self, "summary", {})
                self.log.stages[self.name] = {"status": "OK", "seconds": secs, **extra}
                self.log.event(self.name, "OK", seconds=secs, **extra)
            return False

    def stage(self, name: str):
        return RunLog._Stage(self, name)

    def finish(self, overall: str, **summary) -> None:
        status = {"run_id": self.run_id, "overall_status": overall, "started": self.started,
                  "finished": now(), "stages": self.stages, **summary}
        atomic_write(self.status_path, json.dumps(status, indent=2, ensure_ascii=False, default=str))
        self.event("pipeline", overall, **{k: v for k, v in summary.items() if not isinstance(v, (dict, list))})
