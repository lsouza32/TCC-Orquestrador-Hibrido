#!/usr/bin/env python3
"""
EXP-01 — braço Orquestrador Híbrido (10 iterações seriais).

Cold start: limpa tasks/task_results no SQLite.
Depois submete task_exp1_memory.py via POST /submit_task e aguarda
status terminal no DB antes da próxima corrida.

Uso:
  python scripts/run_exp1_hybrid.py
  python scripts/run_exp1_hybrid.py --runs 10 --api http://127.0.0.1:5000
"""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    print("Instale requests: pip install requests", file=sys.stderr)
    raise SystemExit(1)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCRIPT = ROOT / "scripts" / "eval_tasks" / "task_exp1_memory.py"
DEFAULT_DB = ROOT / "data" / "nodes.db"
TERMINAL = frozenset({"concluida", "falha", "cancelada", "erro_agente", "falha_alocacao"})


def _connect(db: Path) -> sqlite3.Connection:
    c = sqlite3.connect(str(db))
    c.row_factory = sqlite3.Row
    return c


def cold_start_reset(db: Path, keep_uploads: bool = False) -> None:
    """Espelha scripts/reset_eval_db.py — limpa tasks/task_results para Cold Start."""
    if not db.is_file():
        raise FileNotFoundError(f"DB não encontrado: {db}")

    backup = db.with_name(f"{db.stem}.bak_{int(time.time())}{db.suffix}")
    shutil.copy2(db, backup)
    print(f"Cold start backup: {backup}", flush=True)

    conn = sqlite3.connect(str(db))
    try:
        cur = conn.cursor()
        n_tasks = cur.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
        n_res = cur.execute("SELECT COUNT(*) FROM task_results").fetchone()[0]
        cur.execute("DELETE FROM task_results")
        cur.execute("DELETE FROM tasks")
        try:
            cur.execute(
                "UPDATE nodes SET cpu_reserved_threads=0, memory_reserved_gb=0, disk_reserved_gb=0"
            )
        except sqlite3.Error:
            pass
        conn.commit()
        print(f"Cold start: removidos {n_tasks} tasks, {n_res} results", flush=True)
    finally:
        conn.close()

    if not keep_uploads:
        up = ROOT / "uploaded_tasks"
        if up.is_dir():
            removed = 0
            for p in up.iterdir():
                if p.is_file() and not p.name.startswith("."):
                    p.unlink(missing_ok=True)
                    removed += 1
            print(f"Cold start: uploads removidos={removed}", flush=True)


def submit(api: str, script: Path, user_id: str) -> str:
    url = f"{api.rstrip('/')}/submit_task"
    with script.open("rb") as f:
        r = requests.post(
            url,
            data={"userId": user_id, "taskType": "auto"},
            files={"file": (script.name, f, "text/x-python")},
            timeout=60,
        )
    r.raise_for_status()
    body = r.json()
    tid = body.get("task_id") or body.get("taskId")
    if not tid:
        raise RuntimeError(f"submit sem task_id: {body}")
    return str(tid)


def wait_done(db: Path, task_id: str, timeout_s: float) -> dict:
    deadline = time.time() + timeout_s
    last: dict = {}
    while time.time() < deadline:
        conn = _connect(db)
        try:
            row = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            res = conn.execute(
                "SELECT status_final, duration_seconds, cpu_peak_percent, ram_peak_mb, "
                "prediction_delta_ram, detected_libraries FROM task_results WHERE task_id=?",
                (task_id,),
            ).fetchone()
        finally:
            conn.close()
        if not row:
            time.sleep(2)
            continue
        last = dict(row)
        status = last.get("status")
        print(
            f"  [{datetime.now().strftime('%H:%M:%S')}] {task_id[:8]}… "
            f"status={status} image={last.get('docker_image') or '—'}",
            flush=True,
        )
        if status in TERMINAL:
            out = dict(last)
            if res:
                out["result"] = dict(res)
            return out
        time.sleep(5)
    raise TimeoutError(f"Timeout aguardando {task_id} (último status={last.get('status')})")


def extract_librarian(ai_raw: str | None) -> dict:
    if not ai_raw:
        return {}
    try:
        data = json.loads(ai_raw)
    except (json.JSONDecodeError, TypeError):
        return {}
    if not isinstance(data, dict):
        return {}
    trace = data.get("multi_agent_trace") or {}
    lib = trace.get("librarian_output") or data.get("librarian_output") or {}
    return lib if isinstance(lib, dict) else {}


def main() -> int:
    p = argparse.ArgumentParser(description="EXP-01 híbrido: 10 submits seriais com cold start")
    p.add_argument("--api", default="http://127.0.0.1:5000")
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--script", type=Path, default=DEFAULT_SCRIPT)
    p.add_argument("--user", default="lsouza")
    p.add_argument("--runs", type=int, default=10)
    p.add_argument("--timeout", type=float, default=600.0)
    p.add_argument("--gap", type=float, default=8.0, help="Pausa entre runs (s)")
    p.add_argument("--keep-uploads", action="store_true")
    p.add_argument("--skip-reset", action="store_true", help="Não limpar SQLite (debug)")
    args = p.parse_args()
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from _runlog import start_run_log
    start_run_log("exp1", "hybrid")

    if not args.script.is_file():
        print(f"Script não encontrado: {args.script}", file=sys.stderr)
        return 1
    if not args.db.is_file():
        print(f"DB não encontrado: {args.db}", file=sys.stderr)
        return 1

    print("=" * 64)
    print("EXP-01 — Orquestrador Híbrido")
    print(f"API={args.api}  runs={args.runs}  script={args.script.name}")
    print("=" * 64)

    if not args.skip_reset:
        cold_start_reset(args.db, keep_uploads=args.keep_uploads)
    else:
        print("AVISO: --skip-reset — histórico SQLite preservado", flush=True)

    series: list[dict] = []
    for i in range(1, args.runs + 1):
        print(f"\n--- Run {i}/{args.runs} ---", flush=True)
        t0 = time.monotonic()
        try:
            tid = submit(args.api, args.script, args.user)
        except Exception as e:
            print(f"submit erro/parse: {e}")
            conn = _connect(args.db)
            try:
                row = conn.execute(
                    "SELECT task_id FROM tasks WHERE user_id=? ORDER BY submitted_at DESC LIMIT 1",
                    (args.user,),
                ).fetchone()
                tid = row["task_id"] if row else None
            finally:
                conn.close()
            if not tid:
                print("Não foi possível obter task_id", file=sys.stderr)
                return 1
            print(f"  task_id inferido: {tid}")
        else:
            print(f"  submetida: {tid}", flush=True)

        try:
            row = wait_done(args.db, tid, args.timeout)
        except TimeoutError as e:
            print(f"  ERRO: {e}", file=sys.stderr)
            series.append(
                {
                    "run": i,
                    "task_id": tid,
                    "status": "Timeout",
                    "duration_wall_s": round(time.monotonic() - t0, 3),
                    "at": datetime.now(timezone.utc).isoformat(),
                }
            )
            return 1

        wall = time.monotonic() - t0
        lib = extract_librarian(row.get("ai_raw_response"))
        result = row.get("result") or {}
        entry = {
            "run": i,
            "task_id": tid,
            "status": row.get("status"),
            "docker_image": row.get("docker_image"),
            "ai_image_source": row.get("ai_image_source"),
            "preferred_compute_layer": row.get("preferred_compute_layer"),
            "allocated_compute_layer": row.get("allocated_compute_layer"),
            "allocated_node_id": row.get("allocated_node_id"),
            "librarian_best_image": lib.get("best_image"),
            "ceiling_signals": lib.get("ceiling_signals"),
            "safety_margin": lib.get("safety_margin"),
            "result": result,
            "ram_peak_mb": result.get("ram_peak_mb"),
            "cpu_peak_percent": result.get("cpu_peak_percent"),
            "prediction_delta_ram": result.get("prediction_delta_ram"),
            "duration_wall_s": round(wall, 3),
            "at": datetime.now(timezone.utc).isoformat(),
        }
        series.append(entry)
        print(
            f"  RESULTADO: status={entry['status']} image={entry['docker_image']} "
            f"ram_peak_mb={entry['ram_peak_mb']} wall_s={wall:.1f}",
            flush=True,
        )
        if i < args.runs:
            time.sleep(args.gap)

    print("\n" + "=" * 64)
    print("RESUMO HÍBRIDO")
    for s in series:
        print(
            f"  run{s['run']}: {s.get('docker_image')} | status={s['status']} | "
            f"ram_peak_mb={s.get('ram_peak_mb')} | node={s.get('allocated_node_id')}"
        )

    out = (
        ROOT
        / "results"
        / "metrics_export"
        / "exp1"
        / "hibrido"
        / f"exp1_hybrid_{int(time.time())}.json"
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(series, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nJSON: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
