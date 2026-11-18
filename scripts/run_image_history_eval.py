#!/usr/bin/env python3
"""
Roteiro fase 1: mesma tarefa N vezes em sequência.
Verifica gravação no DB e se a IA reutiliza histórico / mesma docker_image.

Uso:
  python scripts/run_image_history_eval.py
  python scripts/run_image_history_eval.py --runs 4 --api http://127.0.0.1:5000
"""

from __future__ import annotations

import argparse
import json
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
DEFAULT_SCRIPT = ROOT / "scripts" / "eval_tasks" / "task_requests_probe.py"
DEFAULT_DB = ROOT / "data" / "nodes.db"
TERMINAL = frozenset({"concluida", "falha", "cancelada", "erro_agente", "falha_alocacao"})


def _connect(db: Path) -> sqlite3.Connection:
    c = sqlite3.connect(str(db))
    c.row_factory = sqlite3.Row
    return c


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
        # alguns payloads só retornam message — busca a mais recente do user
        raise RuntimeError(f"submit sem task_id: {body}")
    return str(tid)


def wait_done(db: Path, task_id: str, timeout_s: float) -> dict:
    deadline = time.time() + timeout_s
    last = {}
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
            f"status={status} image={last.get('docker_image') or '—'} "
            f"src={last.get('ai_image_source') or '—'}",
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
    p = argparse.ArgumentParser()
    p.add_argument("--api", default="http://127.0.0.1:5000")
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--script", type=Path, default=DEFAULT_SCRIPT)
    p.add_argument("--user", default="lsouza")
    p.add_argument("--runs", type=int, default=4)
    p.add_argument("--timeout", type=float, default=600)
    p.add_argument("--gap", type=float, default=8.0, help="Pausa entre runs (s)")
    args = p.parse_args()

    if not args.script.is_file():
        print(f"Script não encontrado: {args.script}")
        return 1
    if not args.db.is_file():
        print(f"DB não encontrado: {args.db}")
        return 1

    print("=" * 64)
    print("FASE 1 — Histórico / mesma imagem Docker")
    print(f"API={args.api}  runs={args.runs}  script={args.script.name}")
    print("=" * 64)

    series = []
    for i in range(1, args.runs + 1):
        print(f"\n--- Run {i}/{args.runs} ---", flush=True)
        try:
            tid = submit(args.api, args.script, args.user)
        except Exception as e:
            # fallback: submit pode não devolver task_id
            print(f"submit erro/parse: {e}")
            # tenta pegar a tarefa mais recente analisando_ia/na_fila
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
                print("Não foi possível obter task_id")
                return 1
            print(f"  task_id inferido: {tid}")
        else:
            print(f"  submetida: {tid}", flush=True)

        row = wait_done(args.db, tid, args.timeout)
        lib = extract_librarian(row.get("ai_raw_response"))
        entry = {
            "run": i,
            "task_id": tid,
            "status": row.get("status"),
            "docker_image": row.get("docker_image"),
            "ai_image_source": row.get("ai_image_source"),
            "ai_category": row.get("ai_category"),
            "ai_detected_libraries": row.get("ai_detected_libraries"),
            "preferred_compute_layer": row.get("preferred_compute_layer"),
            "allocated_compute_layer": row.get("allocated_compute_layer"),
            "allocated_node_id": row.get("allocated_node_id"),
            "librarian_best_image": lib.get("best_image"),
            "librarian_alignment": lib.get("catalog_alignment"),
            "result": row.get("result"),
            "at": datetime.now(timezone.utc).isoformat(),
        }
        series.append(entry)
        print(
            f"  RESULTADO: status={entry['status']} docker_image={entry['docker_image']} "
            f"source={entry['ai_image_source']} librarian={entry['librarian_best_image']}",
            flush=True,
        )
        if i < args.runs:
            time.sleep(args.gap)

    # Resumo / consistência
    images = [s["docker_image"] for s in series]
    print("\n" + "=" * 64)
    print("RESUMO")
    for s in series:
        print(
            f"  run{s['run']}: {s['docker_image']} | src={s['ai_image_source']} | "
            f"status={s['status']} | node={s['allocated_node_id']}"
        )
    unique = set(images)
    print(f"\nImagens distintas: {unique}")
    if len(unique) == 1 and images[0]:
        print("OK: IA manteve a mesma imagem em todas as runs.")
    else:
        print("ATENÇÃO: imagens divergiram entre runs (ou vazias).")

    # Histórico no DB após runs
    conn = _connect(args.db)
    try:
        stats = conn.execute(
            """
            SELECT t.docker_image, tr.status_final, COUNT(*) n
            FROM tasks t
            JOIN task_results tr ON tr.task_id = t.task_id
            GROUP BY t.docker_image, tr.status_final
            ORDER BY n DESC
            """
        ).fetchall()
        print("\nAgregado DB (imagem × status_final):")
        for r in stats:
            print(f"  {r['docker_image']!r} / {r['status_final']}: {r['n']}")
    finally:
        conn.close()

    out = ROOT / "results" / "metrics_export" / f"image_history_eval_{int(time.time())}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(series, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nJSON: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
