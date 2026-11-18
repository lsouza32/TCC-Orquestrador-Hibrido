#!/usr/bin/env python3
"""
Fase 2b — realocação pós-falha.

1) Submete tarefa leve.
2) Quando status=executando, apaga o Pod do Job (força Failed).
3) Espera realocação (na_fila → outro nó) e conclusão.

Requer: kubectl no PATH *ou* execução via docker exec no context-server.

Uso:
  python scripts/run_reallocation_eval.py --api http://127.0.0.1:5000
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "eval_tasks" / "task_edge_light.py"
DEFAULT_DB = ROOT / "data" / "nodes.db"
TERMINAL = frozenset({"concluida", "falha", "cancelada", "erro_agente", "falha_alocacao"})


def _connect(db: Path) -> sqlite3.Connection:
    c = sqlite3.connect(str(db))
    c.row_factory = sqlite3.Row
    return c


def submit(api: str, user_id: str) -> str:
    with SCRIPT.open("rb") as f:
        r = requests.post(
            f"{api.rstrip('/')}/submit_task",
            data={"userId": user_id, "taskType": "auto"},
            files={"file": (SCRIPT.name, f, "text/x-python")},
            timeout=60,
        )
    r.raise_for_status()
    return str(r.json()["task_id"])


def delete_task_pods(task_id: str) -> str:
    """Apaga pods com label task_id via context-server (tem kubeconfig)."""
    py = f"""
from kubernetes import client, config
config.load_kube_config()
c = client.CoreV1Api()
deleted = []
for p in c.list_namespaced_pod('default', label_selector='task_id={task_id}').items:
    c.delete_namespaced_pod(p.metadata.name, 'default', grace_period_seconds=0)
    deleted.append(p.metadata.name)
print(','.join(deleted) if deleted else 'none')
"""
    r = subprocess.run(
        ["docker", "exec", "context-server", "python", "-c", py],
        capture_output=True,
        text=True,
        timeout=60,
    )
    if r.returncode != 0:
        return f"ERR:{r.stderr[:200]}"
    return (r.stdout or "").strip()


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--api", default="http://127.0.0.1:5000")
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--user", default="lsouza")
    p.add_argument("--timeout", type=float, default=1200)
    args = p.parse_args()

    print("=" * 64)
    print("FASE 2b — Realocação pós-falha")
    print("=" * 64)

    tid = submit(args.api, args.user)
    print(f"submetida: {tid}", flush=True)

    killed = False
    first_node = None
    deadline = time.time() + args.timeout
    timeline = []

    while time.time() < deadline:
        conn = _connect(args.db)
        try:
            row = dict(conn.execute("SELECT * FROM tasks WHERE task_id=?", (tid,)).fetchone() or {})
            res = conn.execute(
                "SELECT status_final, duration_seconds FROM task_results WHERE task_id=?",
                (tid,),
            ).fetchone()
        finally:
            conn.close()

        st = row.get("status")
        node = row.get("allocated_node_id")
        print(
            f"  [{datetime.now().strftime('%H:%M:%S')}] status={st} node={node}",
            flush=True,
        )
        timeline.append({"t": time.time(), "status": st, "node": node})

        if st == "executando" and not killed and node:
            first_node = node
            time.sleep(8)  # deixa o Job criar o Pod
            out = delete_task_pods(tid)
            print(f"  >>> pods apagados: {out}", flush=True)
            killed = True
            timeline.append({"t": time.time(), "event": "pod_deleted", "pods": out})

        if st in TERMINAL:
            raw = row.get("ai_raw_response") or ""
            realloc = {}
            try:
                realloc = (json.loads(raw) or {}).get("reallocation") or {}
            except Exception:
                pass
            result = {
                "task_id": tid,
                "final_status": st,
                "first_node": first_node,
                "final_node": node,
                "killed": killed,
                "reallocation": realloc,
                "result": dict(res) if res else None,
                "timeline": timeline,
                "success": bool(
                    killed
                    and st == "concluida"
                    and realloc.get("attempts", 0) >= 1
                    and node
                    and node != first_node
                ),
                "at": datetime.now(timezone.utc).isoformat(),
            }
            # sucesso parcial: realocou e concluiu mesmo no mesmo hostname após retry
            if killed and st == "concluida" and int(realloc.get("attempts") or 0) >= 1:
                result["success_reallocated"] = True
            print("\nRESUMO 2b")
            print(json.dumps({k: v for k, v in result.items() if k != "timeline"}, ensure_ascii=False, indent=2))
            out = ROOT / "results" / "metrics_export" / f"reallocation_eval_{int(time.time())}.json"
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"JSON: {out}")
            return 0

        time.sleep(5)

    print("Timeout")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
