#!/usr/bin/env python3
"""
Fase 2a — afinidade Edge / Fog / Cloud.

Submete 3 tarefas (leve, CPU, ML), espera conclusão e verifica:
  preferred_compute_layer × allocated_compute_layer × hostname.

Uso:
  python scripts/run_layer_affinity_eval.py --api http://127.0.0.1:5000
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
EVAL = ROOT / "scripts" / "eval_tasks"
DEFAULT_DB = ROOT / "data" / "nodes.db"
TERMINAL = frozenset({"concluida", "falha", "cancelada", "erro_agente", "falha_alocacao"})

CASES = [
    {
        "name": "edge_light",
        "script": EVAL / "task_edge_light.py",
        "expected_preferred": "edge",
        "expected_host_substr": "worker2",  # tipado edge no Kind
    },
    {
        "name": "fog_cpu",
        "script": EVAL / "task_fog_cpu.py",
        "expected_preferred": "fog",
        "expected_host_substr": "worker",  # tcc-cluster-worker (fog); cuidado com worker2
    },
    {
        "name": "cloud_ml",
        "script": EVAL / "task_cloud_ml.py",
        "expected_preferred": "cloud",
        "expected_host_substr": "control-plane",
    },
]


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
    tid = r.json().get("task_id")
    if not tid:
        raise RuntimeError(r.json())
    return str(tid)


def wait_done(db: Path, task_id: str, timeout_s: float) -> dict:
    deadline = time.time() + timeout_s
    last = {}
    while time.time() < deadline:
        conn = _connect(db)
        try:
            row = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            res = conn.execute(
                "SELECT status_final, duration_seconds, cpu_peak_percent, ram_peak_mb "
                "FROM task_results WHERE task_id=?",
                (task_id,),
            ).fetchone()
        finally:
            conn.close()
        if not row:
            time.sleep(3)
            continue
        last = dict(row)
        st = last.get("status")
        print(
            f"  [{datetime.now().strftime('%H:%M:%S')}] {task_id[:8]}… "
            f"status={st} pref={last.get('preferred_compute_layer')} "
            f"alloc={last.get('allocated_compute_layer')} "
            f"node={last.get('allocated_node_id')} img={last.get('docker_image')}",
            flush=True,
        )
        if st in TERMINAL:
            out = dict(last)
            if res:
                out["result"] = dict(res)
            return out
        time.sleep(6)
    raise TimeoutError(f"Timeout {task_id} last={last.get('status')}")


def host_ok(allocated_node_id: str | None, expected_substr: str, case_name: str) -> bool:
    host = (allocated_node_id or "")
    if host.startswith("k8s:"):
        host = host[4:]
    # fog case: "worker" also matches "worker2" — exigir fog exato
    if case_name == "fog_cpu":
        return host == "tcc-cluster-worker" or host.endswith("-worker") and "worker2" not in host
    if case_name == "edge_light":
        return "worker2" in host
    if case_name == "cloud_ml":
        return "control-plane" in host
    return expected_substr in host


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--api", default="http://127.0.0.1:5000")
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--user", default="lsouza")
    p.add_argument("--timeout", type=float, default=900)
    p.add_argument("--gap", type=float, default=10)
    args = p.parse_args()

    print("=" * 64)
    print("FASE 2a — Afinidade Edge / Fog / Cloud")
    print("=" * 64)

    series = []
    for case in CASES:
        print(f"\n--- Caso {case['name']} (esperado pref={case['expected_preferred']}) ---", flush=True)
        tid = submit(args.api, case["script"], args.user)
        print(f"  submetida: {tid}", flush=True)
        row = wait_done(args.db, tid, args.timeout)
        pref = row.get("preferred_compute_layer")
        alloc = row.get("allocated_compute_layer")
        node = row.get("allocated_node_id")
        pref_ok = (pref == case["expected_preferred"])
        host_match = host_ok(node, case["expected_host_substr"], case["name"])
        layer_match = (pref == alloc) if pref and alloc else False
        entry = {
            "case": case["name"],
            "task_id": tid,
            "expected_preferred": case["expected_preferred"],
            "preferred_compute_layer": pref,
            "allocated_compute_layer": alloc,
            "allocated_node_id": node,
            "docker_image": row.get("docker_image"),
            "status": row.get("status"),
            "result": row.get("result"),
            "checks": {
                "preferred_matches_expected": pref_ok,
                "preferred_equals_allocated": layer_match,
                "hostname_matches_layer_node": host_match,
            },
            "at": datetime.now(timezone.utc).isoformat(),
        }
        series.append(entry)
        print(
            f"  RESULTADO pref={pref} alloc={alloc} node={node} "
            f"checks={entry['checks']}",
            flush=True,
        )
        time.sleep(args.gap)

    print("\n" + "=" * 64)
    print("RESUMO 2a")
    for s in series:
        c = s["checks"]
        print(
            f"  {s['case']}: pref={s['preferred_compute_layer']} "
            f"alloc={s['allocated_compute_layer']} node={s['allocated_node_id']} "
            f"| pref_ok={c['preferred_matches_expected']} "
            f"layer_eq={c['preferred_equals_allocated']} "
            f"host_ok={c['hostname_matches_layer_node']}"
        )

    out = ROOT / "results" / "metrics_export" / f"layer_affinity_eval_{int(time.time())}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(series, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nJSON: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
