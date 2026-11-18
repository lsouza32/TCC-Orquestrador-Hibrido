#!/usr/bin/env python3
"""
EXP-02 — braço Orquestrador Híbrido (noisy neighbor + 5 submits em rajada).

1) Cold start do SQLite (evita warm-start do EXP-01 puxando para worker2)
2) Sobe Pod de estresse em tcc-cluster-worker2
3) Warm-up longo (default 55s) para o Metrics Server refletir CPU alta
4) Cinco POST /submit_task quase simultâneos (mesmo task_exp2_cpu.py)
5) Aguarda status terminal das 5, mede makespan, limpa o estresse
6) Exporta JSON em results/metrics_export/exp2/hibrido/

Uso:
  python scripts/run_exp2_hybrid.py
  python scripts/run_exp2_hybrid.py --warmup 60 --api http://127.0.0.1:5000
"""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    print("Instale requests: pip install requests", file=sys.stderr)
    raise SystemExit(1)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCRIPT = ROOT / "scripts" / "eval_tasks" / "task_exp2_cpu.py"
DEFAULT_DB = ROOT / "data" / "nodes.db"
STRESS_YAML = ROOT / "docs" / "experimentos" / "exp2_stress_pod.yaml"
STRESS_POD = "exp2-noisy-neighbor"
HOT_NODE = "tcc-cluster-worker2"
TERMINAL = frozenset({"concluida", "falha", "cancelada", "erro_agente", "falha_alocacao"})


def _connect(db: Path) -> sqlite3.Connection:
    c = sqlite3.connect(str(db))
    c.row_factory = sqlite3.Row
    return c


def kubectl(args: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["kubectl", *args], check=check, text=True, capture_output=True)


def cold_start_reset(db: Path, keep_uploads: bool = False) -> None:
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


def ensure_namespace(namespace: str) -> None:
    r = kubectl(["get", "ns", namespace], check=False)
    if r.returncode != 0:
        kubectl(["create", "namespace", namespace])
        print(f"Namespace criado: {namespace}", flush=True)


def apply_stress(yaml_path: Path, namespace: str) -> None:
    r = kubectl(["apply", "-f", str(yaml_path), "-n", namespace])
    out = (r.stdout or "").strip() or (r.stderr or "").strip()
    if out:
        print(out, flush=True)


def wait_stress_running(namespace: str, timeout_s: float) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        r = kubectl(
            ["get", "pod", STRESS_POD, "-n", namespace, "-o", "jsonpath={.status.phase}"],
            check=False,
        )
        phase = (r.stdout or "").strip()
        print(
            f"  [{datetime.now().strftime('%H:%M:%S')}] stress pod phase={phase or '?'}",
            flush=True,
        )
        if phase == "Running":
            return
        if phase in {"Failed", "Succeeded"}:
            raise RuntimeError(f"Pod de estresse terminou prematuramente: {phase}")
        time.sleep(2)
    raise TimeoutError(f"Timeout aguardando Pod {STRESS_POD} Running")


def top_nodes_snapshot() -> str:
    r = kubectl(["top", "nodes"], check=False)
    return (r.stdout or r.stderr or "").strip()


def delete_stress(namespace: str) -> None:
    kubectl(
        ["delete", "pod", STRESS_POD, "-n", namespace, "--ignore-not-found", "--wait=true", "--timeout=120s"],
        check=False,
    )
    print(f"Stress pod {STRESS_POD} removido (ns={namespace})", flush=True)


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


def fetch_task(db: Path, task_id: str) -> dict | None:
    conn = _connect(db)
    try:
        row = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        res = conn.execute(
            "SELECT status_final, duration_seconds, cpu_peak_percent, ram_peak_mb, "
            "prediction_delta_ram FROM task_results WHERE task_id=?",
            (task_id,),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return None
    out = dict(row)
    if res:
        out["result"] = dict(res)
    return out


def wait_all(db: Path, task_ids: list[str], timeout_s: float, poll_s: float = 5.0) -> list[dict]:
    deadline = time.time() + timeout_s
    done: dict[str, dict] = {}
    while time.time() < deadline:
        pending = []
        for tid in task_ids:
            if tid in done:
                continue
            row = fetch_task(db, tid)
            if not row:
                pending.append(tid[:8])
                continue
            status = row.get("status")
            short = tid[:8]
            print(
                f"  [{datetime.now().strftime('%H:%M:%S')}] {short}… "
                f"status={status} node={row.get('allocated_node_id') or '—'}",
                flush=True,
            )
            if status in TERMINAL:
                done[tid] = row
            else:
                pending.append(short)
        if len(done) == len(task_ids):
            return [done[tid] for tid in task_ids]
        time.sleep(poll_s)
    missing = [tid for tid in task_ids if tid not in done]
    raise TimeoutError(f"Timeout; pendentes={missing}; done={list(done)}")


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


def hostname_from_allocated(node_id: str | None) -> str | None:
    if not node_id:
        return None
    s = str(node_id)
    if s.startswith("k8s:"):
        return s[4:]
    return s


def main() -> int:
    p = argparse.ArgumentParser(description="EXP-02 híbrido: stress + 5 submits em rajada")
    p.add_argument("--api", default="http://127.0.0.1:5000")
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--script", type=Path, default=DEFAULT_SCRIPT)
    p.add_argument("--stress-yaml", type=Path, default=STRESS_YAML)
    p.add_argument("--namespace", default="tcc-baseline", help="NS do Pod de estresse")
    p.add_argument("--user", default="lsouza")
    p.add_argument("--runs", type=int, default=5)
    p.add_argument(
        "--warmup",
        type=float,
        default=55.0,
        help="Segundos após stress Running (Metrics Server atrasa no Kind)",
    )
    p.add_argument("--timeout", type=float, default=1200.0)
    p.add_argument("--keep-uploads", action="store_true")
    p.add_argument("--skip-reset", action="store_true")
    p.add_argument("--skip-cleanup", action="store_true")
    args = p.parse_args()
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from _runlog import start_run_log
    start_run_log("exp2", "hybrid")

    if not args.script.is_file():
        print(f"Script não encontrado: {args.script}", file=sys.stderr)
        return 1
    if not args.db.is_file():
        print(f"DB não encontrado: {args.db}", file=sys.stderr)
        return 1
    if not args.stress_yaml.is_file():
        print(f"YAML de estresse não encontrado: {args.stress_yaml}", file=sys.stderr)
        return 1

    print("=" * 64)
    print("EXP-02 — Orquestrador Híbrido (noisy neighbor)")
    print(f"API={args.api}  runs={args.runs}  hot_node={HOT_NODE}  warmup={args.warmup:.0f}s")
    print("=" * 64)

    # Limpa resíduos de Jobs/tasks do broker e stress antigo
    kubectl(
        ["delete", "jobs,pods,cm", "-l", "app=tcc-task-runner", "--all-namespaces", "--ignore-not-found"],
        check=False,
    )
    delete_stress(args.namespace)
    time.sleep(2.0)

    if not args.skip_reset:
        cold_start_reset(args.db, keep_uploads=args.keep_uploads)
    else:
        print("AVISO: --skip-reset — histórico SQLite preservado", flush=True)

    ensure_namespace(args.namespace)

    print("\n[1/5] Aplicando Pod de estresse...", flush=True)
    apply_stress(args.stress_yaml, args.namespace)
    wait_stress_running(args.namespace, timeout_s=180.0)

    print(
        f"\n[2/5] Warm-up {args.warmup:.0f}s (Metrics Server / watcher refletirem CPU alta)...",
        flush=True,
    )
    time.sleep(args.warmup)
    tops = top_nodes_snapshot()
    if tops:
        print("kubectl top nodes:", flush=True)
        print(tops, flush=True)
        # Validade: o nó sob estresse precisa estar de fato quente (>= 500m).
        for ln in tops.splitlines():
            if HOT_NODE in ln:
                cols = ln.split()
                cpu_m = int(cols[1].rstrip("m")) if len(cols) > 1 and cols[1].endswith("m") else 0
                if cpu_m < 500:
                    print(
                        f"ERRO: {HOT_NODE} com {cpu_m}m de CPU; estresse não efetivo, "
                        "corrida inválida.",
                        file=sys.stderr,
                    )
                    delete_stress(args.namespace)
                    return 3

    print(f"\n[3/5] Submetendo {args.runs} tarefas em rajada via /submit_task...", flush=True)
    t_batch_start = time.monotonic()
    batch_started_at = datetime.now(timezone.utc).isoformat()
    task_ids: list[str] = []
    submit_errors: list[str] = []
    for i in range(1, args.runs + 1):
        try:
            tid = submit(args.api, args.script, args.user)
            task_ids.append(tid)
            print(f"  run{i} submetida: {tid}", flush=True)
        except Exception as e:
            msg = f"run{i}: {e}"
            submit_errors.append(msg)
            print(f"  ERRO submit {msg}", file=sys.stderr)

    if len(task_ids) < args.runs:
        print(f"Falha na rajada: só {len(task_ids)}/{args.runs} submits", file=sys.stderr)
        if not args.skip_cleanup:
            delete_stress(args.namespace)
        return 1

    print("\n[4/5] Monitorando tarefas até status terminal...", flush=True)
    try:
        rows = wait_all(args.db, task_ids, args.timeout)
    except TimeoutError as e:
        print(f"ERRO: {e}", file=sys.stderr)
        if not args.skip_cleanup:
            delete_stress(args.namespace)
        return 1

    t_batch_end = time.monotonic()
    makespan_s = t_batch_end - t_batch_start
    batch_ended_at = datetime.now(timezone.utc).isoformat()

    # Makespan de cluster: 1ª execução vista → última terminal (aproximação via DB)
    exec_starts: list[float] = []
    exec_ends: list[float] = []
    series: list[dict] = []
    for i, (tid, row) in enumerate(zip(task_ids, rows), start=1):
        lib = extract_librarian(row.get("ai_raw_response"))
        result = row.get("result") or {}
        node = row.get("allocated_node_id")
        host = hostname_from_allocated(node)
        entry = {
            "run": i,
            "task_id": tid,
            "status": row.get("status"),
            "docker_image": row.get("docker_image"),
            "preferred_compute_layer": row.get("preferred_compute_layer"),
            "allocated_compute_layer": row.get("allocated_compute_layer"),
            "allocated_node_id": node,
            "hostname": host,
            "on_hot_node": host == HOT_NODE,
            "librarian_best_image": lib.get("best_image"),
            "ceiling_signals": lib.get("ceiling_signals"),
            "safety_margin": lib.get("safety_margin"),
            "result": result,
            "ram_peak_mb": result.get("ram_peak_mb"),
            "cpu_peak_percent": result.get("cpu_peak_percent"),
            "duration_seconds": result.get("duration_seconds"),
            "submitted_at": row.get("submitted_at"),
            "started_at": row.get("started_at") or row.get("execution_started_at"),
            "finished_at": row.get("finished_at") or row.get("completed_at"),
        }
        series.append(entry)

    node_counts = Counter(s.get("hostname") or "unknown" for s in series)
    n_hot = sum(1 for s in series if s.get("on_hot_node"))

    print("\n[5/5] Cleanup do estresse...", flush=True)
    if not args.skip_cleanup:
        delete_stress(args.namespace)
    else:
        print("  --skip-cleanup: stress mantido", flush=True)

    report = {
        "experiment": "exp2",
        "arm": "hybrid",
        "hot_node": HOT_NODE,
        "stress_pod": STRESS_POD,
        "batch_started_at": batch_started_at,
        "batch_ended_at": batch_ended_at,
        "makespan_s": round(makespan_s, 3),
        "makespan_note": (
            "ponta-a-ponta: 1º POST /submit_task até último status terminal "
            "(inclui planejamento IA / Ollama)"
        ),
        "n_tasks": len(series),
        "n_on_hot_node": n_hot,
        "node_distribution": dict(node_counts),
        "tasks": series,
        "kubectl_top_nodes_after_warmup": tops,
        "submit_errors": submit_errors,
        "at": datetime.now(timezone.utc).isoformat(),
    }

    print("\n" + "=" * 64)
    print("RESUMO EXP-02 HÍBRIDO")
    print(f"  Makespan (ponta a ponta): {makespan_s:.2f}s")
    print(f"  Tarefas no nó sobrecarregado ({HOT_NODE}): {n_hot}/{len(series)}")
    print("  Distribuição por nó:")
    for node, cnt in sorted(node_counts.items(), key=lambda x: (-x[1], x[0])):
        mark = " <-- HOT" if node == HOT_NODE else ""
        print(f"    {node}: {cnt}{mark}")
    for s in series:
        print(
            f"  run{s['run']}: status={s['status']} node={s.get('allocated_node_id')} "
            f"dur={s.get('duration_seconds')} hot={s.get('on_hot_node')}"
        )

    out = (
        ROOT
        / "results"
        / "metrics_export"
        / "exp2"
        / "hibrido"
        / f"exp2_hybrid_{int(time.time())}.json"
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nJSON: {out}")

    ok = all(s.get("status") == "concluida" for s in series)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
