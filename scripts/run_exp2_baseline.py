#!/usr/bin/env python3
"""
EXP-02 — braço Kubernetes Padrão (lote de 5 Jobs + noisy neighbor).

1) Sobe Pod de estresse em tcc-cluster-worker2
2) Aguarda aquecimento
3) Aplica 5 Jobs concorrentes (sem nodeSelector)
4) Mede makespan e coleta nodeName de cada Job
5) Limpa recursos e grava JSON em results/metrics_export/exp2/baseline/

Uso:
  python scripts/run_exp2_baseline.py
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STRESS_YAML = ROOT / "docs" / "experimentos" / "exp2_stress_pod.yaml"
JOBS_YAML = ROOT / "docs" / "experimentos" / "exp2_baseline_jobs.yaml"
STRESS_POD = "exp2-noisy-neighbor"
HOT_NODE = "tcc-cluster-worker2"
JOB_NAMES = [f"exp2-baseline-{i}" for i in range(1, 6)]
LABEL_SEL = "app=tcc-baseline,experiment=exp2"


def _run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, check=check, text=True, capture_output=True)


def kubectl(args: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return _run(["kubectl", *args], check=check)


def apply_manifest(yaml_path: Path, namespace: str) -> None:
    r = kubectl(["apply", "-f", str(yaml_path), "-n", namespace])
    out = (r.stdout or "").strip() or (r.stderr or "").strip()
    if out:
        print(out, flush=True)


def wait_pod_running(name: str, namespace: str, timeout_s: float) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        r = kubectl(
            [
                "get",
                "pod",
                name,
                "-n",
                namespace,
                "-o",
                "jsonpath={.status.phase}",
            ],
            check=False,
        )
        phase = (r.stdout or "").strip()
        print(f"  [{datetime.now().strftime('%H:%M:%S')}] stress pod phase={phase or '?'}", flush=True)
        if phase == "Running":
            return
        if phase in {"Failed", "Succeeded"}:
            raise RuntimeError(f"Pod de estresse terminou prematuramente: {phase}")
        time.sleep(2)
    raise TimeoutError(f"Timeout aguardando Pod {name} Running")


def job_terminal(name: str, namespace: str) -> str | None:
    r = kubectl(["get", "job", name, "-n", namespace, "-o", "json"], check=False)
    if r.returncode != 0:
        return None
    try:
        data = json.loads(r.stdout)
    except json.JSONDecodeError:
        return None
    status = data.get("status") or {}
    for cond in status.get("conditions") or []:
        if cond.get("type") == "Complete" and cond.get("status") == "True":
            return "Succeeded"
        if cond.get("type") == "Failed" and cond.get("status") == "True":
            return "Failed"
    if int(status.get("succeeded") or 0) >= 1:
        return "Succeeded"
    if int(status.get("failed") or 0) >= 1:
        return "Failed"
    return None


def wait_all_jobs(namespace: str, timeout_s: float, poll_s: float = 2.0) -> dict[str, str]:
    deadline = time.time() + timeout_s
    results: dict[str, str] = {}
    while time.time() < deadline:
        pending = []
        for name in JOB_NAMES:
            if name in results:
                continue
            st = job_terminal(name, namespace)
            if st:
                results[name] = st
                print(f"  [{datetime.now().strftime('%H:%M:%S')}] {name} -> {st}", flush=True)
            else:
                pending.append(name)
        if not pending:
            return results
        print(
            f"  [{datetime.now().strftime('%H:%M:%S')}] aguardando {len(pending)} Job(s): "
            f"{', '.join(pending)}",
            flush=True,
        )
        time.sleep(poll_s)
    missing = [n for n in JOB_NAMES if n not in results]
    raise TimeoutError(f"Timeout; Jobs pendentes: {missing}; concluídos={results}")


def collect_pod_placements(namespace: str) -> list[dict]:
    r = kubectl(
        [
            "get",
            "pods",
            "-n",
            namespace,
            "-l",
            "app=tcc-baseline,experiment=exp2,arm=k8s-default",
            "-o",
            "json",
        ],
        check=False,
    )
    if r.returncode != 0:
        return []
    try:
        items = (json.loads(r.stdout).get("items")) or []
    except json.JSONDecodeError:
        return []

    out: list[dict] = []
    for pod in items:
        labels = (pod.get("metadata") or {}).get("labels") or {}
        if labels.get("role") == "stress":
            continue
        name = (pod.get("metadata") or {}).get("name")
        node = ((pod.get("spec") or {}).get("nodeName")) or None
        phase = ((pod.get("status") or {}).get("phase")) or None
        run = labels.get("run")
        out.append(
            {
                "pod": name,
                "run": run,
                "nodeName": node,
                "phase": phase,
                "on_hot_node": node == HOT_NODE,
            }
        )
    out.sort(key=lambda x: int(x["run"]) if str(x.get("run") or "").isdigit() else 99)
    return out


def top_nodes_snapshot() -> str:
    r = kubectl(["top", "nodes"], check=False)
    return (r.stdout or r.stderr or "").strip()


def cleanup(namespace: str) -> None:
    for name in JOB_NAMES:
        kubectl(
            ["delete", "job", name, "-n", namespace, "--ignore-not-found", "--wait=false"],
            check=False,
        )
    kubectl(
        ["delete", "configmap", "exp2-baseline-script", "-n", namespace, "--ignore-not-found"],
        check=False,
    )
    kubectl(
        ["delete", "pod", STRESS_POD, "-n", namespace, "--ignore-not-found", "--wait=true", "--timeout=120s"],
        check=False,
    )
    kubectl(
        [
            "delete",
            "pods",
            "-n",
            namespace,
            "-l",
            LABEL_SEL,
            "--ignore-not-found",
            "--wait=false",
        ],
        check=False,
    )


def main() -> int:
    p = argparse.ArgumentParser(description="EXP-02 baseline: noisy neighbor + 5 Jobs")
    p.add_argument("--namespace", default="tcc-baseline")
    p.add_argument("--stress-yaml", type=Path, default=STRESS_YAML)
    p.add_argument("--jobs-yaml", type=Path, default=JOBS_YAML)
    p.add_argument("--warmup", type=float, default=10.0, help="Segundos após stress Running")
    p.add_argument("--timeout", type=float, default=600.0)
    p.add_argument("--skip-cleanup", action="store_true")
    args = p.parse_args()
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from _runlog import start_run_log
    start_run_log("exp2", "baseline")

    for path in (args.stress_yaml, args.jobs_yaml):
        if not path.is_file():
            print(f"Arquivo não encontrado: {path}", file=sys.stderr)
            return 1

    print("=" * 64)
    print("EXP-02 — Baseline Kubernetes Padrão (noisy neighbor)")
    print(f"ns={args.namespace}  hot_node={HOT_NODE}")
    print("=" * 64)

    # Limpa resíduos de corridas anteriores do EXP-02
    cleanup(args.namespace)
    time.sleep(2.0)

    print("\n[1/5] Aplicando Pod de estresse...", flush=True)
    apply_manifest(args.stress_yaml, args.namespace)
    wait_pod_running(STRESS_POD, args.namespace, timeout_s=180.0)

    print(f"\n[2/5] Warm-up {args.warmup:.0f}s para o estresse bater no nó...", flush=True)
    time.sleep(args.warmup)
    tops = top_nodes_snapshot()
    if tops:
        print("kubectl top nodes:", flush=True)
        print(tops, flush=True)

    print("\n[3/5] Aplicando 5 Jobs concorrentes (rajada)...", flush=True)
    t_batch_start = time.monotonic()
    batch_started_at = datetime.now(timezone.utc).isoformat()
    apply_manifest(args.jobs_yaml, args.namespace)

    print("\n[4/5] Monitorando Jobs...", flush=True)
    try:
        job_status = wait_all_jobs(args.namespace, args.timeout)
    except TimeoutError as e:
        print(f"ERRO: {e}", file=sys.stderr)
        placements = collect_pod_placements(args.namespace)
        print(json.dumps(placements, indent=2), flush=True)
        if not args.skip_cleanup:
            cleanup(args.namespace)
        return 1

    t_batch_end = time.monotonic()
    makespan_s = t_batch_end - t_batch_start
    batch_ended_at = datetime.now(timezone.utc).isoformat()

    # Coleta nodeName enquanto pods ainda existem
    placements = collect_pod_placements(args.namespace)
    node_counts = Counter(p.get("nodeName") or "unknown" for p in placements)
    n_hot = sum(1 for p in placements if p.get("on_hot_node"))

    print("\n[5/5] Cleanup...", flush=True)
    if not args.skip_cleanup:
        cleanup(args.namespace)
    else:
        print("  --skip-cleanup: recursos mantidos", flush=True)

    report = {
        "experiment": "exp2",
        "arm": "k8s-default",
        "hot_node": HOT_NODE,
        "stress_pod": STRESS_POD,
        "batch_started_at": batch_started_at,
        "batch_ended_at": batch_ended_at,
        "makespan_s": round(makespan_s, 3),
        "job_status": job_status,
        "placements": placements,
        "node_distribution": dict(node_counts),
        "n_on_hot_node": n_hot,
        "kubectl_top_nodes_after_warmup": tops,
        "at": datetime.now(timezone.utc).isoformat(),
    }

    print("\n" + "=" * 64)
    print("RESUMO EXP-02 BASELINE")
    print(f"  Makespan: {makespan_s:.2f}s")
    print(f"  Jobs no nó sobrecarregado ({HOT_NODE}): {n_hot}/5")
    print("  Distribuição por nó:")
    for node, cnt in sorted(node_counts.items(), key=lambda x: (-x[1], x[0])):
        mark = " <-- HOT" if node == HOT_NODE else ""
        print(f"    {node}: {cnt}{mark}")
    for p in placements:
        print(
            f"  run{p.get('run')}: pod={p.get('pod')} node={p.get('nodeName')} "
            f"phase={p.get('phase')}"
        )

    out = (
        ROOT
        / "results"
        / "metrics_export"
        / "exp2"
        / "baseline"
        / f"exp2_baseline_{int(time.time())}.json"
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nJSON: {out}")
    return 0 if all(s == "Succeeded" for s in job_status.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
