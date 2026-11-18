#!/usr/bin/env python3
"""
EXP-01 — braço Kubernetes Padrão (10 iterações seriais).

Aplica o Job+ConfigMap baseline, aguarda Succeeded/Failed, registra duração
e apaga os recursos antes da próxima corrida.

Uso:
  python scripts/run_exp1_baseline.py
  python scripts/run_exp1_baseline.py --runs 10 --namespace tcc-baseline
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_YAML = ROOT / "docs" / "experimentos" / "exp1_baseline_job.yaml"
JOB_NAME = "exp1-baseline"
CM_NAME = "exp1-baseline-script"
LABEL_SEL = "app=tcc-baseline,experiment=exp1"


def _run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, check=check, text=True, capture_output=True)


def kubectl(args: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return _run(["kubectl", *args], check=check)


def apply_manifest(yaml_path: Path, namespace: str) -> None:
    r = kubectl(["apply", "-f", str(yaml_path), "-n", namespace])
    print(r.stdout.strip() or r.stderr.strip(), flush=True)


def delete_resources(namespace: str) -> None:
    kubectl(
        ["delete", "job", JOB_NAME, "-n", namespace, "--ignore-not-found", "--wait=true"],
        check=False,
    )
    kubectl(
        ["delete", "configmap", CM_NAME, "-n", namespace, "--ignore-not-found"],
        check=False,
    )
    # Limpa pods órfãos do Job, se houver.
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


def job_terminal_status(namespace: str) -> str | None:
    """Retorna 'Succeeded', 'Failed' ou None se ainda em andamento."""
    r = kubectl(
        [
            "get",
            "job",
            JOB_NAME,
            "-n",
            namespace,
            "-o",
            "json",
        ],
        check=False,
    )
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
    # Fallback por contadores.
    if int(status.get("succeeded") or 0) >= 1:
        return "Succeeded"
    if int(status.get("failed") or 0) >= 1:
        return "Failed"
    return None


def wait_job(namespace: str, timeout_s: float, poll_s: float = 2.0) -> str:
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        last = job_terminal_status(namespace)
        if last:
            return last
        print(
            f"  [{datetime.now().strftime('%H:%M:%S')}] Job {JOB_NAME} ainda rodando…",
            flush=True,
        )
        time.sleep(poll_s)
    raise TimeoutError(
        f"Timeout ({timeout_s}s) aguardando Job {JOB_NAME} (último={last})"
    )


def main() -> int:
    p = argparse.ArgumentParser(description="EXP-01 baseline: 10 Jobs seriais no kube-scheduler")
    p.add_argument("--yaml", type=Path, default=DEFAULT_YAML)
    p.add_argument("--namespace", default="tcc-baseline")
    p.add_argument("--runs", type=int, default=10)
    p.add_argument("--timeout", type=float, default=300.0)
    p.add_argument("--gap", type=float, default=5.0, help="Pausa entre iterações (s)")
    args = p.parse_args()
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from _runlog import start_run_log
    start_run_log("exp1", "baseline")

    if not args.yaml.is_file():
        print(f"YAML não encontrado: {args.yaml}", file=sys.stderr)
        return 1

    print("=" * 64)
    print("EXP-01 — Baseline Kubernetes Padrão")
    print(f"yaml={args.yaml}  ns={args.namespace}  runs={args.runs}")
    print("=" * 64)

    series: list[dict] = []
    for i in range(1, args.runs + 1):
        print(f"\n--- Run {i}/{args.runs} ---", flush=True)
        delete_resources(args.namespace)
        time.sleep(1.0)

        t0 = time.monotonic()
        apply_manifest(args.yaml, args.namespace)
        try:
            status = wait_job(args.namespace, args.timeout)
        except TimeoutError as e:
            print(f"  ERRO: {e}", file=sys.stderr)
            status = "Timeout"
        elapsed = time.monotonic() - t0

        entry = {
            "run": i,
            "status": status,
            "duration_s": round(elapsed, 3),
            "job": JOB_NAME,
            "at": datetime.now(timezone.utc).isoformat(),
        }
        series.append(entry)
        print(f"  RESULTADO: status={status} duration_s={elapsed:.2f}", flush=True)

        delete_resources(args.namespace)
        if i < args.runs:
            time.sleep(args.gap)

    print("\n" + "=" * 64)
    print("RESUMO BASELINE")
    for s in series:
        print(f"  run{s['run']}: status={s['status']} duration_s={s['duration_s']}")

    out = (
        ROOT
        / "results"
        / "metrics_export"
        / "exp1"
        / "baseline"
        / f"exp1_baseline_{int(time.time())}.json"
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(series, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nJSON: {out}")
    return 0 if all(s["status"] != "Timeout" for s in series) else 1


if __name__ == "__main__":
    raise SystemExit(main())
