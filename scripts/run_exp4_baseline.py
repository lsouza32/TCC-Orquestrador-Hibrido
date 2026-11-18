#!/usr/bin/env python3
"""
EXP-04 — braço Kubernetes Padrão (topologia cega, N=9 serial).

Ordem intercalada: Leve, Fog, ML × 3.
Imagem única python:3.11-slim, com pré-pull nos 3 workers.
Sem nodeSelector — coleta nodeName + compute-layer e calcula hit rate
vs preferência teórica (edge/fog/cloud).

Uso:
  python scripts/run_exp4_baseline.py
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CM_YAML = ROOT / "docs" / "experimentos" / "exp4_baseline_jobs.yaml"
LAYER_LABEL = "tcc.utfpr.edu.br/compute-layer"
JOB_NAME = "exp4-baseline-run"
CM_NAME = "exp4-baseline-scripts"
LABEL_SEL = "app=tcc-baseline,experiment=exp4"
PIN_IMAGE = "python:3.11-slim"
KIND_WORKERS = (
    "tcc-cluster-worker",
    "tcc-cluster-worker2",
    "tcc-cluster-worker3",
)

# (profile_id, script_filename, preferred_layer)
PROFILES = {
    "edge": ("task_edge_light.py", "edge"),
    "fog": ("task_fog_cpu.py", "fog"),
    "cloud": ("task_cloud_ml.py", "cloud"),
}

# Lote N=9 intercalado: Leve, Fog, ML × 3
SEQUENCE = ["edge", "fog", "cloud"] * 3


def _run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, check=check, text=True, capture_output=True)


def kubectl(args: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return _run(["kubectl", *args], check=check)


def prepull_runtime_image(image: str = PIN_IMAGE) -> dict:
    """Equaliza o cache inicial de python:3.11-slim nos três workers Kind."""
    results: dict[str, dict] = {}
    for host in KIND_WORKERS:
        r = subprocess.run(
            ["docker", "exec", host, "crictl", "pull", image],
            capture_output=True,
            text=True,
            check=False,
            timeout=300,
        )
        ok = r.returncode == 0
        err = (r.stderr or r.stdout or "").strip()[-300:]
        results[host] = {"ok": ok, "detail": err}
        print(f"  pre-pull {image} @ {host}: {'ok' if ok else 'FAIL'} {err}", flush=True)
        if not ok:
            raise RuntimeError(f"pré-pull falhou em {host}: {err}")
    return results


def ensure_configmap(namespace: str) -> None:
    """Aplica o manifesto e remove o Job template (só queremos o ConfigMap)."""
    kubectl(["apply", "-f", str(CM_YAML), "-n", namespace])
    kubectl(
        ["delete", "job", "exp4-baseline-template", "-n", namespace, "--ignore-not-found", "--wait=false"],
        check=False,
    )


def render_job_yaml(namespace: str, run_idx: int, profile: str, script: str) -> str:
    return f"""apiVersion: batch/v1
kind: Job
metadata:
  name: {JOB_NAME}
  namespace: {namespace}
  labels:
    app: tcc-baseline
    experiment: exp4
    arm: k8s-default
    profile: {profile}
    run: "{run_idx}"
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 86400
  template:
    metadata:
      labels:
        app: tcc-baseline
        experiment: exp4
        arm: k8s-default
        profile: {profile}
        run: "{run_idx}"
    spec:
      restartPolicy: Never
      containers:
        - name: task-runner
          image: {PIN_IMAGE}
          imagePullPolicy: IfNotPresent
          command: ["/bin/sh", "-c"]
          args:
            - cd /app/scripts && python3 {script}
          volumeMounts:
            - name: task-scripts
              mountPath: /app/scripts
          resources:
            requests:
              cpu: 100m
              memory: 128Mi
            limits:
              cpu: 500m
              memory: 256Mi
      volumes:
        - name: task-scripts
          configMap:
            name: {CM_NAME}
"""


def delete_job(namespace: str) -> None:
    kubectl(
        ["delete", "job", JOB_NAME, "-n", namespace, "--ignore-not-found", "--wait=true"],
        check=False,
    )
    kubectl(
        [
            "delete",
            "pods",
            "-n",
            namespace,
            "-l",
            f"job-name={JOB_NAME}",
            "--ignore-not-found",
            "--wait=false",
        ],
        check=False,
    )


def job_terminal(namespace: str) -> str | None:
    r = kubectl(["get", "job", JOB_NAME, "-n", namespace, "-o", "json"], check=False)
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


def wait_job(namespace: str, timeout_s: float, poll_s: float = 2.0) -> str:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        st = job_terminal(namespace)
        if st:
            return st
        print(
            f"  [{datetime.now().strftime('%H:%M:%S')}] Job {JOB_NAME} rodando…",
            flush=True,
        )
        time.sleep(poll_s)
    raise TimeoutError(f"Timeout aguardando {JOB_NAME}")


def get_pod_node(namespace: str) -> tuple[str | None, str | None]:
    r = kubectl(
        [
            "get",
            "pods",
            "-n",
            namespace,
            "-l",
            f"job-name={JOB_NAME}",
            "-o",
            "json",
        ],
        check=False,
    )
    if r.returncode != 0:
        return None, None
    try:
        items = (json.loads(r.stdout).get("items")) or []
    except json.JSONDecodeError:
        return None, None
    if not items:
        return None, None
    items.sort(key=lambda p: (p.get("metadata") or {}).get("creationTimestamp") or "")
    pod = items[-1]
    pod_name = (pod.get("metadata") or {}).get("name")
    node = (pod.get("spec") or {}).get("nodeName")
    return pod_name, node


def get_node_layer(node_name: str | None) -> str | None:
    if not node_name:
        return None
    # Escapa pontos do domínio da label para jsonpath do kubectl.
    r = kubectl(
        [
            "get",
            "node",
            node_name,
            "-o",
            r"jsonpath={.metadata.labels.tcc\.utfpr\.edu\.br/compute-layer}",
        ],
        check=False,
    )
    if r.returncode != 0:
        return None
    layer = (r.stdout or "").strip()
    return layer or None


def confusion_matrix(rows: list[dict]) -> dict[str, dict[str, int]]:
    layers = ["edge", "fog", "cloud"]
    matrix = {pref: {alloc: 0 for alloc in layers} for pref in layers}
    for row in rows:
        pref = row.get("preferred_layer")
        alloc = row.get("allocated_layer")
        if pref in matrix and alloc in matrix[pref]:
            matrix[pref][alloc] += 1
    return matrix


def main() -> int:
    p = argparse.ArgumentParser(description="EXP-04 baseline: topologia cega N=9 serial")
    p.add_argument("--namespace", default="tcc-baseline")
    p.add_argument("--timeout", type=float, default=300.0)
    p.add_argument("--gap", type=float, default=3.0)
    args = p.parse_args()
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from _runlog import start_run_log
    start_run_log("exp4", "baseline")

    if not CM_YAML.is_file():
        print(f"YAML não encontrado: {CM_YAML}", file=sys.stderr)
        return 1

    print("=" * 64)
    print("EXP-04 — Baseline Kubernetes Padrão (topologia)")
    print(f"ns={args.namespace}  N={len(SEQUENCE)}  ordem={SEQUENCE}  image={PIN_IMAGE}")
    print("=" * 64)

    print(f"\nPré-pull {PIN_IMAGE} nos 3 workers (cache inicial)...", flush=True)
    try:
        prepull = prepull_runtime_image(PIN_IMAGE)
    except RuntimeError as e:
        print(f"ERRO: {e}", file=sys.stderr)
        return 1

    print("\nPreparando ConfigMap de scripts...", flush=True)
    ensure_configmap(args.namespace)
    delete_job(args.namespace)

    series: list[dict] = []
    for i, profile in enumerate(SEQUENCE, start=1):
        script, preferred = PROFILES[profile]
        print(f"\n--- Run {i}/{len(SEQUENCE)} profile={profile} preferred={preferred} ---", flush=True)
        delete_job(args.namespace)
        time.sleep(1.0)

        job_yaml = render_job_yaml(args.namespace, i, profile, script)
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False, encoding="utf-8"
        ) as tmp:
            tmp.write(job_yaml)
            tmp_path = tmp.name

        t0 = time.monotonic()
        r = kubectl(["apply", "-f", tmp_path, "-n", args.namespace])
        print((r.stdout or r.stderr or "").strip(), flush=True)
        Path(tmp_path).unlink(missing_ok=True)

        try:
            status = wait_job(args.namespace, args.timeout)
        except TimeoutError as e:
            print(f"  ERRO: {e}", file=sys.stderr)
            status = "Timeout"

        pod_name, node_name = get_pod_node(args.namespace)
        allocated = get_node_layer(node_name)
        hit = allocated == preferred
        elapsed = time.monotonic() - t0

        entry = {
            "run": i,
            "profile": profile,
            "script": script,
            "preferred_layer": preferred,
            "status": status,
            "pod": pod_name,
            "nodeName": node_name,
            "allocated_layer": allocated,
            "hit": hit,
            "docker_image": PIN_IMAGE,
            "duration_s": round(elapsed, 3),
            "at": datetime.now(timezone.utc).isoformat(),
        }
        series.append(entry)
        print(
            f"  RESULTADO: status={status} node={node_name} layer={allocated} "
            f"preferred={preferred} hit={hit} duration_s={elapsed:.1f}",
            flush=True,
        )

        delete_job(args.namespace)
        if i < len(SEQUENCE):
            time.sleep(args.gap)

    # Cleanup ConfigMap opcional — mantém para inspeção; remove Job template se sobrou
    kubectl(
        ["delete", "job", "exp4-baseline-template", "-n", args.namespace, "--ignore-not-found"],
        check=False,
    )

    hits = sum(1 for s in series if s.get("hit"))
    n = len(series)
    hit_rate = hits / n if n else 0.0
    by_profile = Counter(s["profile"] for s in series)
    layer_dist = Counter(s.get("allocated_layer") or "unknown" for s in series)
    cm = confusion_matrix(series)

    # Hit por classe
    hit_by_pref: dict[str, dict] = {}
    for pref in ("edge", "fog", "cloud"):
        rows = [s for s in series if s["preferred_layer"] == pref]
        h = sum(1 for s in rows if s.get("hit"))
        hit_by_pref[pref] = {"n": len(rows), "hits": h, "hit_rate": (h / len(rows) if rows else 0.0)}

    report = {
        "experiment": "exp4",
        "arm": "k8s-default",
        "n": n,
        "sequence": SEQUENCE,
        "runtime_image": PIN_IMAGE,
        "prepull": prepull,
        "note_image_control": (
            "Imagem única python:3.11-slim nas 9 jobs; pré-pull nos 3 workers "
            "antes do lote para equalizar ImageLocality/cache."
        ),
        "hits": hits,
        "hit_rate": round(hit_rate, 4),
        "hit_by_preferred": hit_by_pref,
        "confusion_matrix_preferred_vs_allocated": cm,
        "allocated_layer_distribution": dict(layer_dist),
        "profile_counts": dict(by_profile),
        "runs": series,
        "at": datetime.now(timezone.utc).isoformat(),
    }

    print("\n" + "=" * 64)
    print("RESUMO EXP-04 BASELINE")
    print(f"  Hit rate: {hits}/{n} = {hit_rate:.1%}")
    print("  Matriz preferred \\ allocated:")
    print(f"  {'':10} {'edge':>6} {'fog':>6} {'cloud':>6}")
    for pref in ("edge", "fog", "cloud"):
        row = cm[pref]
        print(
            f"  {pref:10} {row['edge']:6d} {row['fog']:6d} {row['cloud']:6d}"
        )
    print("  Por classe:")
    for pref, info in hit_by_pref.items():
        print(f"    {pref}: {info['hits']}/{info['n']} ({info['hit_rate']:.0%})")
    print("  Distribuição allocated:")
    for layer, cnt in sorted(layer_dist.items()):
        print(f"    {layer}: {cnt}")

    out = (
        ROOT
        / "results"
        / "metrics_export"
        / "exp4"
        / "baseline"
        / f"exp4_baseline_{int(time.time())}.json"
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nJSON: {out}")
    return 0 if all(s["status"] == "Succeeded" for s in series) else 1


if __name__ == "__main__":
    raise SystemExit(main())
