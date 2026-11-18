#!/usr/bin/env python3
"""
EXP-03 — braço Kubernetes Padrão (anti-deadlock / oversize → Pending).

1) Lê Allocatable de RAM dos workers
2) Calcula request/limit > R_max (oversize garantido)
3) Gera docs/experimentos/exp3_baseline_job.yaml e aplica
4) Monitora até 90 s: Pod deve ficar Pending com FailedScheduling
5) Captura eventos, limpa recursos, grava JSON em results/metrics_export/exp3/baseline/

Uso:
  python scripts/run_exp3_baseline.py
"""

from __future__ import annotations

import argparse
import json
import math
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_YAML = ROOT / "docs" / "experimentos" / "exp3_baseline_job.yaml"
JOB_NAME = "exp3-baseline-oversize"
LABEL_SEL = "app=tcc-baseline,experiment=exp3"
MONITOR_DEFAULT_S = 90.0
# Pedido padronizado entre os dois bracos (baseline e hibrido): 9,45 Gi.
# Fica em (R_max, 1,25*R_max] no cluster Kind (R_max efetivo ~7,566 Gi -> teto 9,457 Gi),
# logo e oversize para o kube-scheduler E admissivel pela janela de 80% do broker.
STANDARD_OVERSIZE_GI = 9.45


class _Tee:
    """Espelha stdout/stderr num arquivo de log (evidencia para artigo/slides)."""

    def __init__(self, stream, fh):
        self._s, self._f = stream, fh

    def write(self, data):
        self._s.write(data)
        try:
            self._f.write(data)
            self._f.flush()
        except Exception:
            pass
        return len(data)

    def flush(self):
        self._s.flush()
        try:
            self._f.flush()
        except Exception:
            pass

    def __getattr__(self, name):
        return getattr(self._s, name)


def start_run_log(arm: str) -> Path:
    log_dir = ROOT / "results" / "metrics_export" / "exp3" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / f"exp3_{arm}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    fh = path.open("w", encoding="utf-8", errors="replace")
    sys.stdout = _Tee(sys.stdout, fh)
    sys.stderr = _Tee(sys.stderr, fh)
    return path


def _run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, check=check, text=True, capture_output=True)


def kubectl(args: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return _run(["kubectl", *args], check=check)


def parse_k8s_memory_to_bytes(qty: str) -> int:
    """Converte quantity K8s (Ki/Mi/Gi/Ti ou bytes) para bytes."""
    s = str(qty).strip()
    m = re.fullmatch(r"(?i)(\d+(?:\.\d+)?)([KMGT]i?)?", s)
    if not m:
        raise ValueError(f"Quantidade de memória inválida: {qty!r}")
    value = float(m.group(1))
    unit = (m.group(2) or "").lower()
    factors = {
        "": 1,
        "k": 1000,
        "m": 1000**2,
        "g": 1000**3,
        "t": 1000**4,
        "ki": 1024,
        "mi": 1024**2,
        "gi": 1024**3,
        "ti": 1024**4,
    }
    if unit not in factors:
        raise ValueError(f"Unidade desconhecida: {unit!r} em {qty!r}")
    return int(value * factors[unit])


def bytes_to_gi(n: int) -> float:
    return n / (1024**3)


def get_worker_allocatable_memory() -> list[dict]:
    """Retorna lista de workers com memória allocatable (exclui control-plane)."""
    r = kubectl(["get", "nodes", "-o", "json"])
    data = json.loads(r.stdout)
    workers: list[dict] = []
    for node in data.get("items") or []:
        meta = node.get("metadata") or {}
        labels = meta.get("labels") or {}
        name = meta.get("name") or ""
        roles = str(labels.get("kubernetes.io/role") or "")
        is_cp = (
            "node-role.kubernetes.io/control-plane" in labels
            or "node-role.kubernetes.io/master" in labels
            or roles == "control-plane"
            or "control-plane" in name
        )
        if is_cp:
            continue
        alloc = ((node.get("status") or {}).get("allocatable") or {}).get("memory")
        if not alloc:
            continue
        b = parse_k8s_memory_to_bytes(alloc)
        workers.append(
            {
                "name": name,
                "allocatable_raw": alloc,
                "allocatable_bytes": b,
                "allocatable_gi": round(bytes_to_gi(b), 3),
                "compute_layer": labels.get("tcc.utfpr.edu.br/compute-layer"),
            }
        )
    if not workers:
        raise RuntimeError("Nenhum worker encontrado com allocatable memory")
    return workers


def choose_oversize_gi(max_allocatable_gi: float) -> int:
    """
    Escolhe um request em GiB estritamente maior que R_max.
    Usa ceil(R_max)+2 para ficar claramente unschedulable (ex.: 7.71 → 10).
    """
    return max(int(math.ceil(max_allocatable_gi)) + 2, int(math.floor(max_allocatable_gi)) + 3)


def memory_quantity(memory_gi: float) -> str:
    """Mesma regra de quantidade usada em run_exp3_hybrid.inject_oversize."""
    if float(memory_gi).is_integer():
        return f"{int(memory_gi)}Gi"
    return f"{int(memory_gi * 1024)}Mi"


def render_job_yaml(memory_gi: float, namespace: str) -> str:
    mem = memory_quantity(memory_gi)
    return f"""# EXP-03 — Job oversize (gerado por scripts/run_exp3_baseline.py)
# request/limit = {mem} > allocatable do maior worker → Pending / FailedScheduling
---
apiVersion: batch/v1
kind: Job
metadata:
  name: {JOB_NAME}
  namespace: {namespace}
  labels:
    app: tcc-baseline
    experiment: exp3
    arm: k8s-default
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 86400
  template:
    metadata:
      labels:
        app: tcc-baseline
        experiment: exp3
        arm: k8s-default
    spec:
      restartPolicy: Never
      containers:
        - name: task-runner
          image: python:3.11-slim
          imagePullPolicy: IfNotPresent
          command: ["/bin/sh", "-c"]
          args:
            - echo "exp3_oversize_hello" && sleep 2
          resources:
            requests:
              cpu: 100m
              memory: {mem}
            limits:
              cpu: 200m
              memory: {mem}
"""


def write_yaml(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def cleanup(namespace: str) -> None:
    kubectl(
        ["delete", "job", JOB_NAME, "-n", namespace, "--ignore-not-found", "--wait=false"],
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


def find_job_pod(namespace: str) -> dict | None:
    r = kubectl(
        ["get", "pods", "-n", namespace, "-l", LABEL_SEL, "-o", "json"],
        check=False,
    )
    if r.returncode != 0:
        return None
    try:
        items = (json.loads(r.stdout).get("items")) or []
    except json.JSONDecodeError:
        return None
    if not items:
        return None
    # Preferir o mais recente
    items.sort(key=lambda p: (p.get("metadata") or {}).get("creationTimestamp") or "")
    return items[-1]


def extract_scheduling_events(pod: dict) -> list[dict]:
    """Extrai evidências FailedScheduling / Insufficient memory do status do Pod."""
    evidence: list[dict] = []
    status = pod.get("status") or {}
    for cond in status.get("conditions") or []:
        msg = str(cond.get("message") or "")
        reason = str(cond.get("reason") or "")
        blob = f"{reason} {msg}".lower()
        if (
            "failedscheduling" in blob
            or "unschedulable" in blob
            or "insufficient memory" in blob
            or "didn't fit" in blob
        ):
            evidence.append(
                {
                    "source": "pod.condition",
                    "type": cond.get("type"),
                    "reason": reason,
                    "message": msg,
                    "status": cond.get("status"),
                }
            )
    return evidence


def describe_events(pod_name: str, namespace: str) -> tuple[str, list[str]]:
    """kubectl describe + linhas relevantes de scheduling."""
    r = kubectl(["describe", "pod", pod_name, "-n", namespace], check=False)
    text = (r.stdout or "") + (r.stderr or "")
    interesting: list[str] = []
    for line in text.splitlines():
        low = line.lower()
        if any(
            k in low
            for k in (
                "failedscheduling",
                "insufficient memory",
                "unschedulable",
                "didn't fit",
                "0/4 nodes",
                "0/3 nodes",
            )
        ):
            interesting.append(line.strip())
    return text, interesting


def main() -> int:
    p = argparse.ArgumentParser(description="EXP-03 baseline: oversize -> Pending")
    p.add_argument("--namespace", default="tcc-baseline")
    p.add_argument("--yaml", type=Path, default=DEFAULT_YAML)
    p.add_argument("--monitor", type=float, default=MONITOR_DEFAULT_S)
    p.add_argument("--poll", type=float, default=5.0)
    p.add_argument("--skip-cleanup", action="store_true")
    p.add_argument(
        "--memory-gi",
        type=float,
        default=STANDARD_OVERSIZE_GI,
        help="Pedido oversize (default 9.45, identico ao braco hibrido).",
    )
    args = p.parse_args()
    log_path = start_run_log("baseline")

    print("=" * 64)
    print("EXP-03 — Baseline Kubernetes Padrão (oversize / Pending)")
    print(f"ns={args.namespace}  monitor={args.monitor:.0f}s")
    print("=" * 64)

    workers = get_worker_allocatable_memory()
    richest = max(workers, key=lambda w: w["allocatable_bytes"])
    r_max_gi = richest["allocatable_gi"]
    request_gi = float(args.memory_gi)
    if request_gi <= r_max_gi:
        print(
            f"ERRO: pedido {request_gi}Gi nao excede R_max={r_max_gi}Gi (nao e oversize).",
            file=sys.stderr,
        )
        return 2

    print("\nWorkers (allocatable memory):")
    for w in workers:
        mark = " <-- R_max" if w["name"] == richest["name"] else ""
        print(
            f"  {w['name']}: {w['allocatable_raw']} ({w['allocatable_gi']} Gi) "
            f"layer={w.get('compute_layer')}{mark}"
        )
    print(f"\nOversize escolhido: {request_gi}Gi  (R_max ~ {r_max_gi} Gi)", flush=True)

    yaml_body = render_job_yaml(request_gi, args.namespace)
    write_yaml(args.yaml, yaml_body)
    print(f"YAML gerado: {args.yaml}", flush=True)

    cleanup(args.namespace)
    time.sleep(1.5)

    print("\nAplicando Job oversize...", flush=True)
    r = kubectl(["apply", "-f", str(args.yaml), "-n", args.namespace])
    print((r.stdout or r.stderr or "").strip(), flush=True)

    started = datetime.now(timezone.utc).isoformat()
    t0 = time.monotonic()
    deadline = t0 + args.monitor

    samples: list[dict] = []
    last_pod_name: str | None = None
    last_phase: str | None = None
    last_node: str | None = None
    event_hits: list[str] = []
    condition_evidence: list[dict] = []
    describe_full = ""

    print(f"\nMonitorando até {args.monitor:.0f}s (esperado: Pending)...", flush=True)
    while time.monotonic() < deadline:
        pod = find_job_pod(args.namespace)
        if pod:
            meta = pod.get("metadata") or {}
            spec = pod.get("spec") or {}
            status = pod.get("status") or {}
            last_pod_name = meta.get("name")
            last_phase = status.get("phase")
            last_node = spec.get("nodeName") or None
            cond_ev = extract_scheduling_events(pod)
            if cond_ev:
                condition_evidence = cond_ev
            sample = {
                "t_s": round(time.monotonic() - t0, 2),
                "pod": last_pod_name,
                "phase": last_phase,
                "nodeName": last_node,
            }
            samples.append(sample)
            print(
                f"  [{datetime.now().strftime('%H:%M:%S')}] "
                f"pod={last_pod_name} phase={last_phase} nodeName={last_node or '—'}",
                flush=True,
            )
            if last_pod_name and (not event_hits or len(samples) % 3 == 0):
                describe_full, hits = describe_events(last_pod_name, args.namespace)
                if hits:
                    event_hits = hits
                    for h in hits[:5]:
                        print(f"    event: {h}", flush=True)
        else:
            print(
                f"  [{datetime.now().strftime('%H:%M:%S')}] Pod ainda não criado…",
                flush=True,
            )
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(args.poll, remaining))

    # Captura final de eventos
    if last_pod_name:
        describe_full, hits = describe_events(last_pod_name, args.namespace)
        if hits:
            event_hits = hits
        pod = find_job_pod(args.namespace)
        if pod:
            last_phase = (pod.get("status") or {}).get("phase") or last_phase
            last_node = (pod.get("spec") or {}).get("nodeName") or last_node
            condition_evidence = extract_scheduling_events(pod) or condition_evidence

    stayed_pending = last_phase == "Pending" and not last_node
    has_fail_sched = any("failedscheduling" in e.lower() for e in event_hits) or any(
        "failedscheduling" in str(c.get("reason", "")).lower()
        or "unschedulable" in str(c.get("reason", "")).lower()
        for c in condition_evidence
    )
    has_insuff_mem = any("insufficient memory" in e.lower() for e in event_hits) or any(
        "insufficient memory" in str(c.get("message", "")).lower() for c in condition_evidence
    )

    print("\nCleanup...", flush=True)
    if not args.skip_cleanup:
        cleanup(args.namespace)
    else:
        print("  --skip-cleanup: recursos mantidos", flush=True)

    report = {
        "experiment": "exp3",
        "arm": "k8s-default",
        "started_at": started,
        "ended_at": datetime.now(timezone.utc).isoformat(),
        "monitor_s": args.monitor,
        "workers": workers,
        "r_max_node": richest["name"],
        "r_max_gi": r_max_gi,
        "memory_request_gi": request_gi,
        "memory_request": memory_quantity(request_gi),
        "pod_name": last_pod_name,
        "final_phase": last_phase,
        "final_nodeName": last_node,
        "stayed_pending": stayed_pending,
        "failed_scheduling_seen": has_fail_sched,
        "insufficient_memory_seen": has_insuff_mem,
        "event_lines": event_hits,
        "condition_evidence": condition_evidence,
        "samples": samples,
        "describe_excerpt": "\n".join(event_hits) if event_hits else describe_full[-4000:],
        "yaml_path": str(args.yaml),
    }

    out = (
        ROOT
        / "results"
        / "metrics_export"
        / "exp3"
        / "baseline"
        / f"exp3_baseline_{int(time.time())}.json"
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n" + "=" * 64)
    print("RESUMO EXP-03 BASELINE")
    print(f"  R_max (maior worker): {r_max_gi} Gi ({richest['name']})")
    print(f"  Request/Limit:        {memory_quantity(request_gi)}")
    print(f"  Pod:                  {last_pod_name}")
    print(f"  Fase final:           {last_phase}")
    print(f"  nodeName:             {last_node or '(vazio)'}")
    print(f"  Permaneceu Pending:   {stayed_pending}")
    print(f"  FailedScheduling:     {has_fail_sched}")
    print(f"  Insufficient memory:  {has_insuff_mem}")
    if event_hits:
        print("  Eventos-chave:")
        for h in event_hits[:8]:
            print(f"    - {h}")
    print(f"\nJSON: {out}")
    desc_path = log_path.with_name(log_path.stem + "_kubectl_describe.txt")
    desc_path.write_text(describe_full or "(sem pod)", encoding="utf-8")
    print(f"kubectl describe completo: {desc_path}")
    print(f"LOG: {log_path}")

    return 0 if stayed_pending and (has_fail_sched or has_insuff_mem) else 1


if __name__ == "__main__":
    raise SystemExit(main())
