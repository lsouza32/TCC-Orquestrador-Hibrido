"""
Coleta de picos de CPU/RAM de Pods via metrics.k8s.io (quando disponível).
Usado pelo k8s_watcher durante a janela de retenção pós-script.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

logger = logging.getLogger(__name__)


def telemetry_retention_seconds() -> int:
    try:
        return max(15, int(os.getenv("K8S_TELEMETRY_RETENTION_SECONDS", "60")))
    except (TypeError, ValueError):
        return 60


def prestop_sleep_seconds() -> int:
    try:
        return max(5, int(os.getenv("K8S_TELEMETRY_PRESTOP_SECONDS", "30")))
    except (TypeError, ValueError):
        return 30


def retention_shell_suffix() -> str:
    """Mantém o container vivo após o script para o watcher amostrar métricas."""
    secs = telemetry_retention_seconds()
    return (
        f'EC=$?; echo "[TCC] Janela de retenção para telemetria ({secs}s)"; '
        f"i=0; while [ $i -lt {secs} ]; do "
        f'if [ -f /tmp/tcc-broker-release ]; then break; fi; '
        f"i=$((i+1)); sleep 1; done; exit $EC"
    )


def prestop_lifecycle_command() -> list[str]:
    """Hook preStop: espera em SIGTERM (ex.: delete do Job pelo Broker)."""
    secs = prestop_sleep_seconds()
    return [
        "/bin/sh",
        "-c",
        (
            f'echo "[TCC] preStop telemetry ({secs}s max)"; '
            f"i=0; while [ $i -lt {secs} ]; do "
            f'if [ -f /tmp/tcc-broker-release ]; then exit 0; fi; '
            f"i=$((i+1)); sleep 1; done; exit 0"
        ),
    ]


def _parse_cpu_to_millicores(value: Any) -> Optional[float]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        if s.endswith("n"):
            return float(s[:-1]) / 1_000_000.0
        if s.endswith("u"):
            return float(s[:-1]) / 1000.0
        if s.endswith("m"):
            return float(s[:-1])
        return float(s) * 1000.0
    except (TypeError, ValueError):
        return None


def _parse_memory_to_bytes(value: Any) -> Optional[float]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        if s.endswith("Ki"):
            return float(s[:-2]) * 1024.0
        if s.endswith("Mi"):
            return float(s[:-2]) * 1024.0**2
        if s.endswith("Gi"):
            return float(s[:-2]) * 1024.0**3
        if s.endswith("K"):
            return float(s[:-1]) * 1000.0
        if s.endswith("M"):
            return float(s[:-1]) * 1000.0**2
        if s.endswith("G"):
            return float(s[:-1]) * 1000.0**3
        return float(s)
    except (TypeError, ValueError):
        return None


def collect_pod_container_peaks(
    namespace: str,
    pod_name: str,
    container_name: str = "task-runner",
    cpu_limit_millicores: Optional[float] = None,
) -> tuple[Optional[float], Optional[float]]:
    """
    Lê metrics.k8s.io para o container e devolve (cpu_peak_percent, ram_peak_mb).
    """
    try:
        from kubernetes import client  # type: ignore[import-untyped]
        from kubernetes.client import ApiException  # type: ignore[import-untyped]

        api = client.CustomObjectsApi()
        obj = api.get_namespaced_custom_object(
            group="metrics.k8s.io",
            version="v1beta1",
            namespace=namespace,
            plural="pods",
            name=pod_name,
            _request_timeout=(5.0, 15.0),
        )
    except ApiException as e:
        if e.status != 404:
            logger.debug("metrics.k8s.io pod %s: %s %s", pod_name, e.status, e.reason)
        return None, None
    except Exception as e:
        logger.debug("metrics.k8s.io indisponível para %s: %s", pod_name, e)
        return None, None

    containers = (obj or {}).get("containers") or []
    usage_cpu_m = None
    usage_ram_b = None
    for c in containers:
        if str(c.get("name")) != container_name:
            continue
        usage = c.get("usage") or {}
        usage_cpu_m = _parse_cpu_to_millicores(usage.get("cpu"))
        usage_ram_b = _parse_memory_to_bytes(usage.get("memory"))
        break

    if usage_ram_b is None and usage_cpu_m is None:
        return None, None

    ram_mb = round(usage_ram_b / (1024.0**2), 2) if usage_ram_b is not None else None

    cpu_pct = None
    if usage_cpu_m is not None:
        if cpu_limit_millicores and cpu_limit_millicores > 0:
            cpu_pct = round(min(100.0, (usage_cpu_m / cpu_limit_millicores) * 100.0), 2)
        else:
            cpu_pct = round(min(100.0, usage_cpu_m / 10.0), 2)

    return cpu_pct, ram_mb


def merge_peak_samples(
    current: dict[str, Optional[float]],
    cpu_pct: Optional[float],
    ram_mb: Optional[float],
) -> dict[str, Optional[float]]:
    out = dict(current)
    if cpu_pct is not None:
        prev = out.get("cpu_peak_percent")
        out["cpu_peak_percent"] = max(float(prev or 0), float(cpu_pct))
    if ram_mb is not None:
        prev = out.get("ram_peak_mb")
        out["ram_peak_mb"] = max(float(prev or 0), float(ram_mb))
    return out


def fetch_cluster_node_usage() -> dict[str, dict[str, float]]:
    """
    Lê metrics.k8s.io/v1beta1/nodes e devolve por hostname:
      cpu_used_percent, memory_used_percent, memory_available_gb (aprox.).

    Usado pelo NodeRanker (vetor dvCpu/dvMem) — sem isso todos os workers
    caem no fallback de telemetria nula (~50% de carga fictícia).
    """
    try:
        from kubernetes import client  # type: ignore[import-untyped]
        from kubernetes.client import ApiException  # type: ignore[import-untyped]

        api = client.CustomObjectsApi()
        obj = api.list_cluster_custom_object(
            group="metrics.k8s.io",
            version="v1beta1",
            plural="nodes",
            _request_timeout=(5.0, 15.0),
        )
    except Exception as e:
        logger.warning("metrics.k8s.io/nodes indisponível: %s", e)
        return {}

    # Capacidade via CoreV1 para percentuais
    capacity: dict[str, tuple[float, float]] = {}
    try:
        from kubernetes import client as _c  # type: ignore[import-untyped]

        for kn in _c.CoreV1Api().list_node(_request_timeout=(5.0, 15.0)).items or []:
            name = kn.metadata.name
            alloc = (kn.status and kn.status.allocatable) or {}
            cpu_m = _parse_cpu_to_millicores(alloc.get("cpu")) or 1000.0
            # allocatable cpu "12" → 12000m
            if cpu_m < 100:  # cores sem sufixo já convertidos *1000 em _parse
                pass
            mem_b = _parse_memory_to_bytes(alloc.get("memory")) or (1.0 * 1024**3)
            capacity[name] = (float(cpu_m), float(mem_b))
    except Exception as e:
        logger.debug("Capacidade dos nós para metrics: %s", e)

    out: dict[str, dict[str, float]] = {}
    for item in (obj or {}).get("items") or []:
        meta = item.get("metadata") or {}
        name = str(meta.get("name") or "").strip()
        if not name:
            continue
        usage = item.get("usage") or {}
        cpu_m = _parse_cpu_to_millicores(usage.get("cpu"))
        mem_b = _parse_memory_to_bytes(usage.get("memory"))
        cap_cpu_m, cap_mem_b = capacity.get(name, (12000.0, 8.0 * 1024**3))
        cpu_pct = (
            round(min(100.0, max(0.0, (cpu_m / cap_cpu_m) * 100.0)), 2)
            if cpu_m is not None and cap_cpu_m > 0
            else None
        )
        mem_pct = (
            round(min(100.0, max(0.0, (mem_b / cap_mem_b) * 100.0)), 2)
            if mem_b is not None and cap_mem_b > 0
            else None
        )
        mem_avail_gb = None
        if mem_b is not None and cap_mem_b > 0:
            mem_avail_gb = round(max(0.0, (cap_mem_b - mem_b) / (1024.0**3)), 3)
        entry: dict[str, float] = {}
        if cpu_pct is not None:
            entry["cpu_used_percent"] = float(cpu_pct)
            entry["cpu_available_percent"] = float(round(max(0.0, 100.0 - cpu_pct), 2))
        if mem_pct is not None:
            entry["memory_used_percent"] = float(mem_pct)
        if mem_avail_gb is not None:
            entry["memory_available_gb"] = float(mem_avail_gb)
        if entry:
            out[name] = entry
    return out
