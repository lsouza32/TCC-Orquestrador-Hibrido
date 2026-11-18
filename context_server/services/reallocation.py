"""
Realocação pós-falha: reenvia a tarefa à fila excluindo o nó que falhou.

Estado persistido em tasks.ai_raw_response["reallocation"]:
  attempts, excluded_hostnames, history[].
"""

from __future__ import annotations

import json
import time
from typing import Any, Mapping, Optional, Sequence

from context_server import config
from context_server.logging_config import get_logger
from context_server.services.k8s_job_executor import kubernetes_allocation_hostname

logger = get_logger(__name__)

_REALLOC_KEY = "reallocation"


def max_reallocation_attempts() -> int:
    try:
        return max(0, int(getattr(config, "TASK_REALLOCATION_MAX_ATTEMPTS", 3)))
    except (TypeError, ValueError):
        return 3


def _parse_ai_raw(task_row: Mapping[str, Any]) -> dict[str, Any]:
    raw = task_row.get("ai_raw_response") if isinstance(task_row, Mapping) else None
    if isinstance(raw, dict):
        return dict(raw)
    if not raw or not str(raw).strip():
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, TypeError, ValueError):
        return {}


def get_reallocation_state(task_row: Mapping[str, Any]) -> dict[str, Any]:
    data = _parse_ai_raw(task_row)
    block = data.get(_REALLOC_KEY)
    if not isinstance(block, dict):
        block = {}
    attempts = 0
    try:
        attempts = int(block.get("attempts") or 0)
    except (TypeError, ValueError):
        attempts = 0
    excluded: list[str] = []
    raw_ex = block.get("excluded_hostnames") or []
    if isinstance(raw_ex, (list, tuple)):
        for h in raw_ex:
            s = str(h or "").strip()
            if s and s not in excluded:
                excluded.append(s)
    history = block.get("history") if isinstance(block.get("history"), list) else []
    return {
        "attempts": max(0, attempts),
        "excluded_hostnames": excluded,
        "history": list(history),
    }


def excluded_hostnames_from_task(task_row: Mapping[str, Any]) -> list[str]:
    return list(get_reallocation_state(task_row).get("excluded_hostnames") or [])


def failed_hostname_from_task(
    task_row: Mapping[str, Any],
    *,
    pod: Any = None,
) -> Optional[str]:
    """Hostname do nó que falhou (allocated_node_id ou spec.nodeName do Pod)."""
    host = kubernetes_allocation_hostname(
        task_row.get("allocated_node_id") if isinstance(task_row, Mapping) else None
    )
    if host:
        return host
    if pod is not None:
        try:
            spec = getattr(pod, "spec", None)
            node_name = getattr(spec, "node_name", None) if spec is not None else None
            if node_name and str(node_name).strip():
                return str(node_name).strip()
        except Exception:
            pass
    # Fallback: nodeSelector do spec efetivo
    data = _parse_ai_raw(task_row) if isinstance(task_row, Mapping) else {}
    spec = data.get("k8s_job_spec")
    if isinstance(spec, dict):
        ns = spec.get("nodeSelector") or {}
        if isinstance(ns, dict):
            h = ns.get("kubernetes.io/hostname") or ns.get("hostname")
            if h and str(h).strip():
                return str(h).strip()
    return None


def can_reallocate(
    task_row: Mapping[str, Any],
    *,
    failed_hostname: Optional[str] = None,
    available_hostnames: Optional[Sequence[str]] = None,
) -> tuple[bool, str]:
    """
    Decide se a tarefa deve voltar à fila.

    Returns:
        (ok, motivo) — ok=False com motivo quando deve finalizar como falha.
    """
    max_att = max_reallocation_attempts()
    if max_att <= 0:
        return False, "realocacao_desabilitada"

    state = get_reallocation_state(task_row)
    if state["attempts"] >= max_att:
        return False, f"limite_tentativas ({state['attempts']}/{max_att})"

    failed = (failed_hostname or "").strip()
    if available_hostnames is not None:
        excluded = set(state["excluded_hostnames"])
        if failed:
            excluded.add(failed)
        remaining = [
            h for h in available_hostnames
            if str(h or "").strip() and str(h).strip() not in excluded
        ]
        if not remaining:
            return False, "sem_nos_alternativos"

    return True, "ok"


def build_reallocation_ai_raw(
    task_row: Mapping[str, Any],
    *,
    failed_hostname: Optional[str],
    reason: str,
    phase: str = "Failed",
) -> str:
    """Monta novo ai_raw_response com attempts++ e hostname excluído."""
    data = _parse_ai_raw(task_row)
    state = get_reallocation_state(task_row)
    excluded = list(state["excluded_hostnames"])
    host = (failed_hostname or "").strip()
    if host and host not in excluded:
        excluded.append(host)

    attempts = int(state["attempts"]) + 1
    history = list(state["history"])
    history.append(
        {
            "hostname": host or None,
            "reason": str(reason or phase),
            "phase": str(phase),
            "at": time.time(),
            "attempt": attempts,
        }
    )
    # Mantém histórico enxuto
    if len(history) > 20:
        history = history[-20:]

    data[_REALLOC_KEY] = {
        "attempts": attempts,
        "excluded_hostnames": excluded,
        "history": history,
        "last_reason": str(reason or phase),
    }
    return json.dumps(data, ensure_ascii=False)


def filter_nodes_excluding_hostnames(
    nodes: Sequence[Mapping[str, Any]],
    excluded: Sequence[str],
) -> list[dict[str, Any]]:
    """Remove nós cujo hostname/node_id está na lista de exclusão."""
    ban = {str(h).strip().lower() for h in (excluded or []) if str(h or "").strip()}
    if not ban:
        return [dict(n) for n in nodes if isinstance(n, Mapping)]

    out: list[dict[str, Any]] = []
    for node in nodes:
        if not isinstance(node, Mapping):
            continue
        keys = (
            str(node.get("hostname") or "").strip().lower(),
            str(node.get("node_id") or "").strip().lower(),
            str(node.get("k8s_hostname") or "").strip().lower(),
        )
        if any(k and k in ban for k in keys):
            continue
        out.append(dict(node))
    return out


def active_k8s_hostnames(nodes: Sequence[Mapping[str, Any]]) -> list[str]:
    names: list[str] = []
    for node in nodes:
        if not isinstance(node, Mapping):
            continue
        h = str(node.get("hostname") or node.get("node_id") or "").strip()
        if h and h not in names:
            names.append(h)
    return names
