"""
Logs estruturados [TCC-METRICS] para coleta quantitativa na monografia (grep no terminal do Broker).
"""

from __future__ import annotations

import json
from typing import Any, Optional

from context_server.logging_config import get_logger

logger = get_logger(__name__)

_TAG = "[TCC-METRICS]"


def _fmt_k8s_qty(value: Any) -> str:
    if value is None or value == "":
        return "n/d"
    return str(value).strip()


def format_libraries_for_log(libs: Any) -> str:
    """Normaliza bibliotecas (JSON string, lista ou vazio) para exibição em log."""
    if libs is None:
        return "[]"
    if isinstance(libs, list):
        return json.dumps(libs, ensure_ascii=False)
    s = str(libs).strip()
    if not s:
        return "[]"
    try:
        parsed = json.loads(s)
        if isinstance(parsed, list):
            return json.dumps(parsed, ensure_ascii=False)
    except json.JSONDecodeError:
        pass
    return s


def extract_k8s_resources_from_ai_raw(ai_raw_response: Any) -> dict[str, str]:
    """Extrai cpu/mem req/limit do k8s_job_spec no JSON persistido pela IA."""
    out = {
        "cpu_request": "n/d",
        "cpu_limit": "n/d",
        "memory_request": "n/d",
        "memory_limit": "n/d",
    }
    if not ai_raw_response:
        return out
    try:
        blob = json.loads(ai_raw_response) if isinstance(ai_raw_response, str) else ai_raw_response
    except (json.JSONDecodeError, TypeError):
        return out
    if not isinstance(blob, dict):
        return out
    k8s = blob.get("k8s_job_spec")
    if not isinstance(k8s, dict):
        trace = blob.get("multi_agent_trace") or {}
        if isinstance(trace, dict):
            summary = trace.get("strategist_summary") or {}
            if isinstance(summary, dict):
                k8s = summary.get("k8s_job_spec")
    if not isinstance(k8s, dict):
        return out
    out["cpu_request"] = _fmt_k8s_qty(k8s.get("cpu_request"))
    out["cpu_limit"] = _fmt_k8s_qty(k8s.get("cpu_limit"))
    out["memory_request"] = _fmt_k8s_qty(k8s.get("memory_request"))
    out["memory_limit"] = _fmt_k8s_qty(k8s.get("memory_limit"))
    return out


def log_planejamento(
    task_id: Optional[str],
    libs: Any,
    task_type: Any,
    k8s_spec: Optional[dict[str, Any]] = None,
    *,
    ai_raw_response: Any = None,
) -> None:
    """
    Log unificado de planejamento (Analista + recursos K8s do Strategist).
    """
    tid = (task_id or "n/a").strip()
    libs_s = format_libraries_for_log(libs)
    tt = (str(task_type or "").strip() or "n/d")
    if k8s_spec and isinstance(k8s_spec, dict):
        cpu_req = _fmt_k8s_qty(k8s_spec.get("cpu_request"))
        cpu_lim = _fmt_k8s_qty(k8s_spec.get("cpu_limit"))
        mem_req = _fmt_k8s_qty(k8s_spec.get("memory_request"))
        mem_lim = _fmt_k8s_qty(k8s_spec.get("memory_limit"))
    else:
        res = extract_k8s_resources_from_ai_raw(ai_raw_response)
        cpu_req = res["cpu_request"]
        cpu_lim = res["cpu_limit"]
        mem_req = res["memory_request"]
        mem_lim = res["memory_limit"]
    logger.info(
        "%s[PLANEJAMENTO] Task ID: %s | Libs: %s | Tipo IA: %s | "
        "Recursos Solicitados K8s: CPU Req/Lim=%s/%s, RAM Req/Lim=%s/%s",
        _TAG,
        tid,
        libs_s,
        tt,
        cpu_req,
        cpu_lim,
        mem_req,
        mem_lim,
    )


def compute_prediction_delta_ram(
    suggested_ram_gb: Optional[float],
    ram_peak_mb: Optional[float],
) -> Optional[float]:
    """
    Delta RAM (GB): estimativa da IA menos pico real observado.
    Positivo = superdimensionamento; negativo = subdimensionamento.
    """
    if suggested_ram_gb is None or ram_peak_mb is None:
        return None
    try:
        suggested = float(suggested_ram_gb)
        peak_gb = float(ram_peak_mb) / 1024.0
    except (TypeError, ValueError):
        return None
    return round(suggested - peak_gb, 4)


def log_allocation_timing(
    task_id: str,
    *,
    submitted_at: Any = None,
    planning_completed_at: Any = None,
    preferred_layer: Any = None,
    allocated_layer: Any = None,
    hostname: Any = None,
) -> None:
    """
    Latências de alocação no momento do despacho (dispatched_at ≈ agora).
    planning_s = tempo até IA concluir; queue_s = espera na fila; alloc_s = submit→despacho.
    """
    import time as _time

    tid = (task_id or "n/a").strip()
    now = _time.time()
    planning_s = "n/d"
    queue_s = "n/d"
    alloc_s = "n/d"
    try:
        if submitted_at is not None and planning_completed_at is not None:
            planning_s = f"{max(0.0, float(planning_completed_at) - float(submitted_at)):.3f}"
        if planning_completed_at is not None:
            queue_s = f"{max(0.0, now - float(planning_completed_at)):.3f}"
        if submitted_at is not None:
            alloc_s = f"{max(0.0, now - float(submitted_at)):.3f}"
    except (TypeError, ValueError):
        pass

    logger.info(
        "%s[ALOCACAO] Task ID: %s | planning_s=%s | queue_wait_s=%s | submit_to_dispatch_s=%s | "
        "preferred_layer=%s | allocated_layer=%s | hostname=%s",
        _TAG,
        tid,
        planning_s,
        queue_s,
        alloc_s,
        preferred_layer or "n/d",
        allocated_layer or "n/d",
        (hostname or "n/d"),
    )


def log_reallocation(
    task_id: str,
    *,
    failed_hostname: Optional[str],
    attempt: int,
    max_attempts: int,
    reason: str,
    excluded_hostnames: Optional[list] = None,
) -> None:
    """Log de realocação pós-falha (útil na monografia / grep [TCC-METRICS])."""
    tid = (task_id or "n/a").strip()
    host = (failed_hostname or "n/d").strip() or "n/d"
    excl = excluded_hostnames if isinstance(excluded_hostnames, list) else []
    excl_s = ",".join(str(x) for x in excl) if excl else "(nenhum)"
    logger.info(
        "%s[REALOCACAO] Task ID: %s | Nó falhou: %s | Tentativa: %s/%s | "
        "Motivo: %s | Excluídos: %s | Ação: reenviar_fila",
        _TAG,
        tid,
        host,
        attempt,
        max_attempts,
        str(reason or "falha"),
        excl_s,
    )


def log_execucao_fim(
    task_id: str,
    status: str,
    duration_seconds: float,
    *,
    cpu_peak_percent: Optional[float] = None,
    ram_peak_mb: Optional[float] = None,
    detected_libraries: Any = None,
    suggested_ram_gb: Optional[float] = None,
    prediction_delta_ram: Optional[float] = None,
) -> None:
    """Tríade de logs de fechamento de ciclo (execução, telemetria real, eficiência)."""
    tid = (task_id or "n/a").strip()
    libs_s = format_libraries_for_log(detected_libraries)
    dur = round(float(duration_seconds), 3) if duration_seconds is not None else 0.0

    logger.info(
        "%s[EXECUCAO-FIM] Task ID: %s | Status: %s | Duração: %ss",
        _TAG,
        tid,
        status,
        dur,
    )

    cpu_s = f"{cpu_peak_percent:.2f}" if cpu_peak_percent is not None else "n/d"
    ram_s = f"{ram_peak_mb:.2f}" if ram_peak_mb is not None else "n/d"
    logger.info(
        "%s[TELEMETRIA-REAL] Pico CPU: %s%% | Pico RAM: %s MB | Libs Vinculadas: %s",
        _TAG,
        cpu_s,
        ram_s,
        libs_s,
    )

    suggested = float(suggested_ram_gb) if suggested_ram_gb is not None else None
    peak_gb: Optional[float] = None
    if ram_peak_mb is not None:
        try:
            peak_gb = round(float(ram_peak_mb) / 1024.0, 4)
        except (TypeError, ValueError):
            peak_gb = None

    if suggested is not None and peak_gb is not None:
        delta = prediction_delta_ram
        if delta is None:
            delta = compute_prediction_delta_ram(suggested, ram_peak_mb)
        delta_s = f"{delta:.4f}" if delta is not None else "n/d"
        logger.info(
            "%s[EFICIENCIA] Estimado IA (RAM: %.4fGB) vs Real (Pico RAM: %.4fGB) | Delta: %sGB",
            _TAG,
            suggested,
            peak_gb,
            delta_s,
        )
    else:
        logger.info(
            "%s[EFICIENCIA] Estimado IA (RAM: %sGB) vs Real (Pico RAM: %sGB) | Delta: n/dGB",
            _TAG,
            f"{suggested:.4f}" if suggested is not None else "n/d",
            f"{peak_gb:.4f}" if peak_gb is not None else "n/d",
        )
