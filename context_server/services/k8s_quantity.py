"""
Conversão de quantidades Kubernetes (CPU/memória) para unidades usadas pelo ranker e pela IA.
"""

from __future__ import annotations

from typing import Any, Optional


def parse_cpu_quantity_to_cores(value: Any) -> Optional[float]:
    """Converte quantidade de CPU Kubernetes (ex.: 500m, 2) para núcleos fracionários."""
    if value is None:
        return None
    t = str(value).strip()
    if not t:
        return None
    try:
        if t.endswith("m"):
            return float(t[:-1]) / 1000.0
        return float(t)
    except (TypeError, ValueError):
        return None


def parse_memory_quantity_to_gb(value: Any) -> Optional[float]:
    """Converte quantidade de memória Kubernetes (ex.: 512Mi, 2Gi, 256M) para GB (base 1024)."""
    if value is None:
        return None
    t = str(value).strip()
    if not t:
        return None
    try:
        # Sufixos binários (IEC) — ordem importa (Gi antes de G, Mi antes de M).
        if t.endswith("Gi"):
            return float(t[:-2])
        if t.endswith("Mi"):
            return float(t[:-2]) / 1024.0
        if t.endswith("Ki"):
            return float(t[:-2]) / (1024.0**2)
        if t.endswith("Ti"):
            return float(t[:-2]) * 1024.0
        # Sufixos SI (G/M/K maiúsculos) — "m" minúsculo sozinho é millicore de CPU, não memória.
        if len(t) >= 2 and t[-1] == "G" and t[-2].isdigit():
            return float(t[:-1])
        if len(t) >= 2 and t[-1] == "M" and t[-2].isdigit():
            return float(t[:-1]) / 1024.0
        if len(t) >= 2 and t[-1] in ("K", "k") and t[-2].isdigit():
            return float(t[:-1]) / (1024.0**2)
        v = float(t)
        if v > 1_000_000:
            return v / (1024.0**3)
        if v <= 256:
            return v
        return v / (1024.0**3)
    except (TypeError, ValueError):
        return None


def parse_memory_quantity_to_mb(value: Any) -> Optional[float]:
    """Converte quantidade K8s de memória para megabytes (base 1024: 1 Gi = 1024 Mi)."""
    gb = parse_memory_quantity_to_gb(value)
    if gb is None:
        return None
    return float(gb) * 1024.0


def parse_disk_quantity_to_gb(value: Any) -> Optional[float]:
    """Converte quantidade de disco/ephemeral-storage (Mi, Gi, …) para GB."""
    return parse_memory_quantity_to_gb(value)


def parse_requirement_to_float(
    value: Any,
    *,
    kind: str = "generic",
    default: float = 0.0,
) -> float:
    """
    Normaliza requisito numérico que pode vir como float, int ou string K8s.
    kind: 'cpu' | 'memory' | 'disk' | 'generic'
    """
    if value is None:
        return default
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    s = str(value).strip()
    if not s:
        return default
    if kind == "cpu":
        parsed = parse_cpu_quantity_to_cores(s)
    elif kind in ("memory", "disk"):
        parsed = parse_memory_quantity_to_gb(s)
    else:
        parsed = parse_memory_quantity_to_gb(s)
        if parsed is None:
            parsed = parse_cpu_quantity_to_cores(s)
    if parsed is not None:
        return float(parsed)
    try:
        return float(s)
    except (TypeError, ValueError):
        return default
