"""
Injeção determinística do nodeSelector a partir do vencedor do NodeRanker.

O Strategist (LLM) não escolhe hostname: este módulo força sempre ranked[0]
(ou fallback configurado se o ranking estiver vazio).
"""

from __future__ import annotations

import os
from typing import Any, Optional


def _fallback_hostnames() -> list[str]:
    raw = os.getenv("STRATEGIST_FALLBACK_HOSTNAMES", "worker,worker2,tcc-cluster-worker")
    return [h.strip() for h in raw.split(",") if h.strip()]


def apply_ranked_winner_node_selector(
    k8s_spec: dict[str, Any],
    ranked_nodes: list[dict[str, Any]],
) -> tuple[dict[str, Any], Optional[dict[str, Any]]]:
    """
    Injeta nodeSelector com o hostname do nó #1 do ranking (vencedor).

    Qualquer hostname proposto pela IA é ignorado/sobrescrito.
    Se o ranking estiver vazio, usa o primeiro fallback configurado.

    Returns:
        (k8s_spec com nodeSelector injetado, meta de injeção ou None se já era o vencedor)
    """
    spec = dict(k8s_spec) if isinstance(k8s_spec, dict) else {}
    selector = spec.get("nodeSelector")
    if not isinstance(selector, dict):
        selector = {}
    else:
        selector = dict(selector)

    original = str(
        selector.get("kubernetes.io/hostname") or selector.get("hostname") or ""
    ).strip()

    winner: Optional[str] = None
    for n in ranked_nodes or []:
        if isinstance(n, dict):
            h = str(n.get("k8s_hostname") or n.get("node_id") or "").strip()
            if h:
                winner = h
                break

    fallbacks = _fallback_hostnames()
    if winner:
        chosen = winner
        reason = "injeção forçada do nó #1 do NodeRanker"
        source = "ranked_winner"
    else:
        chosen = fallbacks[0] if fallbacks else "worker"
        reason = "ranking vazio; injeção de fallback configurado"
        source = "fallback"

    spec["nodeSelector"] = {"kubernetes.io/hostname": chosen}

    # Sem meta se já apontava para o vencedor (nada a corrigir).
    if original == chosen and source == "ranked_winner":
        return spec, None

    meta: dict[str, Any] = {
        "original_hostname": original or None,
        "corrected_hostname": chosen,
        "injected_hostname": chosen,
        "reason": reason,
        "source": source,
        "overrode_llm": bool(original and original != chosen),
    }
    return spec, meta


def enforce_strategist_node_selector(
    k8s_spec: dict[str, Any],
    ranked_top3: list[dict[str, Any]],
) -> tuple[dict[str, Any], Optional[dict[str, Any]]]:
    """
    Compat: alias histórico. Agora sempre injeta o vencedor (ranked[0]),
    sem validar escolha da IA.
    """
    return apply_ranked_winner_node_selector(k8s_spec, ranked_top3)
