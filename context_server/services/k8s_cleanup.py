"""
Limpeza de imagens Docker em nós node_agent e avisos de disco em nós Kubernetes.
"""

from __future__ import annotations

import time
from typing import Any, Optional

import requests

from context_server import config
from context_server.database import crud as db_crud
from context_server.logging_config import get_logger

logger = get_logger(__name__)

_last_critical_cleanup_at: dict[str, float] = {}


def _node_disk_used_percent(node: dict[str, Any]) -> Optional[float]:
    """Retorna percentual de disco USADO (0–100) a partir do registro do nó."""
    used = node.get("disk_used_percent")
    if used is not None:
        try:
            return float(used)
        except (TypeError, ValueError):
            pass
    total = node.get("disk_total_gb")
    avail = node.get("disk_available_gb")
    try:
        t = float(total) if total is not None else 0.0
        a = float(avail) if avail is not None else None
    except (TypeError, ValueError):
        return None
    if t <= 0 or a is None:
        return None
    return max(0.0, min(100.0, (1.0 - (a / t)) * 100.0))


def _node_disk_critical(node: dict[str, Any]) -> bool:
    """True se o disco atingiu o threshold de criticidade (ex.: >= 80% usado)."""
    used_pct = _node_disk_used_percent(node)
    if used_pct is None:
        return False
    return used_pct >= float(config.DISK_CRITICAL_USED_PERCENT)


def _maybe_warn_kubernetes_disk(node: dict[str, Any]) -> None:
    """Kubelet gerencia imagens; avisamos se houver métricas de disco no DB e uso >= threshold."""
    if not _node_disk_critical(node):
        return
    nid = str(node.get("node_id") or "")
    used_pct = _node_disk_used_percent(node) or 0.0
    logger.warning(
        "K8s nó %s: disco aparentemente %.1f%% ocupado (ephemeral/capacity no DB). "
        "GC de imagens costuma ser gerenciada pelo kubelet no cluster.",
        nid,
        used_pct,
    )


def _request_node_agent_cleanup(
    ip: str,
    port: int,
    *,
    rmi_images: list[str],
    prune_dangling: bool,
    prune_all_unused: bool,
    timeout: int = 120,
) -> tuple[bool, str]:
    url = config.build_agent_api_url(ip, int(port), "/docker_cleanup")
    try:
        r = requests.post(
            url,
            json={
                "rmi_images": rmi_images,
                "prune_dangling": prune_dangling,
                "prune_all_unused": prune_all_unused,
            },
            timeout=timeout,
        )
        if r.status_code == 200:
            return True, (r.text or "")[:500]
        return False, f"HTTP {r.status_code}: {(r.text or '')[:400]}"
    except requests.RequestException as e:
        return False, str(e)[:400]


def _process_node_agent(
    db_path: str,
    node: dict[str, Any],
    *,
    aggressive: bool,
) -> bool:
    nid = str(node.get("node_id") or "")
    ip = str(node.get("ip_address") or "").strip()
    try:
        port = int(node.get("agent_port") or config.NODE_AGENT_PORT)
    except (TypeError, ValueError):
        port = config.NODE_AGENT_PORT
    if not ip:
        logger.debug("Limpeza: nó %s sem IP, ignorando.", nid)
        return False

    if not aggressive and not _node_disk_critical(node):
        logger.debug(
            "Limpeza: nó %s com disco abaixo do threshold (%.0f%% usado requerido), ignorando.",
            nid,
            config.DISK_CRITICAL_USED_PERCENT,
        )
        return False

    stale_seconds = float(config.IMAGE_CLEANUP_STALE_DAYS) * 86400.0
    try:
        rmi_images = db_crud.get_stale_docker_images_for_node(db_path, nid, stale_seconds)
    except Exception as e:
        logger.warning("Limpeza: falha ao listar imagens obsoletas no nó %s: %s", nid, e)
        rmi_images = []

    try:
        protected = db_crud.get_protected_docker_images_for_node(db_path, nid)
    except Exception as e:
        logger.warning("Limpeza: falha ao listar imagens protegidas no nó %s: %s", nid, e)
        protected = set()

    if protected:
        before = len(rmi_images)
        rmi_images = [img for img in rmi_images if img not in protected]
        if before != len(rmi_images):
            logger.info(
                "Limpeza: nó %s — %s imagem(ns) protegida(s) por tarefas ativas removidas da lista rmi.",
                nid,
                before - len(rmi_images),
            )

    # F10: não usar prune -a genérico; limpeza agressiva = mesma lista rmi + prune dangling leve
    ok, detail = _request_node_agent_cleanup(
        ip,
        port,
        rmi_images=rmi_images,
        prune_dangling=True,
        prune_all_unused=False,
    )
    if ok:
        logger.info(
            "Limpeza Docker nó %s: ok (aggressive=%s, rmi_count=%s). %s",
            nid,
            aggressive,
            len(rmi_images),
            detail[:200] if detail else "",
        )
    else:
        logger.warning("Limpeza Docker nó %s falhou: %s", nid, detail)
    return ok


def clean_unused_images_job(
    db_path: Optional[str] = None,
    *,
    node_id: Optional[str] = None,
    aggressive: bool = False,
) -> None:
    """
    Job de limpeza: nós Kubernetes apenas aviso de disco; node_agent recebe POST /docker_cleanup.

    Limpeza não agressiva só roda se disco >= DISK_CRITICAL_USED_PERCENT.
    Limpeza agressiva (monitor): mesma política de rmi filtrada, sem docker image prune -a.
    """
    db = db_path or config.DATABASE_PATH
    now = time.time()

    if aggressive and node_id:
        last = _last_critical_cleanup_at.get(str(node_id), 0.0)
        if now - last < float(config.IMAGE_CLEANUP_CRITICAL_COOLDOWN_SECONDS):
            logger.debug(
                "Limpeza agressiva ignorada (cooldown) para nó %s",
                node_id,
            )
            return

    try:
        nodes = db_crud.get_all_nodes(db)
    except Exception as e:
        logger.error("Limpeza: não foi possível listar nós: %s", e)
        return

    if node_id:
        nid = str(node_id).strip()
        nodes = [n for n in nodes if str(n.get("node_id") or "") == nid]

    critical_target_ok = False
    for node in nodes:
        ptype = (node.get("provider_type") or "node_agent").strip()
        if ptype == "kubernetes":
            _maybe_warn_kubernetes_disk(node)
            continue
        if ptype != "node_agent":
            continue
        try:
            ok = _process_node_agent(db, node, aggressive=aggressive)
            if ok and aggressive and node_id and str(node.get("node_id") or "") == str(node_id):
                critical_target_ok = True
        except Exception as e:
            logger.warning("Limpeza: erro inesperado no nó %s: %s", node.get("node_id"), e)

    if aggressive and node_id and critical_target_ok:
        _last_critical_cleanup_at[str(node_id)] = time.time()
