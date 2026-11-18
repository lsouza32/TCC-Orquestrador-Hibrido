"""
Processa a fila de tarefas (status 'na_fila'): despacha execução via Job Kubernetes (Kind).
Executado periodicamente pelo scheduler e após registro de nó (legado).

Antes do despacho: consulta nós ativos em tempo real, re-ranqueia com NodeRanker e
injeta o vencedor no nodeSelector. Skip apenas se não houver nós ativos; se requisitos
excederem o cluster, aplica clamp best-effort (anti-deadlock).
"""

import copy
import os
from typing import Any, Optional

from context_server import config
from context_server.database import crud
from context_server.logging_config import get_logger, log_error_with_context, log_success
from context_server.realtime import sse_hub
from context_server.services.k8s_job_executor import (
    allocation_id_for_kubernetes_task,
    create_kubernetes_job,
    extract_k8s_job_spec,
    is_kubernetes_configured,
)
from context_server.services.k8s_quantity import (
    parse_cpu_quantity_to_cores,
    parse_memory_quantity_to_gb,
)
from context_server.services.node_ranker import (
    filter_kubernetes_nodes,
    get_top_nodes_for_task,
    max_effective_resources_among,
)
from context_server.services.compute_layer import preferred_layer_from_task_row
from context_server.services.reallocation import (
    excluded_hostnames_from_task,
    filter_nodes_excluding_hostnames,
)
from context_server.services.strategist_guard import apply_ranked_winner_node_selector

logger = get_logger(__name__)


def _hostname_from_ranked(ranked: list[dict[str, Any]]) -> str:
    if not ranked:
        return ""
    top = ranked[0] if isinstance(ranked[0], dict) else {}
    return str(top.get("k8s_hostname") or top.get("node_id") or "").strip()


def _required_from_effective_spec(spec: dict[str, Any]) -> tuple[Optional[float], Optional[float]]:
    """Deriva required_cpu_threads / required_memory_gb a partir dos limits efetivos do Job."""
    cpu = parse_cpu_quantity_to_cores(spec.get("cpu_limit") or spec.get("cpu_request"))
    ram = parse_memory_quantity_to_gb(spec.get("memory_limit") or spec.get("memory_request"))
    return cpu, ram


def _notify_task(user_id: Optional[str], task_id: str, status: str, **extra) -> None:
    if not user_id:
        return
    payload = {"type": "task_update", "task_id": task_id, "status": status}
    payload.update(extra)
    sse_hub.publish(user_id, payload)


def _build_task_req_for_ranker(task_row: dict[str, Any], task_requirements: dict[str, Any]) -> dict[str, Any]:
    docker_image = task_requirements.get("docker_image") or "python:3.9-slim"
    warm_counts: dict[str, int] = {}
    try:
        warm_counts = crud.get_completed_task_counts_by_node_for_image(
            config.DATABASE_PATH, docker_image
        )
    except Exception as e:
        logger.warning("Fila: warm counts indisponíveis para %s: %s", docker_image, e)

    # Preferência de camada a partir do perfil da tarefa (Analista + recursos).
    row_for_layer = dict(task_row) if isinstance(task_row, dict) else {}
    row_for_layer.setdefault("required_memory_gb", task_requirements.get("ram_gb"))
    row_for_layer.setdefault("required_cpu_threads", task_requirements.get("cpu_threads"))
    preferred_layer = preferred_layer_from_task_row(row_for_layer)

    return {
        "required_memory_gb": float(task_requirements.get("ram_gb") or 0.5),
        "required_cpu_threads": float(task_requirements.get("cpu_threads") or 1.0),
        "required_disk_gb": float(task_requirements.get("disk_gb") or 0.0),
        "suggested_docker_image": docker_image,
        "image_warm_counts": warm_counts,
        "preferred_layer": preferred_layer,
    }


def _cpu_to_k8s_qty(cores: float) -> str:
    if cores <= 0:
        return "100m"
    if cores < 1.0:
        return f"{max(1, int(cores * 1000))}m"
    if abs(cores - round(cores)) < 1e-6:
        return str(int(round(cores)))
    return f"{int(cores * 1000)}m"


def _ram_to_k8s_qty(gb: float) -> str:
    if gb <= 0:
        return "128Mi"
    mib = max(64, int(gb * 1024))
    if mib >= 1024 and mib % 1024 == 0:
        return f"{mib // 1024}Gi"
    return f"{mib}Mi"


def _clamp_k8s_spec_to_capacity(
    k8s_spec: dict[str, Any],
    max_cpu: float,
    max_ram: float,
) -> dict[str, Any]:
    """Alinha requests/limits do spec ao teto efetivo do cluster (best-effort)."""
    spec = dict(k8s_spec) if isinstance(k8s_spec, dict) else {}
    if max_cpu > 0:
        qty = _cpu_to_k8s_qty(max_cpu)
        # Não aumentar se o spec já pedia menos; só capar o que excede.
        cur_cpu = parse_cpu_quantity_to_cores(spec.get("cpu_limit") or spec.get("cpu_request"))
        if cur_cpu is None or cur_cpu > max_cpu:
            spec["cpu_request"] = qty
            spec["cpu_limit"] = qty
    if max_ram > 0:
        qty = _ram_to_k8s_qty(max_ram)
        cur_ram = parse_memory_quantity_to_gb(spec.get("memory_limit") or spec.get("memory_request"))
        if cur_ram is None or cur_ram > max_ram:
            spec["memory_request"] = qty
            spec["memory_limit"] = qty
    return spec


def _rank_with_optional_best_effort(
    task_id: str,
    task_req: dict[str, Any],
    active_nodes: list[dict[str, Any]],
    k8s_hints: dict[str, Any],
) -> tuple[Optional[list[dict[str, Any]]], dict[str, Any], dict[str, Any]]:
    """
    Ranqueia nós; se filtro duro falhar com nós ativos, clampa requisitos e tenta de novo.

    Returns:
        (ranked, task_req_final, k8s_hints_final) — ranked None só se impossível.
    """
    ranked = get_top_nodes_for_task(task_req, active_nodes)
    if ranked:
        return ranked, task_req, k8s_hints

    max_cpu, max_ram, max_disk = max_effective_resources_among(active_nodes)

    # Regra dos 80%: permite alocação apenas em nós que tenham pelo menos 80% do piso necessário (até 20% a menos que o piso)
    floor_cpu = float(task_req.get("required_cpu_threads") or 1.0) * 0.80
    floor_ram = float(task_req.get("required_memory_gb") or 0.5) * 0.80
    floor_disk = float(task_req.get("required_disk_gb") or 0.0) * 0.80

    if max_cpu < floor_cpu or max_ram < floor_ram or (floor_disk > 0 and max_disk < floor_disk):
        logger.warning(
            "Fila: tarefa %s — nenhum nó ativo possui o mínimo de 80%% do piso "
            "(piso req=%.2f/%.2fGB/%.2fGB, limite 80%%=%.2f/%.2fGB/%.2fGB, teto cluster=%.2f/%.2fGB/%.2fGB); mantendo na fila.",
            task_id,
            float(task_req.get("required_cpu_threads") or 1.0),
            float(task_req.get("required_memory_gb") or 0.5),
            float(task_req.get("required_disk_gb") or 0.0),
            floor_cpu,
            floor_ram,
            floor_disk,
            max_cpu,
            max_ram,
            max_disk,
        )
        return None, task_req, k8s_hints

    clamped_req = dict(task_req)
    clamped_req["required_cpu_threads"] = max(floor_cpu, min(float(task_req["required_cpu_threads"]), max_cpu))
    clamped_req["required_memory_gb"] = max(floor_ram, min(float(task_req["required_memory_gb"]), max_ram))
    if float(task_req.get("required_disk_gb") or 0) > 0 and max_disk > 0:
        clamped_req["required_disk_gb"] = max(floor_disk, min(float(task_req["required_disk_gb"]), max_disk))

    logger.warning(
        "Fila: tarefa %s — alocação ajustada dentro da tolerância de 80%% do piso "
        "cpu=%.2f->%.2f ram=%.2f->%.2f",
        task_id,
        float(task_req["required_cpu_threads"]),
        float(clamped_req["required_cpu_threads"]),
        float(task_req["required_memory_gb"]),
        float(clamped_req["required_memory_gb"]),
    )

    hints = _clamp_k8s_spec_to_capacity(
        copy.deepcopy(k8s_hints) if k8s_hints else {},
        float(clamped_req["required_cpu_threads"]),
        float(clamped_req["required_memory_gb"]),
    )
    ranked2 = get_top_nodes_for_task(clamped_req, active_nodes)
    return (ranked2 or None), clamped_req, hints


def process_queue_job(db_path):
    """
    Para cada tarefa com status 'na_fila', reavalia nós ativos, ranqueia e cria Job K8s.
    """
    try:
        tasks = crud.get_tasks_in_queue(db_path)
        if not tasks:
            return

        if not is_kubernetes_configured():
            from context_server.services.k8s_job_executor import configure_kubernetes
            configure_kubernetes()

        if not is_kubernetes_configured():
            logger.warning(
                "Fila: há tarefas em na_fila mas Kubernetes não está configurado "
                "(configure_kubernetes falhou ou não rodou); nenhum Job será criado."
            )
            return

        for task_row in tasks:
            task_id = task_row["task_id"]
            task_type = task_row.get("task_type")
            if not task_type:
                continue
            fallback = config.TASK_REQUIREMENTS.get(task_type) or {}
            task_requirements = {
                "cpu_threads": task_row.get("required_cpu_threads") or fallback.get("cpu_threads", 2),
                "ram_gb": task_row.get("required_memory_gb") or fallback.get("ram_gb", 0.5),
                "disk_gb": task_row.get("required_disk_gb") or fallback.get("disk_gb", 1.0),
                "docker_image": task_row.get("docker_image") or fallback.get("docker_image", "python:3.9-slim"),
            }
            file_path_on_server = task_row.get("file_path")
            filename = task_row.get("filename") or os.path.basename(file_path_on_server or "")
            if not file_path_on_server or not os.path.isfile(file_path_on_server):
                logger.warning(f"Tarefa {task_id} na fila: arquivo não encontrado, mantendo na fila.")
                continue

            try:
                active_nodes = crud.get_active_nodes(db_path)
            except Exception as e:
                log_error_with_context(logger, e, f"Fila: get_active_nodes para {task_id}")
                continue

            # Jobs K8s só podem pinar kubernetes.io/hostname de workers do cluster.
            # node_agent (ex.: PC-lucas) pode estar "ativo" por heartbeat e ainda
            # assim não existir no Kind — Pod fica Pending e a tarefa trava.
            k8s_nodes = filter_kubernetes_nodes(active_nodes)
            skipped_agents = len(active_nodes) - len(k8s_nodes)
            if skipped_agents > 0:
                logger.info(
                    "Fila: tarefa %s — %d node_agent ignorado(s) no ranking K8s "
                    "(apenas provider_type=kubernetes).",
                    task_id,
                    skipped_agents,
                )

            excluded = excluded_hostnames_from_task(task_row)
            if excluded:
                before = len(k8s_nodes)
                k8s_nodes = filter_nodes_excluding_hostnames(k8s_nodes, excluded)
                logger.info(
                    "Fila: tarefa %s — excluindo nós falhos no ranking: %s "
                    "(%d → %d candidatos)",
                    task_id,
                    ",".join(excluded),
                    before,
                    len(k8s_nodes),
                )

            if not k8s_nodes:
                if excluded:
                    logger.warning(
                        "Fila: tarefa %s — sem nós após exclusão de falhas %s; "
                        "marcando falha definitiva.",
                        task_id,
                        ",".join(excluded),
                    )
                    try:
                        crud.update_task_status(db_path, task_id, "falha")
                        crud.save_task_result(
                            db_path,
                            task_id,
                            "falha",
                            0.0,
                            "",
                            f"Realocação esgotada: nenhum nó elegível após excluir {excluded}",
                        )
                        _notify_task(
                            task_row.get("user_id"),
                            task_id,
                            "falha",
                            reallocation_exhausted=True,
                            excluded_hostnames=excluded,
                        )
                    except Exception as e:
                        log_error_with_context(logger, e, f"Falhar tarefa {task_id} sem nós")
                    continue
                logger.info(
                    "Fila: tarefa %s — nenhum nó Kubernetes ativo; mantendo na_fila "
                    "(aguardar sync do watcher).",
                    task_id,
                )
                continue

            task_req = _build_task_req_for_ranker(task_row, task_requirements)
            if excluded:
                task_req["excluded_hostnames"] = excluded
            k8s_hints = extract_k8s_job_spec(task_row)
            ranked, _final_req, k8s_hints = _rank_with_optional_best_effort(
                task_id, task_req, k8s_nodes, k8s_hints
            )
            if not ranked:
                logger.warning(
                    "Fila: tarefa %s — ranking vazio após best-effort; mantendo na fila.",
                    task_id,
                )
                continue

            k8s_hints, inj_meta = apply_ranked_winner_node_selector(k8s_hints, ranked)
            winner_host = _hostname_from_ranked(ranked) or str(
                (inj_meta or {}).get("injected_hostname") or ""
            ).strip()
            logger.info(
                "Fila: tarefa %s — nodeSelector injetado hostname=%s (source=%s)",
                task_id,
                winner_host or (inj_meta or {}).get("injected_hostname"),
                (inj_meta or {}).get("source"),
            )

            ok, job_name, err_msg, effective_spec = create_kubernetes_job(task_row, k8s_hints)
            uid = task_row.get("user_id")

            if not ok:
                logger.warning("Fila: falha ao criar Job para %s: %s", task_id, err_msg or "erro desconhecido")
                continue

            # Persiste spec efetivo (limits + nodeSelector) para RL/near_ceiling e warm start.
            if isinstance(effective_spec, dict) and effective_spec:
                try:
                    req_cpu, req_ram = _required_from_effective_spec(effective_spec)
                    crud.persist_effective_k8s_dispatch(
                        db_path,
                        task_id,
                        effective_spec,
                        required_cpu_threads=req_cpu,
                        required_memory_gb=req_ram,
                            dispatch_meta={
                            "injected_hostname": winner_host,
                            "source": (inj_meta or {}).get("source"),
                            "best_effort": _final_req is not task_req,
                            "k8s_job": job_name,
                            "preferred_layer": task_req.get("preferred_layer"),
                            "dispatched_at": __import__("time").time(),
                        },
                    )
                except Exception as e:
                    log_error_with_context(logger, e, f"Persistir k8s_job_spec efetivo {task_id}")

            try:
                alloc = allocation_id_for_kubernetes_task(task_id, hostname=winner_host)
                winner_layer = None
                if ranked and isinstance(ranked[0], dict):
                    winner_layer = ranked[0].get("compute_layer")
                pref_layer = task_req.get("preferred_layer") or task_row.get("preferred_compute_layer")
                crud.update_task_allocation(
                    db_path,
                    task_id,
                    "executando",
                    alloc,
                    allocated_compute_layer=winner_layer,
                    preferred_compute_layer=pref_layer,
                    mark_dispatched=True,
                )
                try:
                    from context_server.services.tcc_metrics import log_allocation_timing

                    log_allocation_timing(
                        task_id,
                        submitted_at=task_row.get("submitted_at"),
                        planning_completed_at=task_row.get("planning_completed_at"),
                        preferred_layer=pref_layer,
                        allocated_layer=winner_layer,
                        hostname=winner_host,
                    )
                except Exception:
                    pass
            except Exception as e:
                log_error_with_context(logger, e, f"Atualizar tarefa {task_id} após criar Job")
                continue

            _notify_task(uid, task_id, "executando", allocated_node_id=alloc, k8s_job=job_name)
            log_success(
                logger,
                f"Tarefa {task_id} despachada como Job Kubernetes",
                f"job={job_name} imagem={task_requirements.get('docker_image')} "
                f"node={winner_host} layer={winner_layer}",
            )

    except Exception as e:
        log_error_with_context(logger, e, "Processar fila de tarefas")
