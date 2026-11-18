"""
Ranking híbrido de nós (vetor de desvio de carga, afinidade de imagem, disco) antes do Strategist.

O score de recursos usa o vetor de desvio de carga do cluster (literatura SchedInspector):
  dvCpu_i = lCpu_avg - lCpu_i
  dvMem_i = lMem_avg - lMem_i
com lCpu/lMem = fração de carga efetiva (0–100) derivada da telemetria do nó.
"""

from __future__ import annotations

import time
from typing import Any, Mapping, Optional

from context_server.logging_config import get_logger
from context_server.services.compute_layer import (
    layer_affinity_score,
    node_compute_layer,
    normalize_layer,
)
from context_server.services.k8s_quantity import parse_requirement_to_float

logger = get_logger(__name__)

# Pesos do score híbrido (somam 100). Camada Edge/Fog/Cloud é eixo da proposta do TCC.
WEIGHT_RESOURCES = 40.0
WEIGHT_IMAGE = 25.0
WEIGHT_DISK = 15.0
WEIGHT_LAYER = 20.0

# Nós Kubernetes sem telemetria do agente: não assumir 100% livre
K8S_NULL_TELEMETRY_FACTOR = 0.5

_k8s_allocatable_cache: dict[str, tuple[float, float, float, float]] = {}
_K8S_CACHE_TTL_SECONDS = 45.0


def _f(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except (TypeError, ValueError):
        return default


def _k8s_hostname(node: dict[str, Any]) -> str:
    h = (node.get("hostname") or node.get("node_id") or "").strip()
    return h


def _is_kubernetes_node(node: dict[str, Any]) -> bool:
    return (node.get("provider_type") or "node_agent").strip() == "kubernetes"


def _is_control_plane_node(node: dict[str, Any]) -> bool:
    """Control-plane Kind tem taint NoSchedule — não é alvo de Jobs do TCC."""
    for key in ("hostname", "node_id"):
        h = str(node.get(key) or "").strip().lower()
        if "control-plane" in h or "controlplane" in h:
            return True
    return False


def filter_kubernetes_nodes(nodes_list: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Mantém apenas nós sincronizados pelo cluster (provider_type=kubernetes).

    Necessário no despacho via Job K8s: node_agent voluntários podem estar
    is_active=1 (heartbeat) sem existir como kubernetes.io/hostname no cluster,
    o que deixa o Pod Pending e a tarefa travada em 'executando'.

    Exclui o control-plane (taint NoSchedule; sem label de camada Edge/Fog/Cloud).
    """
    out: list[dict[str, Any]] = []
    for node in nodes_list or []:
        if not isinstance(node, dict) or not _is_kubernetes_node(node):
            continue
        if _is_control_plane_node(node):
            continue
        out.append(node)
    return out


def _node_missing_live_telemetry(node: dict[str, Any]) -> bool:
    return (
        node.get("cpu_available_percent") is None
        and node.get("memory_available_gb") is None
        and node.get("disk_available_gb") is None
    )


def _fetch_k8s_allocatable_gb(node_id: str) -> Optional[tuple[float, float, float]]:
    """
    Busca allocatable (CPU threads aprox., RAM GB, disco GB) via CoreV1Api.
    Resultado em cache curto para não bloquear o ranking a cada tarefa.
    """
    nid = str(node_id or "").strip()
    if not nid:
        return None
    now = time.time()
    cached = _k8s_allocatable_cache.get(nid)
    if cached and (now - cached[3]) < _K8S_CACHE_TTL_SECONDS:
        return cached[0], cached[1], cached[2]

    try:
        from context_server.services.k8s_job_executor import is_kubernetes_configured

        if not is_kubernetes_configured():
            return None
        from kubernetes import client  # type: ignore[import-untyped]

        from context_server.services.k8s_watcher import (
            _parse_cpu_capacity_to_threads,
            _parse_k8s_quantity_to_gb,
        )

        core = client.CoreV1Api()
        kn = core.read_node(nid, _request_timeout=10)
        alloc = (kn.status and kn.status.allocatable) or {}
        cpu_t = float(_parse_cpu_capacity_to_threads(alloc.get("cpu")))
        mem_gb = _parse_k8s_quantity_to_gb(alloc.get("memory"))
        disk_raw = alloc.get("ephemeral-storage") or alloc.get("storage")
        disk_gb = _parse_k8s_quantity_to_gb(disk_raw) if disk_raw else max(mem_gb, 1.0)
        _k8s_allocatable_cache[nid] = (cpu_t, mem_gb, disk_gb, now)
        return cpu_t, mem_gb, disk_gb
    except Exception:
        return None


def _effective_resources(node: dict[str, Any]) -> tuple[float, float, float]:
    """
    Espelha a lógica do alocador (threads/RAM/disco efetivos).
    Nós Kubernetes sem telemetria: allocatable da API ou redutor de segurança (50%).
    """
    total_threads = max(int(node.get("cpu_total_threads") or 0), 1)
    total_ram = max(_f(node.get("memory_total_gb"), 0.0), 0.25)
    total_disk = max(_f(node.get("disk_total_gb"), 0.0), 0.25)

    pct = node.get("cpu_available_percent")
    ram_m = node.get("memory_available_gb")
    disk_m = node.get("disk_available_gb")

    k8s_no_telemetry = _is_kubernetes_node(node) and _node_missing_live_telemetry(node)
    allocatable: Optional[tuple[float, float, float]] = None
    if k8s_no_telemetry:
        allocatable = _fetch_k8s_allocatable_gb(str(node.get("node_id") or ""))

    if pct is None:
        if allocatable:
            available_threads = min(float(total_threads), allocatable[0] * K8S_NULL_TELEMETRY_FACTOR)
        elif k8s_no_telemetry:
            available_threads = float(total_threads) * K8S_NULL_TELEMETRY_FACTOR
        else:
            available_threads = float(total_threads)
    else:
        available_threads = (_f(pct, 0.0) / 100.0) * float(total_threads)

    if ram_m is None:
        if allocatable:
            available_ram = min(total_ram, allocatable[1] * K8S_NULL_TELEMETRY_FACTOR)
        elif k8s_no_telemetry:
            available_ram = total_ram * K8S_NULL_TELEMETRY_FACTOR
        else:
            available_ram = total_ram
    else:
        available_ram = max(_f(ram_m, 0.0), 0.0)

    if disk_m is None:
        if allocatable:
            available_disk = min(total_disk, allocatable[2] * K8S_NULL_TELEMETRY_FACTOR)
        elif k8s_no_telemetry:
            available_disk = total_disk * K8S_NULL_TELEMETRY_FACTOR
        else:
            available_disk = total_disk
    else:
        available_disk = max(_f(disk_m, 0.0), 0.0)

    reserved_cpu = _f(node.get("cpu_reserved_threads"), 0.0)
    reserved_ram = _f(node.get("memory_reserved_gb"), 0.0)
    reserved_disk = _f(node.get("disk_reserved_gb"), 0.0)

    effective_threads = min(available_threads, max(float(total_threads) - reserved_cpu, 0.0))
    effective_ram = min(available_ram, max(total_ram - reserved_ram, 0.0))
    effective_disk = min(available_disk, max(total_disk - reserved_disk, 0.0))

    return effective_threads, effective_ram, effective_disk


def _min_max_norm(values: list[float], x: float) -> float:
    """Min-max relativo entre candidatos; pool único → pontuação máxima (1.0)."""
    if not values:
        return 1.0
    if len(values) == 1:
        return 1.0
    lo, hi = min(values), max(values)
    if hi <= lo:
        return 1.0
    return max(0.0, min(1.0, (x - lo) / (hi - lo)))


def _node_load_cpu_mem_percent(
    node: dict[str, Any],
    eff_threads: float,
    eff_ram_gb: float,
) -> tuple[float, float]:
    """
    Carga efetiva do nó (0–100): CPU e RAM consumidos em relação à capacidade total.
    Preferência: telemetria direta; senão deriva de recursos efetivos livres.
    """
    cpu_used = node.get("cpu_used_percent")
    if cpu_used is not None:
        l_cpu = max(0.0, min(100.0, _f(cpu_used, 0.0)))
    elif node.get("cpu_available_percent") is not None:
        l_cpu = max(0.0, min(100.0, 100.0 - _f(node.get("cpu_available_percent"), 0.0)))
    else:
        total_t = max(float(node.get("cpu_total_threads") or 1), 1.0)
        l_cpu = max(0.0, min(100.0, (1.0 - eff_threads / total_t) * 100.0))

    mem_used = node.get("memory_used_percent")
    if mem_used is not None:
        l_mem = max(0.0, min(100.0, _f(mem_used, 0.0)))
    else:
        total_ram = max(_f(node.get("memory_total_gb"), 0.25), 0.25)
        l_mem = max(0.0, min(100.0, (1.0 - eff_ram_gb / total_ram) * 100.0))

    return l_cpu, l_mem


def _load_deviation_vector(
    loads_cpu: list[float],
    loads_mem: list[float],
) -> tuple[float, float, list[float], list[float]]:
    """Médias de carga do cluster e desvios por candidato (dvCpu_i, dvMem_i)."""
    n = len(loads_cpu)
    if n == 0:
        return 0.0, 0.0, [], []
    l_cpu_avg = sum(loads_cpu) / n
    l_mem_avg = sum(loads_mem) / n
    dv_cpu = [l_cpu_avg - lc for lc in loads_cpu]
    dv_mem = [l_mem_avg - lm for lm in loads_mem]
    return l_cpu_avg, l_mem_avg, dv_cpu, dv_mem


def _resource_score_norm_from_deviations(
    dv_cpu: float,
    dv_mem: float,
    neg_scale_cpu: float,
    neg_scale_mem: float,
) -> float:
    """
    Normaliza score de recursos (0–1) a partir do vetor de desvio de carga.
    dv >= 0 em CPU e RAM → 1.0; caso contrário, penaliza pela média dos desvios negativos normalizados.
    """
    if dv_cpu >= 0.0 and dv_mem >= 0.0:
        return 1.0
    penalties: list[float] = []
    if dv_cpu < 0.0:
        scale = max(neg_scale_cpu, 1e-6)
        penalties.append(min(1.0, abs(dv_cpu) / scale))
    if dv_mem < 0.0:
        scale = max(neg_scale_mem, 1e-6)
        penalties.append(min(1.0, abs(dv_mem) / scale))
    if not penalties:
        return 1.0
    return max(0.0, 1.0 - sum(penalties) / len(penalties))


def _parse_task_requirements(task_requirements: dict[str, Any]) -> tuple[float, float, float]:
    req_ram = max(
        parse_requirement_to_float(
            task_requirements.get("required_memory_gb"), kind="memory", default=0.25
        ),
        0.25,
    )
    req_cpu = max(
        parse_requirement_to_float(
            task_requirements.get("required_cpu_threads"), kind="cpu", default=1.0
        ),
        0.1,
    )
    req_disk = parse_requirement_to_float(
        task_requirements.get("required_disk_gb"), kind="disk", default=0.0
    )
    if req_disk <= 0:
        req_disk = 0.0
    return req_ram, req_cpu, req_disk


def max_effective_resources_among(
    nodes_list: list[dict[str, Any]],
) -> tuple[float, float, float]:
    """Retorna (max_cpu_threads, max_ram_gb, max_disk_gb) efetivos entre os nós."""
    max_cpu = 0.0
    max_ram = 0.0
    max_disk = 0.0
    for node in nodes_list or []:
        if not isinstance(node, dict):
            continue
        eff_t, eff_r, eff_d = _effective_resources(node)
        max_cpu = max(max_cpu, eff_t)
        max_ram = max(max_ram, eff_r)
        max_disk = max(max_disk, eff_d)
    return max_cpu, max_ram, max_disk


def get_top_nodes_for_task(
    task_requirements: dict[str, Any],
    nodes_list: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Filtra e pontua nós para alocação determinística (vencedor = ranked[0]).

    task_requirements:
        - required_memory_gb (float ou "512Mi"/"2Gi")
        - required_cpu_threads (float ou "500m"/"2")
        - suggested_docker_image (str, opcional)
        - image_warm_counts (Mapping[str, int], opcional)
        - required_disk_gb (float ou quantidade K8s; 0 = sem filtro duro)
        - preferred_layer (edge|fog|cloud, opcional)
        - excluded_hostnames (lista de hostnames/node_id a pular — realocação)

    Retorna até 3 dicts com scores parciais e score_total (0–100).
    """
    req_ram, req_cpu, req_disk = _parse_task_requirements(task_requirements)
    preferred_layer = normalize_layer(task_requirements.get("preferred_layer"))

    excluded_raw = task_requirements.get("excluded_hostnames") or []
    excluded: set[str] = set()
    if isinstance(excluded_raw, (list, tuple, set)):
        for h in excluded_raw:
            s = str(h or "").strip().lower()
            if s:
                excluded.add(s)

    warm: Mapping[str, int] = task_requirements.get("image_warm_counts") or {}
    if not isinstance(warm, Mapping):
        warm = {}

    candidates: list[tuple[dict[str, Any], float, float, float, str]] = []
    for node in nodes_list:
        if not isinstance(node, dict):
            continue
        if excluded:
            keys = (
                str(node.get("hostname") or "").strip().lower(),
                str(node.get("node_id") or "").strip().lower(),
                _k8s_hostname(node).strip().lower() if _k8s_hostname(node) else "",
            )
            if any(k and k in excluded for k in keys):
                continue
        eff_t, eff_r, eff_d = _effective_resources(node)
        if eff_r < req_ram or eff_t < req_cpu:
            continue
        if req_disk > 0 and eff_d < req_disk:
            continue
        layer = node_compute_layer(node)
        candidates.append((node, eff_t, eff_r, eff_d, layer))

    if not candidates:
        return []

    eff_disk_list = [c[3] for c in candidates]

    loads_cpu: list[float] = []
    loads_mem: list[float] = []
    for node, eff_t, eff_r, _eff_d, _layer in candidates:
        lc, lm = _node_load_cpu_mem_percent(node, eff_t, eff_r)
        loads_cpu.append(lc)
        loads_mem.append(lm)

    l_cpu_avg, l_mem_avg, dv_cpus, dv_mems = _load_deviation_vector(loads_cpu, loads_mem)
    neg_scale_cpu = max((abs(d) for d in dv_cpus if d < 0.0), default=1.0)
    neg_scale_mem = max((abs(d) for d in dv_mems if d < 0.0), default=1.0)

    logger.info(
        "NodeRanker: vetor de desvio de carga | lCpu_avg=%.2f lMem_avg=%.2f | candidatos=%d | preferred_layer=%s",
        l_cpu_avg,
        l_mem_avg,
        len(candidates),
        preferred_layer or "(neutro)",
    )

    max_warm = 0
    if warm:
        try:
            max_warm = max(int(v) for v in warm.values())
        except (TypeError, ValueError):
            max_warm = 0

    ranked: list[dict[str, Any]] = []
    for idx, (node, eff_t, eff_r, eff_d, layer) in enumerate(candidates):
        dv_cpu = dv_cpus[idx]
        dv_mem = dv_mems[idx]
        l_cpu = loads_cpu[idx]
        l_mem = loads_mem[idx]
        r_norm = _resource_score_norm_from_deviations(
            dv_cpu, dv_mem, neg_scale_cpu, neg_scale_mem
        )
        score_res = r_norm * WEIGHT_RESOURCES

        nid = str(node.get("node_id") or "")
        host = str(node.get("hostname") or "").strip()
        k8s_host = _k8s_hostname(node)
        score_layer = layer_affinity_score(layer, preferred_layer, WEIGHT_LAYER)
        logger.info(
            "NodeRanker: nó=%s layer=%s lCpu=%.2f lMem=%.2f dvCpu=%.2f dvMem=%.2f "
            "score_recursos=%.2f score_camada=%.2f",
            nid or node.get("hostname"),
            layer,
            l_cpu,
            l_mem,
            dv_cpu,
            dv_mem,
            score_res,
            score_layer,
        )
        c = 0
        if warm:
            for key in (nid, host, k8s_host, f"k8s:{nid}" if nid else "", f"k8s:{k8s_host}" if k8s_host else ""):
                if key and key in warm:
                    c = max(c, int(warm.get(key) or 0))
        if max_warm <= 0:
            score_img = 0.0
        else:
            score_img = (c / float(max_warm)) * WEIGHT_IMAGE

        d_norm = _min_max_norm(eff_disk_list, eff_d)
        score_disk = d_norm * WEIGHT_DISK

        total = score_res + score_img + score_disk + score_layer
        ranked.append(
            {
                "node_id": nid,
                "hostname": (node.get("hostname") or nid or "").strip(),
                "k8s_hostname": _k8s_hostname(node),
                "compute_layer": layer,
                "preferred_layer": preferred_layer,
                "score_total": round(total, 3),
                "score_resources": round(score_res, 3),
                "score_image_affinity": round(score_img, 3),
                "score_disk": round(score_disk, 3),
                "score_layer": round(score_layer, 3),
                "effective_cpu_threads": round(eff_t, 4),
                "effective_memory_gb": round(eff_r, 4),
                "effective_disk_gb": round(eff_d, 4),
                "load_cpu_percent": round(l_cpu, 3),
                "load_mem_percent": round(l_mem, 3),
                "load_deviation_cpu": round(dv_cpu, 3),
                "load_deviation_mem": round(dv_mem, 3),
                "cluster_load_cpu_avg": round(l_cpu_avg, 3),
                "cluster_load_mem_avg": round(l_mem_avg, 3),
                "warm_task_count": c,
            }
        )

    ranked.sort(key=lambda x: (-x["score_total"], -x["effective_memory_gb"], x["node_id"]))
    top3 = ranked[:3]
    if top3:
        _log_sched_inspector_ranking_preview(top3)
        winner = top3[0]
        logger.info(
            "[TCC-METRICS][LAYER] preferred=%s chosen=%s hostname=%s score_layer=%.2f score_total=%.2f",
            preferred_layer or "n/d",
            winner.get("compute_layer"),
            _node_label(winner),
            float(winner.get("score_layer") or 0.0),
            float(winner.get("score_total") or 0.0),
        )
    return top3


def _node_label(entry: dict[str, Any]) -> str:
    return str(entry.get("k8s_hostname") or entry.get("hostname") or entry.get("node_id") or "n/d")


def log_sched_inspector_decision(
    ranked_top3: list[dict[str, Any]],
    final_hostname: str,
    *,
    was_corrected: bool = False,
) -> None:
    """
    Log [TCC-METRICS][SCHEDINSPECTOR] com Top 3 e hostname final após enforcement.
    """
    parts: list[str] = []
    for i, n in enumerate(ranked_top3[:3], start=1):
        if not isinstance(n, dict):
            continue
        label = _node_label(n)
        score = n.get("score_total", "n/d")
        parts.append(f"{i}º {label} (Score: {score})")
    ranking_txt = ", ".join(parts) if parts else "(ranking vazio — fallback)"
    decision = "Corrigida" if was_corrected else "Validada"
    host = (final_hostname or "n/d").strip() or "n/d"
    logger.info(
        "[TCC-METRICS][SCHEDINSPECTOR] Final Top 3 Ranking -> %s. Decisão IA %s para: %s",
        ranking_txt,
        decision,
        host,
    )


def _log_sched_inspector_ranking_preview(top3: list[dict[str, Any]]) -> None:
    """Vetor detalhado do NodeRanker (pré-Strategist) para métricas do TCC."""
    parts = []
    for i, n in enumerate(top3[:3], start=1):
        parts.append(
            f"{i}º {_node_label(n)} [{n.get('compute_layer') or 'n/d'}] (Score: {n.get('score_total')}, "
            f"layer={n.get('score_layer')}, "
            f"dvCpu={n.get('load_deviation_cpu')}, dvMem={n.get('load_deviation_mem')}, "
            f"lCpu={n.get('load_cpu_percent')}%, lMem={n.get('load_mem_percent')}%)"
        )
    logger.info(
        "[TCC-METRICS][SCHEDINSPECTOR] Vetor NodeRanker (Top %d) -> %s",
        len(top3),
        " | ".join(parts),
    )


def format_ranking_rationale_pt(
    top_nodes: list[dict[str, Any]],
    task_requirements: dict[str, Any],
) -> str:
    """Texto curto em português para o prompt do Strategist (pré-seleção determinística)."""
    req_ram, req_cpu, _ = _parse_task_requirements(task_requirements)
    img = str(task_requirements.get("suggested_docker_image") or "").strip() or "(não especificada)"
    pref = normalize_layer(task_requirements.get("preferred_layer")) or "(neutra)"

    if not top_nodes:
        return (
            f"Nenhum nó ativo passou no filtro duro (mínimo ~{req_cpu:.2f} threads efetivas, "
            f"{req_ram:.2f} GiB RAM livre). O Strategist deve usar o fallback configurado."
        )

    lines = [
        "Pré-seleção híbrida (40% desvio de carga / 25% warm image / 15% disco / 20% camada Edge-Fog-Cloud):",
        f"Imagem de referência (Librarian): {img}. Camada preferida: {pref}.",
    ]
    for i, n in enumerate(top_nodes, start=1):
        lines.append(
            f"  {i}) {n.get('k8s_hostname') or n.get('node_id')} [{n.get('compute_layer')}] — "
            f"score_total={n.get('score_total')} "
            f"(recursos={n.get('score_resources')}, imagem={n.get('score_image_affinity')}, "
            f"disco={n.get('score_disk')}, camada={n.get('score_layer')}); "
            f"dvCpu={n.get('load_deviation_cpu')} dvMem={n.get('load_deviation_mem')} "
            f"(lCpu={n.get('load_cpu_percent')}%, lMem={n.get('load_mem_percent')}%); "
            f"livre ~{n.get('effective_cpu_threads')} threads, {n.get('effective_memory_gb')} GiB RAM, "
            f"{n.get('effective_disk_gb')} GiB disco; warm_count={n.get('warm_task_count', 0)}."
        )
    lines.append("Escolha exatamente um destes nós no nodeSelector (kubernetes.io/hostname) e justifique no decision_rationale.")
    return "\n".join(lines)
