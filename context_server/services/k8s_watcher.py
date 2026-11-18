"""
Sincroniza nós do cluster Kubernetes com o SQLite e finaliza tarefas a partir do estado dos Pods.
Executado periodicamente pelo APScheduler (run.py).
"""

from __future__ import annotations

import datetime
import json
import time
from typing import Any, Optional

from context_server import config
from context_server.database import crud
from context_server.logging_config import get_logger, log_error_with_context
from context_server.realtime import sse_hub
from context_server.services.k8s_job_executor import (
    _api_request_timeout,
    delete_kubernetes_job_for_task_id,
    is_kubernetes_allocation,
    is_kubernetes_configured,
)
from context_server.services.k8s_telemetry import (
    collect_pod_container_peaks,
    fetch_cluster_node_usage,
    merge_peak_samples,
)
from context_server.services.node_ranker import filter_kubernetes_nodes
from context_server.services.reallocation import (
    active_k8s_hostnames,
    build_reallocation_ai_raw,
    can_reallocate,
    failed_hostname_from_task,
    get_reallocation_state,
    max_reallocation_attempts,
)
from context_server.services.task_export import export_task_result
from context_server.services.tcc_metrics import (
    compute_prediction_delta_ram,
    log_execucao_fim,
    log_reallocation,
)

logger = get_logger(__name__)

# Picos acumulados entre ciclos do watcher (task_id → amostras)
_peak_samples: dict[str, dict[str, Optional[float]]] = {}


def _req_timeout() -> tuple[float, float]:
    return _api_request_timeout()


def _parse_cpu_capacity_to_threads(value: Optional[str]) -> int:
    if not value:
        return 1
    t = str(value).strip()
    try:
        if t.endswith("m"):
            return max(1, int(int(t[:-1]) / 1000) or 1)
        return max(1, int(float(t)))
    except (TypeError, ValueError):
        return 1


def _parse_k8s_quantity_to_gb(value: Optional[str]) -> float:
    """Converte quantidade Kubernetes (Mi, Gi, Ki, …) para GB (base 1024 para binários)."""
    if not value:
        return 1.0
    t = str(value).strip()
    try:
        if t.endswith("Ki"):
            return float(t[:-2]) / (1024.0**2)
        if t.endswith("Mi"):
            return float(t[:-2]) / 1024.0
        if t.endswith("Gi"):
            return float(t[:-2])
        if t.endswith("Ti"):
            return float(t[:-2]) * 1024.0
        if t.endswith("K"):
            return float(t[:-1]) * 1000.0 / (1024.0**3)
        if t.endswith("M"):
            return float(t[:-1]) * 1000.0**2 / (1024.0**3)
        if t.endswith("G"):
            return float(t[:-1]) * 1000.0**3 / (1024.0**3)
        v = float(t)
        return v / (1024.0**3) if v > 1_000_000 else max(v, 0.25)
    except (TypeError, ValueError):
        return 1.0


def _node_internal_ip(kn: Any) -> Optional[str]:
    for addr in kn.status.addresses or []:
        if addr.type == "InternalIP" and addr.address:
            return addr.address
    for addr in kn.status.addresses or []:
        if addr.type == "ExternalIP" and addr.address:
            return addr.address
    return None


def _pod_duration_seconds(pod: Any) -> float:
    st = pod.status.start_time
    if not st:
        return 0.0
    try:
        now = datetime.datetime.now(datetime.timezone.utc)
        if st.tzinfo is None:
            st = st.replace(tzinfo=datetime.timezone.utc)
        return max(0.0, (now - st).total_seconds())
    except Exception:
        return 0.0


def _sync_cluster_nodes(db_path: str, core_v1: Any) -> int:
    """
    Sincroniza nós do cluster no SQLite. Retorna quantidade de nós processados com sucesso.
    Falhas por nó não abortam os demais.
    Também persiste uso vivo via metrics.k8s.io (cpu/mem) para o NodeRanker.
    """
    now = time.time()
    ok = 0
    try:
        node_list = core_v1.list_node(_request_timeout=_req_timeout())
    except Exception as e:
        logger.warning("K8s watcher: list_node falhou (%s): %s", type(e).__name__, e)
        return 0

    items = getattr(node_list, "items", None) or []
    if items is None:
        items = []
    logger.info("K8s watcher: list_node retornou %d item(ns).", len(items))

    if not items:
        logger.warning(
            "K8s watcher: lista de nós vazia (sem permissão list nodes, kubeconfig errado ou API inacessível?)."
        )

    for kn in items:
        name = kn.metadata.name
        try:
            cap = (kn.status and kn.status.capacity) or {}
            cpu_threads = _parse_cpu_capacity_to_threads(cap.get("cpu"))
            memory_total_gb = round(_parse_k8s_quantity_to_gb(cap.get("memory")), 3)
            disk_raw = cap.get("ephemeral-storage") or cap.get("storage")
            disk_total_gb = round(
                _parse_k8s_quantity_to_gb(disk_raw) if disk_raw else max(memory_total_gb, 1.0), 3
            )
            ip = _node_internal_ip(kn) or "0.0.0.0"
            labels = (kn.metadata and kn.metadata.labels) or {}
            from context_server.services.compute_layer import resolve_compute_layer

            compute_layer = resolve_compute_layer(
                k8s_labels=labels if isinstance(labels, dict) else {},
                hostname=name,
                cpu_total_threads=cpu_threads,
                memory_total_gb=memory_total_gb,
            )
            crud.register_or_update_node_static_info(
                db_path,
                name,
                ip,
                0,
                name,
                cpu_threads,
                memory_total_gb,
                disk_total_gb,
                now,
                True,
                f"Kubernetes (API / capacity) [{compute_layer}]",
                provider_type="kubernetes",
                compute_layer=compute_layer,
            )
            ok += 1
        except Exception as e:
            log_error_with_context(logger, e, f"K8s watcher: persistir nó '{name}' no SQLite")

    # Telemetria viva (Metrics Server) → cpu_used_percent / memory_* no SQLite
    try:
        usage_by_node = fetch_cluster_node_usage()
        if usage_by_node:
            logger.info(
                "K8s watcher: metrics.k8s.io/nodes → %d nó(s) com uso vivo.",
                len(usage_by_node),
            )
        else:
            logger.warning("K8s watcher: metrics.k8s.io/nodes vazio — NodeRanker sem carga viva.")
        for node_id, usage in usage_by_node.items():
            try:
                cpu_used = usage.get("cpu_used_percent")
                cpu_avail = usage.get("cpu_available_percent")
                mem_used = usage.get("memory_used_percent")
                mem_avail = usage.get("memory_available_gb")
                # Disco: metrics.k8s.io não expõe; preserva total como disponível aprox.
                row = None
                try:
                    row = crud.get_node_details(db_path, node_id)
                except Exception:
                    row = None
                disk_avail = None
                if isinstance(row, dict):
                    disk_avail = row.get("disk_available_gb")
                    if disk_avail is None:
                        disk_avail = row.get("disk_total_gb")
                crud.update_node_resources(
                    db_path,
                    node_id,
                    cpu_available_percent=cpu_avail,
                    memory_available_gb=mem_avail,
                    disk_available_gb=disk_avail if disk_avail is not None else 1.0,
                    last_resource_check=now,
                    cpu_used_percent=cpu_used,
                    memory_used_percent=mem_used,
                )
            except Exception as e:
                log_error_with_context(logger, e, f"K8s watcher: update recursos vivos '{node_id}'")
    except Exception as e:
        log_error_with_context(logger, e, "K8s watcher: sync metrics.k8s.io/nodes")

    return ok


def _read_pod_logs(core_v1: Any, namespace: str, pod_name: str) -> tuple[str, str]:
    try:
        text = core_v1.read_namespaced_pod_log(
            name=pod_name,
            namespace=namespace,
            container="task-runner",
            tail_lines=8000,
            _request_timeout=_req_timeout(),
        )
        return (text or ""), ""
    except Exception as e:
        logger.debug("Log do pod %s/%s: %s", namespace, pod_name, e)
        return "", str(e)[:4000]


def _cpu_limit_millicores_from_task(task_row: dict[str, Any]) -> Optional[float]:
    """
    Preferência: cpu_limit (ou cpu_request) do k8s_job_spec efetivo em ai_raw_response.
    Fallback: required_cpu_threads da tarefa.
    """
    try:
        from context_server.services.k8s_job_executor import extract_k8s_job_spec
        from context_server.services.k8s_quantity import parse_cpu_quantity_to_cores

        spec = extract_k8s_job_spec(task_row)
        if isinstance(spec, dict):
            cores = parse_cpu_quantity_to_cores(spec.get("cpu_limit") or spec.get("cpu_request"))
            if cores is not None and cores > 0:
                return max(100.0, float(cores) * 1000.0)
    except Exception:
        pass
    try:
        cores = float(task_row.get("required_cpu_threads") or 1.0)
        return max(100.0, cores * 1000.0)
    except (TypeError, ValueError):
        return 1000.0


def _detected_libraries_for_task(task_row: dict[str, Any]) -> str:
    raw = task_row.get("ai_detected_libraries")
    if raw is None:
        return ""
    if isinstance(raw, (list, tuple)):
        return json.dumps(list(raw), ensure_ascii=False)
    return str(raw).strip()


def _pod_age_seconds(pod: Any) -> float:
    ts = getattr(getattr(pod, "metadata", None), "creation_timestamp", None)
    if ts is None:
        return 0.0
    try:
        if getattr(ts, "tzinfo", None) is None:
            ts = ts.replace(tzinfo=datetime.timezone.utc)
        return max(0.0, (datetime.datetime.now(datetime.timezone.utc) - ts).total_seconds())
    except Exception:
        return 0.0


def _pod_is_unschedulable(pod: Any, *, min_age_seconds: float = 60.0) -> bool:
    """
    True se o scheduler reportou PodScheduled=False há tempo suficiente
    (evita falso positivo no primeiro ciclo; cobre hostname inexistente no nodeSelector).
    """
    if _pod_age_seconds(pod) < min_age_seconds:
        return False
    status = getattr(pod, "status", None)
    conditions = getattr(status, "conditions", None) or []
    for cond in conditions:
        if getattr(cond, "type", None) != "PodScheduled":
            continue
        if getattr(cond, "status", None) == "False":
            reason = str(getattr(cond, "reason", "") or "")
            if reason == "Unschedulable" or "match" in reason.lower():
                return True
            msg = str(getattr(cond, "message", "") or "").lower()
            if "unschedulable" in msg or "didn't match" in msg or "no nodes" in msg:
                return True
    return False


def _sample_pod_peaks(
    task_id: str,
    namespace: str,
    pod_name: str,
    cpu_limit_millicores: Optional[float],
) -> None:
    cpu_p, ram_mb = collect_pod_container_peaks(
        namespace, pod_name, cpu_limit_millicores=cpu_limit_millicores
    )
    if cpu_p is None and ram_mb is None:
        return
    prev = _peak_samples.get(task_id) or {}
    _peak_samples[task_id] = merge_peak_samples(prev, cpu_p, ram_mb)


def _try_reallocate_k8s_task(
    db_path: str,
    task_id: str,
    task_row: dict[str, Any],
    pod: Any,
    *,
    phase: str,
    reason: str,
) -> bool:
    """
    Se ainda houver tentativas e nós alternativos, devolve a tarefa à fila.
    Remove o Job atual e não grava task_results (execução ainda não é final).
    """
    failed_host = failed_hostname_from_task(task_row, pod=pod)
    available: list[str] = []
    try:
        active = crud.get_active_nodes(db_path)
        k8s_nodes = filter_kubernetes_nodes(active)
        available = active_k8s_hostnames(k8s_nodes)
    except Exception as e:
        logger.warning("K8s watcher: não foi possível listar nós para realocação: %s", e)

    ok, why = can_reallocate(
        task_row,
        failed_hostname=failed_host,
        available_hostnames=available if available else None,
    )
    if not ok:
        logger.info(
            "K8s watcher: realocação negada task_id=%s motivo=%s host_falhou=%s",
            task_id,
            why,
            failed_host or "(desconhecido)",
        )
        return False

    new_raw = build_reallocation_ai_raw(
        task_row,
        failed_hostname=failed_host,
        reason=reason,
        phase=phase,
    )
    state_after = get_reallocation_state({"ai_raw_response": new_raw})

    try:
        crud.enqueue_task_for_reallocation(db_path, task_id, new_raw)
    except Exception as e:
        log_error_with_context(logger, e, f"Reenfileirar tarefa {task_id}")
        return False

    _peak_samples.pop(task_id, None)
    delete_kubernetes_job_for_task_id(task_id)

    log_reallocation(
        task_id,
        failed_hostname=failed_host,
        attempt=int(state_after.get("attempts") or 0),
        max_attempts=max_reallocation_attempts(),
        reason=reason,
        excluded_hostnames=list(state_after.get("excluded_hostnames") or []),
    )
    logger.warning(
        "K8s watcher: tarefa %s realocada (tentativa %s/%s) — excluiu nó %s; status=na_fila",
        task_id,
        state_after.get("attempts"),
        max_reallocation_attempts(),
        failed_host or "(desconhecido)",
    )

    user_id = task_row.get("user_id")
    if user_id:
        _publish_task_sse(
            user_id,
            task_id,
            "na_fila",
            0.0,
            reallocation=True,
            attempt=state_after.get("attempts"),
            failed_hostname=failed_host,
        )
    return True


def _finalize_k8s_task(
    db_path: str,
    core_v1: Any,
    namespace: str,
    pod: Any,
    task_id: str,
    task_row: dict[str, Any],
    phase: str,
    *,
    fail_reason: Optional[str] = None,
) -> None:
    """
    Persiste resultado + telemetria e remove o Job (sinal de morte do Broker).
    Em fase Failed/Unschedulable, tenta realocação antes de marcar falha definitiva.

    ``pod`` pode ser None quando o Job falhou sem Pod restante (ex.: Pod apagado
    com backoffLimit=0) — nesse caso só tenta realocar ou grava falha sem logs.
    """
    if phase != "Succeeded":
        reason = fail_reason or ("Unschedulable" if phase == "Failed" else str(phase))
        if _try_reallocate_k8s_task(
            db_path, task_id, task_row, pod, phase=phase, reason=reason
        ):
            return

    stdout_log = ""
    stderr_log = ""
    duration_seconds = 0.0
    peaks: dict[str, Optional[float]] = {}

    if pod is not None:
        pod_name = pod.metadata.name
        cpu_lim = _cpu_limit_millicores_from_task(task_row)
        _sample_pod_peaks(task_id, namespace, pod_name, cpu_lim)
        peaks = _peak_samples.pop(task_id, {})
        stdout_log, stderr_extra = _read_pod_logs(core_v1, namespace, pod_name)
        if phase == "Failed" and stderr_extra and not stdout_log:
            stderr_log = stderr_extra
            stdout_log = ""
        else:
            stderr_log = stderr_extra if stderr_extra else ""
        duration_seconds = _pod_duration_seconds(pod)
    else:
        peaks = _peak_samples.pop(task_id, {})
        if phase != "Succeeded":
            stderr_log = fail_reason or phase or "Job Failed (sem Pod)"

    status_final = "concluida" if phase == "Succeeded" else "falha"
    det_libs = _detected_libraries_for_task(task_row)

    suggested_ram = task_row.get("required_memory_gb")
    delta_ram = compute_prediction_delta_ram(suggested_ram, peaks.get("ram_peak_mb"))
    log_execucao_fim(
        task_id,
        status_final,
        duration_seconds,
        cpu_peak_percent=peaks.get("cpu_peak_percent"),
        ram_peak_mb=peaks.get("ram_peak_mb"),
        detected_libraries=det_libs or None,
        suggested_ram_gb=suggested_ram,
        prediction_delta_ram=delta_ram,
    )

    crud.save_task_result(
        db_path,
        task_id,
        status_final,
        duration_seconds,
        stdout_log or "",
        stderr_log or "",
        cpu_peak_percent=peaks.get("cpu_peak_percent"),
        ram_peak_mb=peaks.get("ram_peak_mb"),
        detected_libraries=det_libs or None,
        prediction_delta_ram=delta_ram,
    )
    crud.update_task_status(db_path, task_id, status_final)

    user_id = task_row.get("user_id")
    if user_id:
        _publish_task_sse(user_id, task_id, status_final, duration_seconds)

    try:
        result_row = crud.get_task_result(db_path, task_id)
        exports_base = getattr(config, "EXPORTS_BASE", None)
        if result_row and status_final not in ("cancelada",) and exports_base:
            export_task_result(exports_base, task_id, task_row, result_row)
    except Exception as ex:
        log_error_with_context(logger, ex, f"Exportar resultado K8s {task_id}")

    logger.info(
        "K8s watcher: telemetria task_id=%s cpu_peak=%s ram_peak_mb=%s — removendo Job (sinal Broker)",
        task_id,
        peaks.get("cpu_peak_percent"),
        peaks.get("ram_peak_mb"),
    )
    delete_kubernetes_job_for_task_id(task_id)


def _publish_task_sse(
    user_id: str,
    task_id: str,
    status_final: str,
    duration_seconds: float,
    **extra: Any,
) -> None:
    payload = {
        "type": "task_update",
        "task_id": task_id,
        "status": status_final,
        "duration_seconds": duration_seconds,
    }
    payload.update(extra)
    sse_hub.publish(user_id, payload)


def _task_id_from_job_labels(labels: Optional[dict]) -> Optional[str]:
    if not labels:
        return None
    tid = labels.get("task_id") or labels.get("task-id")
    return str(tid).strip() if tid else None


def _job_is_terminal_failed(job: Any) -> bool:
    """Job sem Pod ativo e com falha (backoffLimit esgotado / Pod removido)."""
    st = getattr(job, "status", None)
    if st is None:
        return False
    active = int(getattr(st, "active", None) or 0)
    succeeded = int(getattr(st, "succeeded", None) or 0)
    failed = int(getattr(st, "failed", None) or 0)
    return active == 0 and succeeded == 0 and failed > 0


def _sync_failed_jobs_without_pods(db_path: str, batch_v1: Any, core_v1: Any, namespace: str) -> int:
    """
    Quando o Pod some (apagado / GC) com backoffLimit=0, o Job fica Failed e o
    watcher de Pods não vê nada — a tarefa ficaria eternamente em 'executando'.
    Detecta esses Jobs e dispara realocação / falha definitiva.
    """
    handled = 0
    jobs: list[Any] = []
    for selector in ("task_id", "task-id", "app=tcc-task-runner"):
        try:
            listed = batch_v1.list_namespaced_job(
                namespace=namespace,
                label_selector=selector,
                _request_timeout=_req_timeout(),
            )
            for j in list(getattr(listed, "items", None) or []):
                jobs.append(j)
        except Exception as e:
            logger.debug("K8s watcher: list Job selector=%s falhou: %s", selector, e)

    seen: set[str] = set()
    for job in jobs:
        labels = (job.metadata.labels or {}) if job.metadata else {}
        task_id = _task_id_from_job_labels(labels)
        if not task_id or task_id in seen:
            continue
        seen.add(task_id)
        if not _job_is_terminal_failed(job):
            continue

        try:
            if crud.get_task_result(db_path, task_id):
                continue
            task_row = crud.get_task_by_id(db_path, task_id)
            if not task_row or task_row.get("status") != "executando":
                continue
            if not is_kubernetes_allocation(task_row.get("allocated_node_id")):
                continue

            # Ainda há Pod vivo com o label? então o ciclo de Pods cuida.
            try:
                pods = core_v1.list_namespaced_pod(
                    namespace=namespace,
                    label_selector=f"task_id={task_id}",
                    _request_timeout=_req_timeout(),
                )
                if list(getattr(pods, "items", None) or []):
                    continue
            except Exception:
                pass

            logger.warning(
                "K8s watcher: Job Failed sem Pod task_id=%s job=%s — realocação/falha",
                task_id,
                getattr(job.metadata, "name", "?"),
            )
            _finalize_k8s_task(
                db_path,
                core_v1,
                namespace,
                None,
                task_id,
                task_row,
                "Failed",
                fail_reason="JobFailedNoPod",
            )
            handled += 1
        except Exception as inner:
            log_error_with_context(logger, inner, f"K8s watcher (Job órfão task_id={task_id})")

    return handled


def sync_k8s_state(db_path: str) -> None:
    """
    Sincroniza nós do cluster no SQLite e processa Pods de tarefas (label task_id) em fase terminal.
    Erros de API não interrompem o broker.
    """
    t0 = time.perf_counter()
    logger.info("K8s watcher: sync_k8s_state iniciando (db_path=%s).", db_path)

    if not is_kubernetes_configured():
        logger.info(
            "K8s watcher: sync_k8s_state concluído em %.3fs — Kubernetes não configurado, ignorando.",
            time.perf_counter() - t0,
        )
        return

    try:
        from kubernetes import client  # type: ignore[import-untyped]
        from kubernetes.client import ApiException  # type: ignore[import-untyped]
    except ImportError as e:
        logger.warning("Kubernetes SDK indisponível: %s", e)
        logger.info("K8s watcher: sync_k8s_state fim em %.3fs (sem SDK).", time.perf_counter() - t0)
        return

    ns = getattr(config, "K8S_NAMESPACE", "default")
    nodes_written = 0

    try:
        core_v1 = client.CoreV1Api()
        nodes_written = _sync_cluster_nodes(db_path, core_v1)
        logger.info("K8s watcher: sincronização de nós gravou %d registro(s).", nodes_written)
    except ApiException as e:
        logger.warning("K8s watcher (nós): ApiException %s — %s — body=%s", e.status, e.reason, (e.body or "")[:500])
    except Exception as e:
        log_error_with_context(logger, e, "K8s watcher (sincronização de nós)")

    try:
        core_v1 = client.CoreV1Api()
        pods = []
        try:
            pod_list = core_v1.list_namespaced_pod(
                namespace=ns,
                label_selector="task_id",
                _request_timeout=_req_timeout(),
            )
            pods = list(getattr(pod_list, "items", None) or [])
            logger.info("K8s watcher: list_namespaced_pod(label=task_id) → %d pod(s).", len(pods))
        except ApiException as e:
            logger.warning(
                "K8s watcher: list_namespaced_pod com label_selector falhou (%s %s); listando namespace inteiro.",
                e.status,
                e.reason,
            )
            pod_list = core_v1.list_namespaced_pod(namespace=ns, _request_timeout=_req_timeout())
            all_items = getattr(pod_list, "items", None) or []
            pods = [p for p in all_items if (p.metadata.labels or {}).get("task_id")]
            logger.info("K8s watcher: fallback listou %d pod(s) com label task_id.", len(pods))

        for pod in pods:
            labels = pod.metadata.labels or {}
            task_id = labels.get("task_id")
            if not task_id:
                continue
            phase = pod.status.phase or ""

            try:
                if crud.get_task_result(db_path, task_id):
                    _peak_samples.pop(task_id, None)
                    continue
                task_row = crud.get_task_by_id(db_path, task_id)
                if not task_row:
                    continue
                if task_row.get("status") != "executando":
                    continue
                if not is_kubernetes_allocation(task_row.get("allocated_node_id")):
                    continue

                cpu_lim = _cpu_limit_millicores_from_task(task_row)
                pod_name = pod.metadata.name

                if phase == "Running":
                    _sample_pod_peaks(task_id, ns, pod_name, cpu_lim)
                    continue

                # Pod sem nó elegível (ex.: nodeSelector com hostname de node_agent):
                # falha a tarefa em vez de deixar eternamente em 'executando'.
                if phase == "Pending" and _pod_is_unschedulable(pod):
                    logger.warning(
                        "K8s watcher: pod Pending/Unschedulable task_id=%s name=%s — "
                        "tentando realocação ou falha",
                        task_id,
                        pod_name,
                    )
                    _finalize_k8s_task(
                        db_path,
                        core_v1,
                        ns,
                        pod,
                        task_id,
                        task_row,
                        "Failed",
                        fail_reason="Unschedulable",
                    )
                    continue

                if phase not in ("Succeeded", "Failed"):
                    continue

                _sample_pod_peaks(task_id, ns, pod_name, cpu_lim)
                _finalize_k8s_task(
                    db_path,
                    core_v1,
                    ns,
                    pod,
                    task_id,
                    task_row,
                    phase,
                    fail_reason="Failed" if phase == "Failed" else None,
                )

            except Exception as inner:
                log_error_with_context(logger, inner, f"K8s watcher (pod task_id={task_id})")

        # Jobs Failed sem Pod (ex.: Pod delete forçado com backoffLimit=0)
        try:
            batch_v1 = client.BatchV1Api()
            n_orphan = _sync_failed_jobs_without_pods(db_path, batch_v1, core_v1, ns)
            if n_orphan:
                logger.info("K8s watcher: %d Job(s) Failed sem Pod tratados.", n_orphan)
        except Exception as e:
            log_error_with_context(logger, e, "K8s watcher (Jobs órfãos)")

    except ApiException as e:
        logger.warning("K8s watcher (pods): ApiException %s — %s", e.status, e.reason)
    except Exception as e:
        log_error_with_context(logger, e, "K8s watcher (pods)")

    elapsed = time.perf_counter() - t0
    logger.info("K8s watcher: sync_k8s_state concluído em %.3fs (nós gravados neste ciclo: %s).", elapsed, nodes_written)
