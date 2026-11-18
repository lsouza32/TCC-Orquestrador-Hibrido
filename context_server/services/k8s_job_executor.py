"""
Criação de Jobs Kubernetes (Kind) no lugar do Node Agent HTTP.
Script e requirements são injetados via ConfigMap (equivalente ao volume -v do Docker).
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Optional, Tuple

from context_server import config
from context_server.services.k8s_telemetry import (
    prestop_lifecycle_command,
    retention_shell_suffix,
)
from context_server.services.strategist_guard import apply_ranked_winner_node_selector

logger = logging.getLogger(__name__)

_kubernetes_ready = False


def configure_kubernetes() -> bool:
    """
    Carrega kubeconfig (Kind/local) ou in-cluster. Idempotente: se já carregou com sucesso, não repete.
    Define request_timeout global do cliente HTTP para não bloquear threads do scheduler indefinidamente.
    """
    global _kubernetes_ready
    if _kubernetes_ready:
        return True

    try:
        from kubernetes import client as k8s_client  # type: ignore[import-untyped]
        from kubernetes import config as kube_config  # type: ignore[import-untyped]

        if os.getenv("KUBERNETES_SERVICE_HOST"):
            kube_config.load_incluster_config()
        else:
            kube_config.load_kube_config()

        conf = k8s_client.Configuration.get_default_copy()
        try:
            rt_raw = os.getenv("K8S_REQUEST_TIMEOUT", "5,45").strip()
            parts = [p.strip() for p in rt_raw.split(",") if p.strip()]
            if len(parts) >= 2:
                conf.request_timeout = (float(parts[0]), float(parts[1]))
            elif len(parts) == 1:
                t = float(parts[0])
                conf.request_timeout = (t, t)
        except (TypeError, ValueError, AttributeError) as e:
            logger.debug("Kubernetes: request_timeout não aplicado (%s)", e)

        k8s_client.Configuration.set_default(conf)
        _kubernetes_ready = True
        logger.info(
            "Kubernetes: kubeconfig carregado (request_timeout=%s).",
            getattr(conf, "request_timeout", "default"),
        )
        return True
    except Exception as e:
        _kubernetes_ready = False
        logger.warning("Kubernetes: falha ao carregar kubeconfig (%s). Jobs não serão criados.", e)
        return False


def is_kubernetes_configured() -> bool:
    return _kubernetes_ready


def extract_k8s_job_spec(task_row: dict[str, Any]) -> dict[str, Any]:
    """Extrai k8s_job_spec do JSON salvo em ai_raw_response (raiz ou multi_agent_trace)."""
    raw = task_row.get("ai_raw_response") or ""
    if not str(raw).strip():
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    spec = data.get("k8s_job_spec")
    if isinstance(spec, dict):
        return spec
    trace = data.get("multi_agent_trace") or {}
    summ = trace.get("strategist_summary") or {}
    spec2 = summ.get("k8s_job_spec")
    return spec2 if isinstance(spec2, dict) else {}


def _ranked_top3_from_task_row(task_row: dict[str, Any]) -> list[dict[str, Any]]:
    raw = task_row.get("ai_raw_response") or ""
    if not str(raw).strip():
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return []
    trace = data.get("multi_agent_trace") or {}
    nr = trace.get("node_ranker") or {}
    top = nr.get("top_nodes") or []
    return [x for x in top if isinstance(x, dict)]


def _sanitize_dns_subdomain(name: str, max_len: int = 63) -> str:
    s = re.sub(r"[^a-z0-9-]+", "-", name.lower()).strip("-")
    if not s:
        s = "job"
    if len(s) > max_len:
        s = s[:max_len].rstrip("-")
    if not re.match(r"^[a-z0-9]", s):
        s = "j-" + s
    return s[:max_len].rstrip("-")


def names_for_task(task_id: str) -> tuple[str, str]:
    """Nomes determinísticos de Job e ConfigMap a partir do task_id (UUID)."""
    compact = task_id.replace("-", "")[:40]
    job_name = _sanitize_dns_subdomain(f"task-job-{compact}")
    cm_name = _sanitize_dns_subdomain(f"task-cm-{compact}")
    if cm_name == job_name:
        cm_name = _sanitize_dns_subdomain(f"{cm_name}-cfg")
    return job_name, cm_name


def _float_cpu_to_quantity(cores: float) -> str:
    if cores <= 0:
        return "100m"
    if cores < 1.0:
        return f"{max(1, int(cores * 1000))}m"
    if abs(cores - round(cores)) < 1e-6:
        return str(int(round(cores)))
    return f"{int(cores * 1000)}m"


def _float_ram_to_quantity(gb: float) -> str:
    if gb <= 0:
        return "128Mi"
    mib = max(64, int(gb * 1024))
    if mib >= 1024 and mib % 1024 == 0:
        return f"{mib // 1024}Gi"
    return f"{mib}Mi"


def _build_resource_requirements(
    resource_requirements: dict[str, Any],
    k8s_spec: dict[str, Any],
) -> Any:
    from kubernetes import client

    req_cpu = k8s_spec.get("cpu_request")
    req_mem = k8s_spec.get("memory_request")
    lim_cpu = k8s_spec.get("cpu_limit")
    lim_mem = k8s_spec.get("memory_limit")

    if not req_cpu:
        req_cpu = _float_cpu_to_quantity(float(resource_requirements.get("cpu_threads") or 1))
    if not req_mem:
        req_mem = _float_ram_to_quantity(float(resource_requirements.get("ram_gb") or 0.5))
    if not lim_cpu:
        lim_cpu = req_cpu
    if not lim_mem:
        lim_mem = req_mem

    return client.V1ResourceRequirements(
        requests={"cpu": str(req_cpu), "memory": str(req_mem)},
        limits={"cpu": str(lim_cpu), "memory": str(lim_mem)},
    )


def _api_request_timeout() -> tuple[float, float]:
    try:
        raw = os.getenv("K8S_REQUEST_TIMEOUT", "5,45").strip()
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        if len(parts) >= 2:
            return (float(parts[0]), float(parts[1]))
        if len(parts) == 1:
            t = float(parts[0])
            return (t, t)
    except (TypeError, ValueError):
        pass
    return (5.0, 45.0)


def resolve_job_node_selector(
    k8s_spec: dict[str, Any],
    task_data: dict[str, Any],
) -> tuple[dict[str, Any], Optional[dict[str, Any]]]:
    """
    Define nodeSelector do Job: preserva hostname fresco do caller (fila);
    só cai no ranking stale do planejamento se o caller não injetou hostname.
    """
    spec = dict(k8s_spec) if isinstance(k8s_spec, dict) else {}
    existing_ns = spec.get("nodeSelector") if isinstance(spec.get("nodeSelector"), dict) else {}
    existing_host = str(
        existing_ns.get("kubernetes.io/hostname") or existing_ns.get("hostname") or ""
    ).strip()
    if existing_host:
        spec["nodeSelector"] = {"kubernetes.io/hostname": existing_host}
        return spec, None
    ranked_top3 = _ranked_top3_from_task_row(task_data)
    return apply_ranked_winner_node_selector(spec, ranked_top3)


def create_kubernetes_job(
    task_data: dict[str, Any],
    ai_recommendations: dict[str, Any],
) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Cria ConfigMap com script (+ requirements opcional) e um Job que monta o volume e executa.

    Args:
        task_data: linha da tabela tasks.
        ai_recommendations: normalmente extract_k8s_job_spec(task_data) mesclado com resource hints.

    Returns:
        (sucesso, nome_do_job, mensagem_erro, k8s_spec_efetivo)
        k8s_spec_efetivo é o dict usado no Job (nodeSelector + resources finais); None se falhou antes.
    """
    if not _kubernetes_ready:
        return False, None, "Kubernetes não configurado (kubeconfig).", None

    from kubernetes import client  # type: ignore[import-untyped]
    from kubernetes.client import ApiException  # type: ignore[import-untyped]

    task_id = str(task_data.get("task_id") or "")
    file_path = task_data.get("file_path")
    filename = task_data.get("filename") or (os.path.basename(file_path) if file_path else "script.py")
    if not task_id or not file_path or not os.path.isfile(file_path):
        return False, None, f"Arquivo da tarefa inexistente ou task_id ausente: {file_path}", None

    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            script_body = f.read()
    except OSError as e:
        return False, None, str(e), None

    requirements_txt = (task_data.get("generated_requirements_txt") or "").strip()
    k8s_spec: dict[str, Any] = dict(ai_recommendations) if isinstance(ai_recommendations, dict) else {}
    merged = extract_k8s_job_spec(task_data)
    if merged:
        k8s_spec = {**merged, **k8s_spec}

    # Nome sempre derivado do task_id — metadata_name da IA colide em rajadas
    # (ex.: várias tarefas com "task-scripting" → Conflict 409 / Jobs compartilhados).
    job_name, cm_name = names_for_task(task_id)

    # Idempotência: remove Job/CM residual (ex.: realocação / 409 Conflict).
    # Inclui metadata_name da IA (pode colidir entre tarefas distintas).
    deleted = delete_kubernetes_job_for_task_id(task_id)
    try:
        client.BatchV1Api().delete_namespaced_job(
            name=job_name,
            namespace=getattr(config, "K8S_NAMESPACE", os.getenv("K8S_NAMESPACE", "default")),
            propagation_policy="Background",
            _request_timeout=_api_request_timeout(),
        )
        deleted = True
    except ApiException as pre_del:
        if pre_del.status != 404:
            logger.debug("Pré-delete Job %s: %s", job_name, pre_del)
    if deleted:
        # Evita corrida com garbage collection do Job antigo (mesmo metadata_name).
        import time as _time

        _time.sleep(float(os.getenv("K8S_JOB_RECREATE_DELAY_SECONDS", "2")))

    safe_script = re.sub(r"[^a-zA-Z0-9_.-]", "_", os.path.basename(filename)) or "script.py"
    if not safe_script.endswith(".py"):
        safe_script += ".py"

    ns = getattr(config, "K8S_NAMESPACE", os.getenv("K8S_NAMESPACE", "default"))
    image = str(
        k8s_spec.get("image")
        or task_data.get("docker_image")
        or "python:3.9-slim"
    )

    cm_data: dict[str, str] = {safe_script: script_body}
    if requirements_txt:
        cm_data["requirements.txt"] = requirements_txt

    retention = retention_shell_suffix()
    if requirements_txt:
        inner = (
            f"cd /app/scripts && (pip install --no-cache-dir -r requirements.txt "
            f"&& python3 {safe_script}); {retention}"
        )
    else:
        inner = f"cd /app/scripts && python3 {safe_script}; {retention}"

    # Se a fila (ou caller) já injetou hostname fresco, NÃO sobrescrever com ranking stale do planejamento.
    k8s_spec, _corr = resolve_job_node_selector(k8s_spec, task_data)
    if _corr:
        logger.info(
            "Job %s: nodeSelector injetado no executor %s -> %s",
            task_id,
            _corr.get("original_hostname"),
            _corr.get("injected_hostname") or _corr.get("corrected_hostname"),
        )
    else:
        existing_ns = k8s_spec.get("nodeSelector") if isinstance(k8s_spec.get("nodeSelector"), dict) else {}
        existing_host = str(existing_ns.get("kubernetes.io/hostname") or "").strip()
        if existing_host:
            logger.info(
                "Job %s: preservando nodeSelector fresco do caller hostname=%s",
                task_id,
                existing_host,
            )

    node_selector = k8s_spec.get("nodeSelector")
    if not isinstance(node_selector, dict):
        node_selector = None
    else:
        node_selector = {str(k): str(v) for k, v in node_selector.items() if v is not None}

    batch_v1 = client.BatchV1Api()
    core_v1 = client.CoreV1Api()
    _rt = _api_request_timeout()

    cm = client.V1ConfigMap(
        api_version="v1",
        kind="ConfigMap",
        metadata=client.V1ObjectMeta(name=cm_name, namespace=ns, labels={"task-id": task_id}),
        data=cm_data,
    )

    vol = client.V1Volume(
        name="task-scripts",
        config_map=client.V1ConfigMapVolumeSource(name=cm_name),
    )
    mount = client.V1VolumeMount(name="task-scripts", mount_path="/app/scripts")

    rr = {
        "cpu_threads": task_data.get("required_cpu_threads"),
        "ram_gb": task_data.get("required_memory_gb"),
        "disk_gb": task_data.get("required_disk_gb"),
    }
    resources = _build_resource_requirements(rr, k8s_spec)

    lifecycle = client.V1Lifecycle(
        pre_stop=client.V1LifecycleHandler(
            _exec=client.V1ExecAction(command=prestop_lifecycle_command())
        )
    )

    container = client.V1Container(
        name="task-runner",
        image=image,
        image_pull_policy="IfNotPresent",
        command=["/bin/sh", "-c"],
        args=[inner],
        volume_mounts=[mount],
        resources=resources,
        lifecycle=lifecycle,
    )

    pod_spec_kwargs: dict[str, Any] = {
        "restart_policy": "Never",
        "containers": [container],
        "volumes": [vol],
    }
    if node_selector:
        pod_spec_kwargs["node_selector"] = node_selector

    job = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(
            name=job_name,
            namespace=ns,
            labels={"task-id": task_id, "app": "tcc-task-runner"},
            annotations={"tcc.context-server/configmap": cm_name},
        ),
        spec=client.V1JobSpec(
            ttl_seconds_after_finished=int(os.getenv("K8S_JOB_TTL_SECONDS", "86400")),
            backoff_limit=0,
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(labels={"task-id": task_id, "task_id": task_id}),
                spec=client.V1PodSpec(**pod_spec_kwargs),
            ),
        ),
    )

    try:
        core_v1.create_namespaced_config_map(namespace=ns, body=cm, _request_timeout=_rt)
    except ApiException as e:
        if e.status == 409:
            try:
                core_v1.delete_namespaced_config_map(name=cm_name, namespace=ns, _request_timeout=_rt)
                import time as _time

                _time.sleep(1.0)
                core_v1.create_namespaced_config_map(namespace=ns, body=cm, _request_timeout=_rt)
            except ApiException as e2:
                return False, None, f"ConfigMap: {e2.reason} ({e2.status})", None
        else:
            return False, None, f"ConfigMap: {e.reason} ({e.status})", None

    try:
        batch_v1.create_namespaced_job(namespace=ns, body=job, _request_timeout=_rt)
    except ApiException as e:
        if e.status == 409:
            try:
                # metadata_name da IA pode colidir entre tarefas; apaga pelo nome
                # efetivo além do task_id (label do Job residual pode ser outro).
                delete_kubernetes_job_for_task_id(task_id)
                try:
                    batch_v1.delete_namespaced_job(
                        name=job_name,
                        namespace=ns,
                        propagation_policy="Background",
                        _request_timeout=_rt,
                    )
                except ApiException as del_e:
                    if del_e.status != 404:
                        logger.warning(
                            "Falha ao excluir Job residual %s (409): %s", job_name, del_e
                        )
                import time as _time

                _time.sleep(float(os.getenv("K8S_JOB_RECREATE_DELAY_SECONDS", "2")))
                batch_v1.create_namespaced_job(namespace=ns, body=job, _request_timeout=_rt)
            except ApiException as e2:
                try:
                    core_v1.delete_namespaced_config_map(name=cm_name, namespace=ns, _request_timeout=_rt)
                except ApiException:
                    pass
                return False, None, f"Job: {e2.reason} ({e2.status})", None
        else:
            try:
                core_v1.delete_namespaced_config_map(name=cm_name, namespace=ns, _request_timeout=_rt)
            except ApiException:
                pass
            return False, None, f"Job: {e.reason} ({e.status})", None

    logger.info("Kubernetes Job criado | ns=%s job=%s cm=%s image=%s", ns, job_name, cm_name, image)
    return True, job_name, None, dict(k8s_spec)


def delete_kubernetes_job_for_task_id(task_id: str) -> bool:
    """Remove Job e ConfigMap associados ao task_id (nome determinístico + label)."""
    if not _kubernetes_ready or not task_id:
        return False

    from kubernetes import client  # type: ignore[import-untyped]
    from kubernetes.client import ApiException  # type: ignore[import-untyped]

    job_name, cm_name = names_for_task(task_id)
    ns = getattr(config, "K8S_NAMESPACE", os.getenv("K8S_NAMESPACE", "default"))
    _rt = _api_request_timeout()
    batch_v1 = client.BatchV1Api()
    core_v1 = client.CoreV1Api()

    deleted_any = False

    # 1) Nome determinístico padrão
    for name in (job_name,):
        try:
            batch_v1.delete_namespaced_job(
                name=name,
                namespace=ns,
                propagation_policy="Background",
                _request_timeout=_rt,
            )
            deleted_any = True
        except ApiException as e:
            if e.status != 404:
                logger.warning("Falha ao excluir Job %s: %s", name, e)

    try:
        core_v1.delete_namespaced_config_map(name=cm_name, namespace=ns, _request_timeout=_rt)
        deleted_any = True
    except ApiException as e:
        if e.status != 404:
            logger.debug("ConfigMap %s: %s", cm_name, e)

    # 2) Jobs/CMs criados com metadata_name customizado da IA — busca por label task-id / task_id
    label_selectors = (
        f"task-id={task_id}",
        f"task_id={task_id}",
    )
    for sel in label_selectors:
        try:
            jobs = batch_v1.list_namespaced_job(
                namespace=ns, label_selector=sel, _request_timeout=_rt
            )
            for j in getattr(jobs, "items", None) or []:
                name = j.metadata.name
                try:
                    batch_v1.delete_namespaced_job(
                        name=name,
                        namespace=ns,
                        propagation_policy="Background",
                        _request_timeout=_rt,
                    )
                    deleted_any = True
                    logger.info("Kubernetes: Job residual removido por label (%s): %s", sel, name)
                except ApiException as e:
                    if e.status != 404:
                        logger.warning("Falha ao excluir Job residual %s: %s", name, e)
        except ApiException as e:
            logger.debug("list jobs label %s: %s", sel, e)

        try:
            cms = core_v1.list_namespaced_config_map(
                namespace=ns, label_selector=sel, _request_timeout=_rt
            )
            for cm in getattr(cms, "items", None) or []:
                name = cm.metadata.name
                try:
                    core_v1.delete_namespaced_config_map(
                        name=name, namespace=ns, _request_timeout=_rt
                    )
                    deleted_any = True
                except ApiException as e:
                    if e.status != 404:
                        logger.debug("CM residual %s: %s", name, e)
        except ApiException as e:
            logger.debug("list cm label %s: %s", sel, e)

    return deleted_any


def allocation_id_for_kubernetes_task(task_id: str, hostname: Optional[str] = None) -> str:
    """
    Valor gravado em tasks.allocated_node_id para execuções via cluster.
    Prefere hostname do worker (warm start / NodeRanker); fallback k8s:{task_id}.
    """
    host = str(hostname or "").strip()
    if host:
        return f"k8s:{host}"
    return f"k8s:{task_id}"


def kubernetes_allocation_hostname(allocated_node_id: Optional[str]) -> Optional[str]:
    """Extrai hostname de allocated_node_id no formato k8s:{hostname}."""
    s = str(allocated_node_id or "").strip()
    if not s.startswith("k8s:"):
        return None
    rest = s[4:].strip()
    return rest or None


def is_kubernetes_allocation(allocated_node_id: Optional[str]) -> bool:
    return bool(allocated_node_id and str(allocated_node_id).startswith("k8s:"))
