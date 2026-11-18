"""
Camadas computacionais Edge / Fog / Cloud para o protótipo do TCC.

Define tipagem de nós, inferência (label K8s, capacidade, hostname) e
preferência de camada a partir do perfil da tarefa (Analista / recursos).
"""

from __future__ import annotations

import json
import os
from typing import Any, Mapping, Optional

VALID_LAYERS = ("edge", "fog", "cloud")

# Label Kubernetes aplicada nos workers do Kind (ver kind-multi-node.yaml)
K8S_LAYER_LABEL = os.getenv(
    "TCC_COMPUTE_LAYER_LABEL",
    "tcc.utfpr.edu.br/compute-layer",
)

# Distância ordinal para afinidade (edge=0, fog=1, cloud=2)
_LAYER_ORDINAL = {"edge": 0, "fog": 1, "cloud": 2}


def normalize_layer(value: Any) -> Optional[str]:
    """Normaliza string para edge|fog|cloud ou None se inválida."""
    if value is None:
        return None
    s = str(value).strip().lower()
    if not s:
        return None
    aliases = {
        "borda": "edge",
        "nevoa": "fog",
        "névoa": "fog",
        "nuvem": "cloud",
    }
    s = aliases.get(s, s)
    return s if s in VALID_LAYERS else None


def layer_affinity_score(node_layer: str, preferred_layer: Optional[str], max_score: float) -> float:
    """
    Pontuação de afinidade de camada (0 .. max_score).

    - Sem preferência: max_score (neutro, não altera o ranking relativo).
    - Match exato: max_score
    - Camada adjacente (edge↔fog, fog↔cloud): max_score / 2
    - Opostas (edge↔cloud): 0
    """
    pref = normalize_layer(preferred_layer)
    node = normalize_layer(node_layer) or "fog"
    if not pref:
        return float(max_score)
    d = abs(_LAYER_ORDINAL[node] - _LAYER_ORDINAL[pref])
    if d == 0:
        return float(max_score)
    if d == 1:
        return float(max_score) / 2.0
    return 0.0


def infer_layer_from_capacity(
    cpu_total_threads: Any = None,
    memory_total_gb: Any = None,
) -> str:
    """Heurística por capacidade (dispositivos voluntários / fallback)."""
    try:
        cpu = float(cpu_total_threads) if cpu_total_threads is not None else 1.0
    except (TypeError, ValueError):
        cpu = 1.0
    try:
        ram = float(memory_total_gb) if memory_total_gb is not None else 1.0
    except (TypeError, ValueError):
        ram = 1.0

    if cpu <= 2.0 and ram <= 2.0:
        return "edge"
    if cpu <= 4.0 and ram <= 8.0:
        return "fog"
    return "cloud"


def infer_layer_from_hostname(hostname: Any) -> Optional[str]:
    h = str(hostname or "").strip().lower()
    if not h:
        return None
    if "edge" in h or "borda" in h:
        return "edge"
    if "fog" in h or "nevoa" in h or "névoa" in h:
        return "fog"
    if "cloud" in h or "nuvem" in h:
        return "cloud"
    if "control-plane" in h or "controlplane" in h:
        return "cloud"
    return None


def infer_layer_from_k8s_labels(labels: Optional[Mapping[str, Any]]) -> Optional[str]:
    if not labels or not isinstance(labels, Mapping):
        return None
    raw = labels.get(K8S_LAYER_LABEL) or labels.get("compute-layer") or labels.get("tcc/compute-layer")
    return normalize_layer(raw)


def resolve_compute_layer(
    *,
    explicit: Any = None,
    k8s_labels: Optional[Mapping[str, Any]] = None,
    hostname: Any = None,
    cpu_total_threads: Any = None,
    memory_total_gb: Any = None,
) -> str:
    """
    Resolve a camada do nó com prioridade:
    1) valor explícito (API / env do agent)
    2) label Kubernetes
    3) hostname
    4) capacidade
    """
    for candidate in (
        normalize_layer(explicit),
        infer_layer_from_k8s_labels(k8s_labels),
        infer_layer_from_hostname(hostname),
    ):
        if candidate:
            return candidate
    return infer_layer_from_capacity(cpu_total_threads, memory_total_gb)


def node_compute_layer(node: Mapping[str, Any]) -> str:
    """Lê compute_layer do dict do nó ou infere."""
    if not isinstance(node, Mapping):
        return "fog"
    stored = normalize_layer(node.get("compute_layer"))
    if stored:
        return stored
    return resolve_compute_layer(
        hostname=node.get("hostname") or node.get("node_id"),
        cpu_total_threads=node.get("cpu_total_threads"),
        memory_total_gb=node.get("memory_total_gb"),
    )


def preferred_layer_for_workload(
    *,
    task_type: Any = None,
    ai_category: Any = None,
    complexity_score: Any = None,
    required_memory_gb: float = 0.5,
    required_cpu_threads: float = 1.0,
) -> str:
    """
    Preferência de camada a partir do perfil da tarefa (proposta TCC):
    cargas leves → edge; intermediárias → fog; intensivas/ML → cloud.
    """
    tt = str(task_type or "").strip()
    cat = str(ai_category or "").strip().lower()
    try:
        complexity = int(complexity_score) if complexity_score is not None else None
    except (TypeError, ValueError):
        complexity = None

    ram = max(float(required_memory_gb or 0.0), 0.0)
    cpu = max(float(required_cpu_threads or 0.0), 0.0)

    if tt == "ML-heavy" or "ml" in cat or "tensorflow" in cat or "keras" in cat:
        return "cloud"
    if complexity is not None and complexity >= 8:
        return "cloud"
    if ram >= 4.0 or cpu >= 4.0:
        return "cloud"

    if complexity is not None and complexity >= 5:
        return "fog"
    if tt == "CPU-bound" or ram >= 1.5 or cpu >= 2.0:
        return "fog"

    return "edge"


def preferred_layer_from_task_row(task_row: Mapping[str, Any]) -> str:
    """Extrai preferência a partir da linha da tarefa (fila / ranking)."""
    if not isinstance(task_row, Mapping):
        return "fog"

    task_type = None
    complexity = None
    raw = task_row.get("ai_raw_response")
    if isinstance(raw, str) and raw.strip():
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                trace = data.get("multi_agent_trace") or {}
                analyst = trace.get("analyst_output") or data.get("analyst_output") or {}
                if isinstance(analyst, dict):
                    task_type = analyst.get("task_type")
                    complexity = analyst.get("complexity_score")
        except (json.JSONDecodeError, TypeError, ValueError):
            pass

    try:
        ram = float(task_row.get("required_memory_gb") or 0.5)
    except (TypeError, ValueError):
        ram = 0.5
    try:
        cpu = float(task_row.get("required_cpu_threads") or 1.0)
    except (TypeError, ValueError):
        cpu = 1.0

    return preferred_layer_for_workload(
        task_type=task_type,
        ai_category=task_row.get("ai_category"),
        complexity_score=complexity,
        required_memory_gb=ram,
        required_cpu_threads=cpu,
    )


def layer_label_pt(layer: Any) -> str:
    m = {"edge": "Edge", "fog": "Fog", "cloud": "Cloud"}
    n = normalize_layer(layer)
    return m.get(n or "", str(layer or "—"))
