"""
Serviço de análise de código via Ollama (LFM2.5 ou outro modelo).
Orquestra três agentes (Analista de código, Librarian infra/telemetria, Strategist Kubernetes),
gera requirements.txt, estimativa de CPU/RAM para o alocador legado e um k8s_job_spec para futuro V1Job.
"""

import json
import os
import re
import ast
import sys
from typing import Any, Optional, Tuple

import requests

from context_server import config
from context_server.database import crud as db_crud
from context_server.logging_config import get_logger
from context_server.services.k8s_quantity import (
    parse_cpu_quantity_to_cores,
    parse_memory_quantity_to_gb,
)
from context_server.services.node_ranker import (
    filter_kubernetes_nodes,
    format_ranking_rationale_pt,
    get_top_nodes_for_task,
    log_sched_inspector_decision,
)
from context_server.services.strategist_guard import apply_ranked_winner_node_selector
from context_server.services.tcc_metrics import log_planejamento

logger = get_logger(__name__)


def sched_inspector_enforcement(
    k8s_spec: dict[str, Any],
    ranked_top3: list[dict[str, Any]],
) -> tuple[dict[str, Any], Optional[dict[str, Any]]]:
    """
    Injetor determinístico do nodeSelector (vencedor do NodeRanker).

    O Strategist (LLM) não escolhe hostname: este componente força ``ranked[0]``
    (ou fallback se o ranking estiver vazio), mitigando alucinações de afinidade.

    Returns:
        Tupla ``(k8s_spec_com_nodeSelector, meta_de_injecao)``.
    """
    return apply_ranked_winner_node_selector(k8s_spec, ranked_top3)


# Margem de segurança RL (near_ceiling): +25% uma vez por planejamento, teto 2.0x do piso base.
SAFETY_MARGIN_FACTOR = 1.25
MAX_SAFETY_FACTOR = 2.0
NEAR_CEILING_FRACTION_TRIGGER = 0.30

# URL do Ollama: em Docker use o nome do serviço; local use localhost
OLLAMA_BASE_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "LiquidAI/lfm2.5-1.2b-instruct")

DYNAMIC_PIP_ALIASES = {
    "cv2": "opencv-python-headless",
    "opencv-python": "opencv-python-headless",
    "sklearn": "scikit-learn",
    "pil": "Pillow",
    "bs4": "beautifulsoup4",
    "yaml": "PyYAML",
}


def _stdlib_names_lower() -> set[str]:
    """
    Módulos da biblioteca padrão (nomes em minúsculas). O pip não instala estes pacotes;
    incluir 'sys', 'os', etc. em requirements.txt quebra o pip install.
    """
    # O LLM/AST às vezes emite '__future' (sem dunders finais); o pip rejeita os dois.
    extras = {"__future__", "__future", "__main__", "__pypy__"}
    raw = getattr(sys, "stdlib_module_names", None)
    if raw:
        return {str(n).lower() for n in raw} | extras
    # Python < 3.10: conjunto mínimo (evita depender só de lista incompleta)
    return extras | {
        "sys", "os", "re", "json", "math", "time", "datetime", "pathlib", "collections",
        "itertools", "functools", "operator", "typing", "abc", "io", "copy", "enum",
        "random", "string", "hashlib", "subprocess", "threading", "multiprocessing",
        "queue", "sqlite3", "csv", "socket", "ssl", "struct", "logging", "warnings",
        "traceback", "ast", "inspect", "importlib", "pkgutil", "glob", "tempfile",
        "shutil", "stat", "getpass", "platform", "errno", "select", "asyncio", "contextlib",
        "dataclasses", "decimal", "fractions", "statistics", "secrets", "bisect", "heapq",
        "array", "weakref", "types", "builtins", "email", "http", "urllib", "xml",
    }


def _call_ollama(prompt: str, system: Optional[str] = None, json_mode: bool = True, temperature: float = 0.0) -> Optional[str]:
    """
    Chama a API do Ollama e retorna o texto da resposta (não streaming).
    """
    url = f"{OLLAMA_BASE_URL}/api/generate"
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": float(temperature),
        },
    }
    if system:
        payload["system"] = system
    if json_mode:
        payload["format"] = "json"
    try:
        r = requests.post(url, json=payload, timeout=120)
        r.raise_for_status()
        data = r.json()
        return (data.get("response") or "").strip()
    except requests.exceptions.RequestException as e:
        logger.warning("Ollama indisponível ou erro: %s", e)
        return None
    except Exception as e:
        logger.warning("Erro ao chamar Ollama: %s", e)
        return None


SYSTEM_ANALYST = """Você é o agente Analista (Code Profiler). Sua missão é analisar o script Python em profundidade
e cruzar com as bibliotecas já listadas por AST (são pistas confiáveis, não invente imports inexistentes no código).

Não use dados de banco de dados, telemetria, imagens de runtime ou Kubernetes — apenas o código e o contexto de imports.

REGRAS DE COMPLEXIDADE:
- Scripts extremamente simples, como Hello World, prints básicos, declarações de variáveis ou imports simples da stdlib (sys, os, math, time) DEVEM ter complexity_score baixo (entre 1 e 3) e task_type "CPU-bound" ou "IO-bound". NUNCA classifique scripts simples como "ML-heavy" ou com complexidade alta.
- Só use "ML-heavy" se houver import explícito de bibliotecas de Machine Learning (como tensorflow, torch, keras, sklearn, pytorch).

Responda somente com JSON válido (sem markdown), com exatamente estas chaves:
- "detected_libraries": array de strings — bibliotecas relevantes (alinhe com o AST quando fizer sentido; inclua libs inferíveis pelo uso no código).
- "pip_packages": array de strings — nomes exatos dos pacotes no PyPI prontos para containers headless Linux (ex: "opencv-python-headless" para "cv2", "scikit-learn" para "sklearn", "Pillow" para "PIL").
- "task_type": string — exatamente um dentre: "CPU-bound", "IO-bound", "ML-heavy".
- "complexity_score": inteiro de 1 a 10 — carga computacional e intrínseca do script (loops, volume de dados, treino/ inferência, I/O massivo).
- "rationale": string curta em português justificando task_type e complexity_score.

Exemplo de saída para script trivial (ex: print("Hello")):
{
  "detected_libraries": [],
  "pip_packages": [],
  "task_type": "CPU-bound",
  "complexity_score": 1,
  "rationale": "O script realiza apenas uma operação trivial de saída de texto no console, sem loops ou processamento de dados."
}"""


SYSTEM_LIBRARIAN = """Você é o agente Librarian (Infra & Telemetry / aprendizado por reforço histórico).
Você recebe um único JSON "fatos" vindos do SQLite via a aplicação: candidatos de catálogo,
telemetria agregada (cpu_peak_percent_*, ram_peak_mb_*), image_success_stats (success_rate por docker_image)
e ceiling_signals (near_ceiling quando picos históricos chegaram perto dos limites alocados).

REGRAS ABSOLUTAS:
- Não invente image_name, percentuais, task_id ou picos que não apareçam nos fatos.
- "best_image" DEVE ser exatamente um image_name presente em fatos.allowed_image_names.
- SE O SCRIPT NÃO TIVER BIBLIOTECAS ESPECÍFICAS (array de libs do Analista está vazio), escolha uma imagem leve como "python:3.11-slim" e defina o piso_hardware para o mínimo da imagem (ex: cpu_cores_floor: 1.0, ram_gb_floor: 0.5). NUNCA escolha imagens pesadas de ML (como tcc-tf-ml) para scripts sem dependências.
- "piso_hardware" traduz o histórico real de picos em pisos numéricos (cpu_cores_floor, ram_gb_floor). Use cpu_peak_percent_p95/avg e ram_peak_mb_p95/avg quando existirem; se samples==0, use mínimos do catálogo para a imagem selecionada.

Responda somente com JSON válido, com exatamente estas chaves:
- "best_image": string — nome da imagem de container escolhida.
- "piso_hardware": objeto com chaves numéricas "cpu_cores_floor" (float) e "ram_gb_floor" (float, GB).
- "telemetry_basis": string em português.
- "catalog_alignment": string curta.

Exemplo de resposta para script sem bibliotecas (ex: print("Hello")):
{
  "best_image": "python:3.11-slim",
  "piso_hardware": {
    "cpu_cores_floor": 1.0,
    "ram_gb_floor": 0.5
  },
  "telemetry_basis": "Sem telemetria histórica pois não há bibliotecas importadas. Escolhido o piso mínimo padrão do catálogo.",
  "catalog_alignment": "Alinhado com a imagem padrão python:3.11-slim para automações gerais."
}"""


SYSTEM_STRATEGIST = """Você é o agente Strategist (K8s Resource Packager). Consolide o JSON do Analista
(perfil de código), o JSON do Librarian (best_image + piso_hardware + telemetria factual) e o histórico
textual adicional (se houver). Você NÃO escolhe nós do cluster.

REGRAS DE FORMATO E CONTEÚDO (EXTREMAMENTE IMPORTANTES PARA MODELOS PEQUENOS):
- O campo "requirements_txt" DEVE conter APENAS nomes de pacotes Pip válidos adicionais necessários ao script (um por linha).
- Se o script usa apenas a biblioteca padrão (como sys, os, time) ou não necessita de nenhuma biblioteca externa adicional, o campo "requirements_txt" DEVE ser uma string vazia "".
- É PROIBIDO escrever qualquer explicação textual, conversação, observações (ex: "Ops: Python 3.11...") ou textos longos dentro de "requirements_txt". Qualquer texto que não seja um pacote válido quebrará a instalação.
- Recursos K8s (cpu_request/cpu_limit/memory_request/memory_limit): deves respeitar o piso_hardware do Librarian. Para scripts de complexidade baixa (complexity_score 1 a 3), use recursos mínimos baixos (ex: cpu_request: "200m", cpu_limit: "500m", memory_request: "256Mi", memory_limit: "512Mi").
- O campo image em k8s_job_spec DEVE ser igual a librarian.best_image e existir em allowed_image_names.
- NÃO preencha nodeSelector com hostname de nó. Deixe-o como objeto vazio {}.

Responda somente com JSON válido, com exatamente estas chaves:
- "requirements_txt": string (pacotes estilo pip, um por linha, ou "" se vazio).
- "detected_libraries": array de strings (pode espelhar o Analista).
- "category": string curta para o pipeline legado (escolha exatamente um: "ml_treinamento", "etl_dados", "automacao", "scripting_geral").
- "suggested_disk_gb": número (0.5 a 50).
- "confidence_score": float entre 0 e 1.
- "decision_rationale": string em português.
- "k8s_job_spec": objeto:
  - "metadata_name": string (RFC1123 subdomain, ex: "task-001").
  - "image": string (igual à imagem de catálogo escolhida).
  - "nodeSelector": objeto vazio {}.
  - "cpu_request", "cpu_limit", "memory_request", "memory_limit": strings (formato de recursos K8s).

Exemplo 1 (Script trivial - ex: print("Hello")):
{
  "requirements_txt": "",
  "detected_libraries": [],
  "category": "scripting_geral",
  "suggested_disk_gb": 0.5,
  "confidence_score": 0.95,
  "decision_rationale": "Script simples sem dependências externas adicionais. Alocado com hardware mínimo padrão de segurança.",
  "k8s_job_spec": {
    "metadata_name": "task-trivial",
    "image": "python:3.9-slim",
    "nodeSelector": {},
    "cpu_request": "200m",
    "cpu_limit": "500m",
    "memory_request": "256Mi",
    "memory_limit": "512Mi"
  }
}

Exemplo 2 (Script de manipulação de dados - pandas e numpy):
{
  "requirements_txt": "pandas\nnumpy",
  "detected_libraries": ["pandas", "numpy"],
  "category": "etl_dados",
  "suggested_disk_gb": 1.0,
  "confidence_score": 0.90,
  "decision_rationale": "Script de ETL simples que requer as bibliotecas pandas e numpy, associado a imagem python com dependências.",
  "k8s_job_spec": {
    "metadata_name": "task-etl",
    "image": "python:3.9-slim",
    "nodeSelector": {},
    "cpu_request": "500m",
    "cpu_limit": "1",
    "memory_request": "512Mi",
    "memory_limit": "1Gi"
  }
}"""


def _strip_model_thinking(text: str) -> str:
    if not text:
        return ""
    t = text
    open_t = chr(60) + "think" + chr(62)
    close_t = chr(60) + "/" + "think" + chr(62)
    t = re.sub(open_t + r"[\s\S]*?" + close_t, "", t, flags=re.IGNORECASE)
    open_th = chr(60) + "think" + "ing" + chr(62)
    close_th = chr(60) + "/" + "think" + "ing" + chr(62)
    t = re.sub(open_th + r"[\s\S]*?" + close_th, "", t, flags=re.IGNORECASE)
    return t.strip()


def _extract_json_string_from_response(raw: str) -> str:
    raw = _strip_model_thinking(raw or "")
    if not raw:
        return ""
    if "```json" in raw:
        m = re.search(r"```json\s*([\s\S]*?)\s*```", raw)
        if m:
            return m.group(1).strip()
    if "```" in raw:
        m = re.search(r"```\s*([\s\S]*?)\s*```", raw)
        if m:
            return m.group(1).strip()
    return raw.strip()


def _parse_ollama_json(raw: Optional[str]) -> Optional[dict[str, Any]]:
    if not raw:
        return None
    json_str = _extract_json_string_from_response(raw)
    if not json_str:
        return None
    try:
        out = json.loads(json_str)
        return out if isinstance(out, dict) else None
    except json.JSONDecodeError:
        return None


def _parse_cpu_quantity_to_cores(value: Any) -> Optional[float]:
    return parse_cpu_quantity_to_cores(value)


def _parse_memory_quantity_to_gb(value: Any) -> Optional[float]:
    return parse_memory_quantity_to_gb(value)


def _telemetry_derived_floors(telemetry: dict[str, Any]) -> tuple[float, float]:
    """Pisos determinísticos a partir de p95 de telemetria (cores aproximados, GB)."""
    cpu_floor = 1.0
    ram_floor = 0.5
    if telemetry:
        if telemetry.get("cpu_peak_percent_p95") is not None:
            cpu_floor = max(cpu_floor, float(telemetry["cpu_peak_percent_p95"]) / 100.0)
        if telemetry.get("ram_peak_mb_p95") is not None:
            ram_floor = max(ram_floor, float(telemetry["ram_peak_mb_p95"]) / 1024.0)
    return cpu_floor, ram_floor


def apply_history_safety_margin(
    cpu: float,
    ram: float,
    ceiling_signals: Optional[dict[str, Any]],
) -> tuple[float, float, dict[str, Any]]:
    """
    Aplica margem 1.25x quando o histórico indica near_ceiling, capada em 2.0x do piso base.
    Uma única aplicação por planejamento (anti-deadlock / anti-crescimento composto).
    """
    base_cpu = max(float(cpu), 0.1)
    base_ram = max(float(ram), 0.25)
    meta: dict[str, Any] = {
        "applied": False,
        "factor": 1.0,
        "base_cpu": base_cpu,
        "base_ram": base_ram,
        "reason": "sem sinal de teto",
    }
    signals = ceiling_signals if isinstance(ceiling_signals, dict) else {}
    samples = int(signals.get("samples") or 0)
    near = bool(signals.get("near_ceiling"))
    near_ratio = float(signals.get("near_ceiling_ratio") or 0.0)
    near_count = int(signals.get("near_ceiling_count") or 0)

    should_boost = False
    reason = meta["reason"]
    if samples > 0 and near:
        if samples < 5 and near_count >= 1:
            should_boost = True
            reason = "near_ceiling em amostra pequena (>=1)"
        elif near_ratio >= NEAR_CEILING_FRACTION_TRIGGER:
            should_boost = True
            reason = f"near_ceiling_ratio={near_ratio:.2f} >= {NEAR_CEILING_FRACTION_TRIGGER}"
        elif near_count >= 1:
            should_boost = True
            reason = "near_ceiling detectado no histórico"

    if not should_boost:
        meta["reason"] = reason
        return base_cpu, base_ram, meta

    boosted_cpu = min(base_cpu * SAFETY_MARGIN_FACTOR, base_cpu * MAX_SAFETY_FACTOR)
    boosted_ram = min(base_ram * SAFETY_MARGIN_FACTOR, base_ram * MAX_SAFETY_FACTOR)
    # Cap absoluto vs base (garante <= 2.0x mesmo se fatores mudarem)
    boosted_cpu = min(boosted_cpu, base_cpu * MAX_SAFETY_FACTOR)
    boosted_ram = min(boosted_ram, base_ram * MAX_SAFETY_FACTOR)
    meta.update({
        "applied": True,
        "factor": SAFETY_MARGIN_FACTOR,
        "max_factor": MAX_SAFETY_FACTOR,
        "reason": reason,
        "cpu_after": round(boosted_cpu, 4),
        "ram_after": round(boosted_ram, 4),
    })
    return boosted_cpu, boosted_ram, meta


def _prefer_image_by_success_rate(
    allowed: set[str],
    candidates: list[dict[str, Any]],
    success_stats: list[dict[str, Any]],
    current: str,
) -> str:
    """Escolhe imagem permitida priorizando match_score de bibliotecas do catálogo; desempate por success_rate."""
    if not allowed:
        return current or "python:3.11-slim"
    rate_by_image = {
        str(s.get("docker_image")): float(s.get("success_rate") or 0.0)
        for s in (success_stats or [])
        if str(s.get("docker_image") or "") in allowed
    }
    match_by_image = {
        str(c.get("image_name")): float(c.get("match_score") or 0.0)
        for c in (candidates or [])
        if str(c.get("image_name") or "") in allowed
    }
    if current in allowed and current not in rate_by_image and not rate_by_image and current in match_by_image:
        return current
    # Prioridade primária: match_score de bibliotecas; secundária: success_rate
    ranked = sorted(
        list(allowed),
        key=lambda name: (
            match_by_image.get(name, 0.0),
            rate_by_image.get(name, 0.0),
            1.0 if name == current else 0.0,
        ),
        reverse=True,
    )
    return ranked[0] if ranked else (current or "python:3.11-slim")


def _normalize_librarian_agent_output(
    librarian: dict[str, Any],
    librarian_facts: dict[str, Any],
    best_catalog_image: Optional[dict[str, Any]],
    telemetry_summary: dict[str, Any],
) -> dict[str, Any]:
    """Garante best_image válida no catálogo e piso_hardware coerente quando o modelo omite campos."""
    out = dict(librarian)
    allowed = {str(x) for x in (librarian_facts.get("allowed_image_names") or []) if x}
    candidates = librarian_facts.get("catalog_candidates_for_script_libraries") or []
    success_stats = librarian_facts.get("image_success_stats") or []
    ceiling_signals = librarian_facts.get("ceiling_signals") or {}
    best_img = str(out.get("best_image") or "").strip()

    # Se houver candidatos com match de bibliotecas > 0, dar preferência absoluta ao candidato top
    if candidates and float(candidates[0].get("match_score") or 0.0) > 0:
        top_cand_name = str(candidates[0].get("image_name") or "")
        if top_cand_name in allowed:
            best_img = top_cand_name

    if best_img not in allowed:
        for row in candidates:
            name = str(row.get("image_name") or "")
            if name in allowed:
                best_img = name
                break
        if best_img not in allowed and best_catalog_image:
            cand = str(best_catalog_image.get("image_name") or "")
            if cand in allowed:
                best_img = cand
        if best_img not in allowed and allowed:
            best_img = sorted(allowed)[0]
    best_img = _prefer_image_by_success_rate(allowed, candidates, success_stats, best_img)
    out["best_image"] = best_img or "python:3.11-slim"

    piso = out.get("piso_hardware")
    if not isinstance(piso, dict):
        piso = {}
    cpu_f, ram_f = _telemetry_derived_floors(telemetry_summary)
    try:
        cpu_floor = float(piso.get("cpu_cores_floor", cpu_f))
    except (TypeError, ValueError):
        cpu_floor = cpu_f
    try:
        ram_floor = float(piso.get("ram_gb_floor", ram_f))
    except (TypeError, ValueError):
        ram_floor = ram_f

    # Enforça pisos do candidato de catálogo correspondente
    for row in candidates:
        if str(row.get("image_name")) == out["best_image"]:
            cpu_floor = max(cpu_floor, float(row.get("min_cpu") or 1.0))
            ram_floor = max(ram_floor, float(row.get("min_ram_gb") or 0.5))
            break
    if best_catalog_image and str(best_catalog_image.get("image_name")) == out["best_image"]:
        cpu_floor = max(cpu_floor, float(best_catalog_image.get("min_cpu") or 1.0))
        ram_floor = max(ram_floor, float(best_catalog_image.get("min_ram_gb") or 0.5))

    cpu_floor, ram_floor, margin_meta = apply_history_safety_margin(
        cpu_floor, ram_floor, ceiling_signals if isinstance(ceiling_signals, dict) else {}
    )
    out["piso_hardware"] = {"cpu_cores_floor": round(cpu_floor, 3), "ram_gb_floor": round(ram_floor, 3)}
    out["safety_margin"] = margin_meta
    try:
        disk_s = float(out.get("suggested_disk_gb", 0.5))
    except (TypeError, ValueError):
        disk_s = 0.5
    out["suggested_disk_gb"] = round(max(0.5, min(disk_s, 50.0)), 3)
    if not str(out.get("telemetry_basis") or "").strip():
        out["telemetry_basis"] = (
            "Piso sintetizado a partir de telemetria agregada (p95), success_rate e sinais near_ceiling; "
            "o modelo não preencheu telemetry_basis."
        )
    if margin_meta.get("applied"):
        out["telemetry_basis"] = (
            f"{out['telemetry_basis']} | Margem RL aplicada ({margin_meta.get('reason')})."
        )
    if not str(out.get("catalog_alignment") or "").strip():
        out["catalog_alignment"] = f"Imagem alinhada ao catálogo/histórico: {out['best_image']}."
    return out


def _suggested_from_k8s_job_spec(
    spec: dict[str, Any],
    cpu_floor: float,
    ram_floor: float,
    catalog_min_cpu: float,
    catalog_min_ram: float,
) -> tuple[float, float]:
    """Deriva suggested_cpu_threads e suggested_ram_gb a partir de requests/limits do Job."""
    cr = _parse_cpu_quantity_to_cores(spec.get("cpu_request"))
    cl = _parse_cpu_quantity_to_cores(spec.get("cpu_limit"))
    mr = _parse_memory_quantity_to_gb(spec.get("memory_request"))
    ml = _parse_memory_quantity_to_gb(spec.get("memory_limit"))
    cpu_candidates = [x for x in (cr, cl, cpu_floor, catalog_min_cpu) if x is not None]
    ram_candidates = [x for x in (mr, ml, ram_floor, catalog_min_ram) if x is not None]
    cpu_cores = max(cpu_candidates) if cpu_candidates else max(cpu_floor, catalog_min_cpu)
    ram_gb = max(ram_candidates) if ram_candidates else max(ram_floor, catalog_min_ram)
    return max(1.0, float(cpu_cores)), max(0.25, float(ram_gb))


def _min_cpu_ram_for_catalog_image(
    image_name: str,
    librarian_facts: dict[str, Any],
    best_catalog_image: Optional[dict[str, Any]],
) -> tuple[float, float, float]:
    for row in librarian_facts.get("catalog_candidates_for_script_libraries") or []:
        if str(row.get("image_name")) == image_name:
            return float(row.get("min_cpu") or 1.0), float(row.get("min_ram_gb") or 0.5), float(row.get("min_disk_gb") or 1.0)
    if best_catalog_image and str(best_catalog_image.get("image_name")) == image_name:
        return float(best_catalog_image.get("min_cpu") or 1.0), float(best_catalog_image.get("min_ram_gb") or 0.5), float(best_catalog_image.get("min_disk_gb") or 1.0)
    for row in db_crud.get_docker_images_catalog(config.DATABASE_PATH):
        if str(row.get("image_name")) == image_name:
            return float(row.get("min_cpu") or 1.0), float(row.get("min_ram_gb") or 0.5), float(row.get("min_disk_gb") or 1.0)
    return 1.0, 0.5, 1.0


def _build_librarian_facts(script_libraries: list[str]) -> dict[str, Any]:
    """Fatos: catálogo, telemetria, success_rate por imagem e sinais near_ceiling."""
    db_path = config.DATABASE_PATH
    catalog = db_crud.get_docker_images_catalog(db_path)
    allowed_names = [str(r.get("image_name")) for r in catalog if r.get("image_name")]
    candidates = db_crud.find_catalog_candidates_for_libraries(db_path, script_libraries, limit=5)
    telemetry = db_crud.get_telemetry_summary_for_libraries(db_path, script_libraries, limit=120)
    success_stats: list[dict[str, Any]] = []
    ceiling_signals: dict[str, Any] = {}
    samples: list[dict[str, Any]] = []
    try:
        success_stats = db_crud.get_docker_image_success_stats_for_libraries(
            db_path, script_libraries, limit=200
        )
    except Exception as e:
        logger.warning("success_stats por imagem/libs indisponível: %s", e)
    try:
        ceiling_signals = db_crud.get_resource_ceiling_signals_for_libraries(
            db_path, script_libraries, limit=120
        )
    except Exception as e:
        logger.warning("ceiling_signals indisponível: %s", e)
        ceiling_signals = {}
    try:
        samples = db_crud.get_task_result_samples_for_libraries(db_path, script_libraries, limit=25)
    except Exception as e:
        logger.warning("amostras task_results por libs indisponíveis: %s", e)

    # Reordena candidatos: success_rate (desc) e depois match_score.
    rate_map = {
        str(s.get("docker_image")): float(s.get("success_rate") or 0.0)
        for s in success_stats
        if s.get("docker_image")
    }
    if candidates:
        candidates = sorted(
            candidates,
            key=lambda c: (
                rate_map.get(str(c.get("image_name")), -1.0),
                float(c.get("match_score") or 0.0),
            ),
            reverse=True,
        )
        for c in candidates:
            img = str(c.get("image_name") or "")
            if img in rate_map:
                c["success_rate"] = rate_map[img]

    return {
        "allowed_image_names": allowed_names,
        "catalog_candidates_for_script_libraries": candidates,
        "telemetry_summary": telemetry,
        "image_success_stats": success_stats,
        "ceiling_signals": ceiling_signals,
        "history_samples": samples,
    }


def _run_analyst_agent(script_content: str, libraries_from_ast: list[str]) -> Optional[dict[str, Any]]:
    ast_str = ", ".join(libraries_from_ast) if libraries_from_ast else "(nenhuma)"
    prompt = f"""Bibliotecas detectadas por AST: {ast_str}

Código a analisar:
```
{(script_content or "")[:15000]}
```
"""
    raw = _call_ollama(prompt, system=SYSTEM_ANALYST, json_mode=True, temperature=0.0)
    return _parse_ollama_json(raw)


def _run_librarian_agent(librarian_facts: dict[str, Any]) -> Optional[dict[str, Any]]:
    facts_json = json.dumps(librarian_facts, ensure_ascii=False)
    prompt = f"""Abaixo estão os fatos (JSON) obtidos exclusivamente do banco de dados. Interprete apenas estes dados.

FATOS:
{facts_json}
"""
    raw = _call_ollama(prompt, system=SYSTEM_LIBRARIAN, json_mode=True, temperature=0.0)
    return _parse_ollama_json(raw)


def _format_placement_block(
    ranked_nodes_top3: list[dict[str, Any]],
    ranking_rationale_pt: str,
) -> str:
    """Legado: placement não é mais enviado ao Strategist (seleção é determinística)."""
    return (
        "Seleção de nó é determinística (NodeRanker). "
        "Não escolha hostname; o sistema injeta o vencedor após o ranking.\n"
        f"{ranking_rationale_pt}"
    )


def _run_strategist_agent(
    analyst: dict[str, Any],
    librarian: dict[str, Any],
    history_str: str,
    libraries_from_ast: list[str],
    best_image_name: str,
    best_image_min_cpu: float,
    best_image_min_ram: float,
    allowed_image_names: list[str],
) -> Optional[dict[str, Any]]:
    ast_str = ", ".join(libraries_from_ast) if libraries_from_ast else "(nenhuma)"
    analyst_json = json.dumps(analyst, ensure_ascii=False)
    librarian_json = json.dumps(librarian, ensure_ascii=False)
    allowed_preview = allowed_image_names[:80]
    if len(allowed_image_names) > 80:
        allowed_note = f"(lista truncada; total {len(allowed_image_names)} imagens no catálogo)"
    else:
        allowed_note = ""
    piso = librarian.get("piso_hardware") or {}
    lib_best = str(librarian.get("best_image") or best_image_name)
    prompt = f"""Entradas para empacotamento de recursos do Job Kubernetes (SEM escolha de nó):

Bibliotecas AST: {ast_str}

JSON do Analista (Code Profiler):
{analyst_json}

JSON do Librarian (best_image + piso_hardware + telemetria + margem RL):
{librarian_json}

{history_str or "Sem histórico textual adicional."}

IMPORTANTE: Não escolha kubernetes.io/hostname. A alocação de nó é feita pelo NodeRanker
e injetada deterministicamente pelo sistema. Omita nodeSelector ou use {{}}.

Referência de catálogo para a imagem base librarian.best_image (ou fallback): "{lib_best}"
- min_cpu obrigatório (referência): {best_image_min_cpu}
- min_ram_gb obrigatório (referência): {best_image_min_ram}
- piso_hardware sugerido pelo Librarian (não violar ao dimensionar requests): cpu_cores_floor={piso.get('cpu_cores_floor', 'n/d')}, ram_gb_floor={piso.get('ram_gb_floor', 'n/d')}

allowed_image_names (imagem do container deve estar nesta lista) {allowed_note}:
{json.dumps(allowed_preview, ensure_ascii=False)}

Produza o JSON completo conforme seu system prompt (requirements_txt, k8s_job_spec com recursos e image; sem decisão de nó).
"""
    raw = _call_ollama(prompt, system=SYSTEM_STRATEGIST, json_mode=True, temperature=0.0)
    return _parse_ollama_json(raw)


def _format_agent_reasoning(
    analyst: dict[str, Any],
    librarian: dict[str, Any],
    strategist: dict[str, Any],
    node_ranker_trace: Optional[dict[str, Any]] = None,
) -> str:
    tt = (analyst.get("task_type") or "n/d").strip()
    cs = analyst.get("complexity_score")
    ar = (analyst.get("rationale") or "").strip()
    lines = [
        f"[Analista] task_type={tt} | complexity_score={cs} | {ar or 'Sem rationale.'}",
        f"[Librarian] best_image={librarian.get('best_image', '')} | piso_hardware={librarian.get('piso_hardware', {})}",
        f"[Librarian] telemetry_basis: {librarian.get('telemetry_basis', '')}",
        f"[Librarian] catalog_alignment: {librarian.get('catalog_alignment', '')}",
    ]
    if node_ranker_trace:
        top = node_ranker_trace.get("top_nodes") or []
        h = ", ".join(str(x.get("k8s_hostname") or x.get("node_id")) for x in top if isinstance(x, dict))
        lines.append(f"[NodeRanker] top3={h or '(vazio — fallback worker|worker2)'} | {node_ranker_trace.get('ranking_rationale_pt', '')[:500]}")
    dr = (strategist.get("decision_rationale") or strategist.get("strategist_rationale") or "").strip()
    k8s = strategist.get("k8s_job_spec") if isinstance(strategist.get("k8s_job_spec"), dict) else {}
    lines.append(f"[Strategist] {dr or 'Decisão final conforme k8s_job_spec e regras de negócio.'}")
    if k8s:
        lines.append(f"[Strategist/K8s] nodeSelector={k8s.get('nodeSelector')} | cpu req/limit={k8s.get('cpu_request')}/{k8s.get('cpu_limit')} | mem req/limit={k8s.get('memory_request')}/{k8s.get('memory_limit')}")
    return "\n".join(lines)


def _orchestrate_three_agents(
    script_content: str,
    libraries_from_ast: list[str],
    history_str: str,
    best_catalog_image: Optional[dict[str, Any]],
    librarian_facts: dict[str, Any],
) -> Optional[Tuple[dict[str, Any], dict[str, Any]]]:
    analyst = _run_analyst_agent(script_content, libraries_from_ast)
    if not analyst:
        logger.warning("Agente Analista não retornou JSON válido.")
        return None
    librarian = _run_librarian_agent(librarian_facts)
    if not librarian:
        logger.warning("Agente Librarian não retornou JSON válido.")
        return None
    telemetry_sum = librarian_facts.get("telemetry_summary") or {}
    if not isinstance(telemetry_sum, dict):
        telemetry_sum = {}
    librarian = _normalize_librarian_agent_output(
        librarian, librarian_facts, best_catalog_image, telemetry_sum
    )

    lib_image = str(librarian.get("best_image") or (best_catalog_image or {}).get("image_name") or "python:3.9-slim")
    best_image_min_cpu, best_image_min_ram, best_image_min_disk = _min_cpu_ram_for_catalog_image(
        lib_image, librarian_facts, best_catalog_image
    )

    piso_hw = librarian.get("piso_hardware") or {}
    if not isinstance(piso_hw, dict):
        piso_hw = {}
    try:
        cpu_floor = float(piso_hw.get("cpu_cores_floor", 1.0))
    except (TypeError, ValueError):
        cpu_floor = 1.0
    try:
        ram_floor = float(piso_hw.get("ram_gb_floor", 0.5))
    except (TypeError, ValueError):
        ram_floor = 0.5

    warm_counts: dict[str, int] = {}
    active_nodes: list[dict[str, Any]] = []
    try:
        warm_counts = db_crud.get_completed_task_counts_by_node_for_image(config.DATABASE_PATH, lib_image)
    except Exception as e:
        logger.warning("Warm start por nó/imagem indisponível: %s", e)
    try:
        active_nodes = db_crud.get_active_nodes(config.DATABASE_PATH)
    except Exception as e:
        logger.warning("Lista de nós ativos indisponível para ranking: %s", e)
        active_nodes = []

    # Planejamento do Job K8s: só workers do cluster (mesmo critério da fila).
    k8s_nodes = filter_kubernetes_nodes(active_nodes)

    try:
        disk_floor = float(librarian.get("suggested_disk_gb", 0.5))
    except (TypeError, ValueError):
        disk_floor = 0.5
    _, _, disk_from_libs, _ = _apply_library_guardrails(
        cpu_floor, ram_floor, disk_floor, lib_image, libraries_from_ast
    )
    disk_floor = max(0.5, min(float(disk_from_libs), 50.0))

    from context_server.services.compute_layer import preferred_layer_for_workload

    task_req: dict[str, Any] = {
        "required_memory_gb": max(ram_floor, best_image_min_ram, 0.25),
        "required_cpu_threads": max(cpu_floor, best_image_min_cpu, 1.0),
        "suggested_docker_image": lib_image,
        "image_warm_counts": warm_counts,
        "required_disk_gb": disk_floor,
        "preferred_layer": preferred_layer_for_workload(
            task_type=analyst.get("task_type") if isinstance(analyst, dict) else None,
            complexity_score=analyst.get("complexity_score") if isinstance(analyst, dict) else None,
            required_memory_gb=max(ram_floor, best_image_min_ram, 0.25),
            required_cpu_threads=max(cpu_floor, best_image_min_cpu, 1.0),
        ),
    }
    # NodeRanker (determinístico): vencedor injetado no nodeSelector (não passa pelo Strategist).
    ranked_top3 = get_top_nodes_for_task(task_req, k8s_nodes)
    ranking_rationale_pt = format_ranking_rationale_pt(ranked_top3, task_req)
    node_ranker_trace = {
        "task_requirements": task_req,
        "top_nodes": ranked_top3,
        "ranking_rationale_pt": ranking_rationale_pt,
        "active_nodes_considered": len(k8s_nodes),
        "active_nodes_total": len(active_nodes),
        "note": "Alocação definitiva ocorre na fila (re-ranking em tempo real; só nós kubernetes).",
    }

    allowed = librarian_facts.get("allowed_image_names") or []
    if not isinstance(allowed, list):
        allowed = []
    strategist = _run_strategist_agent(
        analyst,
        librarian,
        history_str,
        libraries_from_ast,
        lib_image,
        best_image_min_cpu,
        best_image_min_ram,
        [str(x) for x in allowed if x],
    )
    if not strategist:
        logger.warning("Agente Strategist não retornou JSON válido.")
        return None

    # Injeção determinística do vencedor (Strategist não escolhe nó).
    k8s_raw = strategist.get("k8s_job_spec")
    if isinstance(k8s_raw, dict):
        enforced_spec, selector_correction = sched_inspector_enforcement(k8s_raw, ranked_top3)
        strategist = dict(strategist)
        strategist["k8s_job_spec"] = enforced_spec
        if selector_correction:
            node_ranker_trace["node_selector_injection"] = selector_correction
            logger.info(
                "NodeSelector injetado %s -> %s (%s)",
                selector_correction.get("original_hostname"),
                selector_correction.get("injected_hostname") or selector_correction.get("corrected_hostname"),
                selector_correction.get("reason"),
            )

    trace = {
        "analyst_output": analyst,
        "librarian_output": librarian,
        "node_ranker": node_ranker_trace,
        "strategist_output": strategist,
    }
    return strategist, trace


def _fallback_requirements(
    libraries: Optional[list[str]] = None,
    catalog_candidate: Optional[dict[str, Any]] = None,
    telemetry: Optional[dict[str, Any]] = None,
    ceiling_signals: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Requisitos padrão quando a IA não está disponível."""
    libs = libraries or []
    base_cpu = float((catalog_candidate or {}).get("min_cpu") or 2.0)
    base_ram = float((catalog_candidate or {}).get("min_ram_gb") or 0.5)
    base_disk = 1.0
    if telemetry and telemetry.get("ram_peak_mb_p95") is not None:
        # Converte MB para GB + margem de 20%
        base_ram = max(base_ram, (float(telemetry["ram_peak_mb_p95"]) / 1024.0) * 1.2)
    if telemetry and telemetry.get("cpu_peak_percent_p95") is not None:
        cpu_from_peak = max(1.0, float(telemetry["cpu_peak_percent_p95"]) / 100.0)
        base_cpu = max(base_cpu, cpu_from_peak)
    base_cpu, base_ram, margin_meta = apply_history_safety_margin(
        base_cpu, base_ram, ceiling_signals
    )
    image = str((catalog_candidate or {}).get("image_name") or "python:3.9-slim")
    cpu, ram, disk, image = _apply_library_guardrails(base_cpu, base_ram, base_disk, image, libs)
    catalog_score = float((catalog_candidate or {}).get("match_score") or 0.0)
    telemetry_samples = int((telemetry or {}).get("samples") or 0)
    confidence_score = min(0.85, 0.35 + (catalog_score * 0.35) + (min(telemetry_samples, 20) / 20.0) * 0.15 + (0.15 if libs else 0.0))
    return {
        "requirements_txt": _build_requirements_from_libraries(libs),
        "suggested_cpu_threads": cpu,
        "suggested_ram_gb": ram,
        "suggested_disk_gb": disk,
        "detected_libraries": json.dumps(libs, ensure_ascii=False),
        "category": "geral",
        "docker_image": image,
        "confidence_score": round(confidence_score, 3),
        "image_source": "catalog_fallback",
        "catalog_match_score": catalog_score,
        "telemetry_samples": telemetry_samples,
        "catalog_candidate": catalog_candidate or None,
        "telemetry_summary": telemetry or None,
        "agent_reasoning": (
            "Modo fallback: Ollama indisponível ou pipeline multi-agente não executado. "
            "Requisitos derivados de catálogo, telemetria agregada e heurísticas locais."
            + (f" Margem RL: {margin_meta.get('reason')}." if margin_meta.get("applied") else "")
        ),
        "ai_raw_response": json.dumps({
            "mode": "fallback",
            "catalog_candidate": catalog_candidate,
            "telemetry_summary": telemetry,
            "ceiling_signals": ceiling_signals,
            "safety_margin": margin_meta,
            "agent_reasoning": (
                "Fallback sem os três agentes (Analista/Librarian/Strategist); "
                "estimativa determinística a partir do banco e bibliotecas detectadas."
            ),
        }, ensure_ascii=False),
        "used_fallback": True,
    }


def _extract_imported_libraries(script_content: str) -> list[str]:
    """
    Extração determinística de bibliotecas importadas no script Python.
    Retorna nomes base (ex.: 'sklearn' de 'sklearn.metrics').
    """
    libs = []
    seen = set()
    try:
        tree = ast.parse(script_content or "")
    except SyntaxError:
        return libs

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                name = (alias.name or "").split(".")[0].strip()
                if name and name not in seen:
                    seen.add(name)
                    libs.append(name)
        elif isinstance(node, ast.ImportFrom):
            module = (node.module or "").split(".")[0].strip()
            if module and module not in seen:
                seen.add(module)
                libs.append(module)
    return libs


def _normalize_detected_libraries(value: Any) -> list[str]:
    """
    Normaliza diferentes formatos para lista de bibliotecas.
    Aceita lista, string JSON ou string simples separada por vírgula.
    """
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [str(x).strip() for x in parsed if str(x).strip()]
        except json.JSONDecodeError:
            pass
        return [p.strip() for p in text.split(",") if p.strip()]

    return []


def _build_requirements_from_libraries(libraries: list[str]) -> str:
    """Monta requirements.txt básico a partir das libs detectadas e aliases dinâmicos PyPI."""
    stdlib = _stdlib_names_lower()
    packages = []
    seen = set()
    for lib in libraries:
        name = str(lib).strip()
        if not name:
            continue
        lower_name = name.lower().strip("'\"")
        if lower_name in stdlib or (lower_name.startswith("__") and lower_name.endswith("__")):
            continue
        pkg = DYNAMIC_PIP_ALIASES.get(lower_name, lower_name)
        if pkg not in seen:
            seen.add(pkg)
            packages.append(pkg)
    return "\n".join(packages)


def _sanitize_requirements_txt(
    requirements_txt: str,
    fallback_libraries: list[str],
    preinstalled_libraries: Optional[list[str]] = None,
) -> str:
    """
    Normaliza requirements.txt para 1 pacote por linha, removendo pacotes pré-instalados
    na imagem de container do catálogo para evitar sobrescrita de binários nativos C++.
    """
    pre_set = set()
    alias_map = {
        "cv2": "opencv-python",
        "opencv-python-headless": "opencv-python",
        "sklearn": "scikit-learn",
        "pil": "pillow",
        "bs4": "beautifulsoup4",
    }
    for p in (preinstalled_libraries or []):
        pl = str(p).strip().lower()
        if pl:
            pre_set.add(pl)
            pre_set.add(alias_map.get(pl, pl))

    text = (requirements_txt or "").strip()
    ast_text = _build_requirements_from_libraries(fallback_libraries)
    combined_text = f"{text}\n{ast_text}".strip()

    lines = []
    seen = set()
    for raw_line in combined_text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        tokens = re.split(r"[\s,;]+", line.replace("\t", " "))
        for token in tokens:
            t = token.strip()
            if not t:
                continue
            if not re.match(r"^[A-Za-z0-9_.\-]+([<>=!~].+)?$", t):
                continue
            pkg_name = re.split(r"[<>=!~]", t, maxsplit=1)[0].strip().lower().strip("'\"")
            if pkg_name in _stdlib_names_lower() or (
                pkg_name.startswith("__") and pkg_name.endswith("__")
            ):
                continue
            normalized_pkg = DYNAMIC_PIP_ALIASES.get(pkg_name, pkg_name)
            
            # Se a biblioteca já está pré-instalada na imagem Docker escolhida, omite do requirements.txt
            if pkg_name in pre_set or normalized_pkg in pre_set:
                continue
                
            suffix = t[len(pkg_name):] if t.lower().startswith(pkg_name) else ""
            normalized = f"{normalized_pkg}{suffix}"
            if normalized not in seen:
                seen.add(normalized)
                lines.append(normalized)

    if not lines:
        return _build_requirements_from_libraries(fallback_libraries)
    return "\n".join(lines)


def _apply_library_guardrails(
    cpu_threads: float,
    ram_gb: float,
    disk_gb: float,
    docker_image: str,
    libraries: list[str],
) -> tuple[float, float, float, str]:
    """
    Garante limites dinâmicos a partir da telemetria e do catálogo de imagens no SQLite,
    sem recorrer a dicionários estáticos hardcoded.
    """
    cpu = float(cpu_threads)
    ram = float(ram_gb)
    disk = float(disk_gb)
    image = str(docker_image or "python:3.11-slim")
    return cpu, ram, disk, image


def _compute_confidence_score(catalog_match_score: float, telemetry_samples: int, has_ast_libraries: bool, used_catalog_image: bool) -> float:
    score = 0.2
    score += min(max(float(catalog_match_score), 0.0), 1.0) * 0.4
    score += (min(max(int(telemetry_samples), 0), 30) / 30.0) * 0.2
    if has_ast_libraries:
        score += 0.1
    if used_catalog_image:
        score += 0.1
    return round(min(1.0, score), 3)


def _build_enriched_context(script_libraries: list[str]) -> tuple[Optional[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    candidates = db_crud.find_catalog_candidates_for_libraries(config.DATABASE_PATH, script_libraries, limit=3)
    best = candidates[0] if candidates else None
    telemetry = db_crud.get_telemetry_summary_for_libraries(config.DATABASE_PATH, script_libraries, limit=120)
    return best, telemetry, candidates


def _image_exists_in_catalog(image_name: str) -> bool:
    if not image_name:
        return False
    catalog = db_crud.get_docker_images_catalog(config.DATABASE_PATH)
    names = {str(row.get("image_name")) for row in catalog}
    return image_name in names


def _task_type_to_category(task_type: Any) -> str:
    """Mapeia task_type do Analista para categoria legada do pipeline."""
    t = str(task_type or "").strip()
    if t == "ML-heavy":
        return "ml_treinamento"
    if t == "IO-bound":
        return "etl_dados"
    if t == "CPU-bound":
        return "computacao_numerica"
    return "geral"


def analyze_task_requirements(
    script_content: str,
    history: Optional[list[dict]] = None,
    task_id: Optional[str] = None,
) -> dict[str, Any]:
    """
    Analisa o código da tarefa e retorna requirements.txt e estimativa de CPU/RAM.

    Args:
        script_content: Conteúdo do script (ex.: código Python).
        history: Lista de resultados anteriores com cpu_peak_percent, ram_peak_mb, detected_libraries
                 para calibrar a estimativa (tarefas similares no histórico).

    Returns:
        Dict com:
          - requirements_txt: string (conteúdo para requirements.txt)
          - suggested_cpu_threads / suggested_ram_gb / suggested_disk_gb: derivados de k8s_job_spec quando presente
          - detected_libraries: string (JSON array de bibliotecas)
          - docker_image: string (imagem de container; alinhada ao k8s_job_spec.image quando válida)
          - ai_raw_response: JSON com multi_agent_trace e k8s_job_spec do Strategist
    """
    history = history or []
    libraries_from_ast = _extract_imported_libraries(script_content)
    best_catalog_image = None
    telemetry_summary = {}
    catalog_candidates = []
    try:
        best_catalog_image, telemetry_summary, catalog_candidates = _build_enriched_context(libraries_from_ast)
    except Exception as e:
        logger.warning("Falha ao enriquecer contexto por catálogo/telemetria: %s", e)

    best_image_name = str((best_catalog_image or {}).get("image_name") or "python:3.9-slim")

    history_str = ""
    if history:
        lines = []
        for h in history[:15]:
            cpu = h.get("cpu_peak_percent")
            ram = h.get("ram_peak_mb")
            libs = h.get("detected_libraries") or "[]"
            if cpu is not None and ram is not None:
                lines.append(f"  - CPU pico: {cpu}%, RAM pico: {ram} MB, libs: {libs}")
        if lines:
            history_str = "Histórico de tarefas similares (uso real):\n" + "\n".join(lines)

    librarian_facts: dict[str, Any] = {}
    try:
        librarian_facts = _build_librarian_facts(libraries_from_ast)
    except Exception as e:
        logger.warning("Falha ao montar fatos do Librarian (catálogo/task_results): %s", e)
    if not librarian_facts.get("allowed_image_names"):
        try:
            catalog = db_crud.get_docker_images_catalog(config.DATABASE_PATH)
            librarian_facts = {
                "allowed_image_names": [str(r.get("image_name")) for r in catalog if r.get("image_name")],
                "catalog_candidates_for_script_libraries": catalog_candidates,
                "telemetry_summary": telemetry_summary,
            }
        except Exception as e2:
            logger.warning("Fallback mínimo de fatos do Librarian falhou: %s", e2)
    if not librarian_facts.get("allowed_image_names"):
        return _fallback_requirements(
            libraries_from_ast,
            catalog_candidate=best_catalog_image,
            telemetry=telemetry_summary,
            ceiling_signals=librarian_facts.get("ceiling_signals"),
        )

    orchestrated = _orchestrate_three_agents(
        script_content,
        libraries_from_ast,
        history_str,
        best_catalog_image,
        librarian_facts,
    )
    if not orchestrated:
        return _fallback_requirements(
            libraries_from_ast,
            catalog_candidate=best_catalog_image,
            telemetry=telemetry_summary,
            ceiling_signals=librarian_facts.get("ceiling_signals"),
        )

    out, trace = orchestrated
    analyst_out = trace.get("analyst_output") or {}
    librarian_out = trace.get("librarian_output") or {}
    node_ranker_trace = trace.get("node_ranker") if isinstance(trace.get("node_ranker"), dict) else None
    agent_reasoning = _format_agent_reasoning(analyst_out, librarian_out, out, node_ranker_trace)

    requirements_txt = out.get("requirements_txt") or ""
    if isinstance(requirements_txt, list):
        requirements_txt = "\n".join(str(x) for x in requirements_txt)
    detected_from_strategist = _normalize_detected_libraries(out.get("detected_libraries"))
    detected_from_analyst = _normalize_detected_libraries(analyst_out.get("detected_libraries"))
    detected_from_code = libraries_from_ast

    merged_detected = []
    merged_seen = set()
    for name in detected_from_code + detected_from_analyst + detected_from_strategist:
        if name not in merged_seen:
            merged_seen.add(name)
            merged_detected.append(name)

    requirements_txt = _sanitize_requirements_txt(requirements_txt, merged_detected)
    detected = json.dumps(merged_detected, ensure_ascii=False)

    k8s_spec = out.get("k8s_job_spec")
    if not isinstance(k8s_spec, dict):
        k8s_spec = {}

    ranked_for_enforce: list[dict[str, Any]] = []
    if node_ranker_trace and isinstance(node_ranker_trace.get("top_nodes"), list):
        ranked_for_enforce = [
            x for x in node_ranker_trace["top_nodes"] if isinstance(x, dict)
        ]
    k8s_spec, selector_correction = sched_inspector_enforcement(k8s_spec, ranked_for_enforce)
    if selector_correction and node_ranker_trace is not None:
        node_ranker_trace["sched_inspector_enforcement"] = selector_correction
    if isinstance(out, dict):
        out = dict(out)
        out["k8s_job_spec"] = k8s_spec

    piso_hw = librarian_out.get("piso_hardware")
    piso = piso_hw if isinstance(piso_hw, dict) else {}
    try:
        cpu_floor = float(piso.get("cpu_cores_floor", 1.0))
    except (TypeError, ValueError):
        cpu_floor = 1.0
    try:
        ram_floor = float(piso.get("ram_gb_floor", 0.5))
    except (TypeError, ValueError):
        ram_floor = 0.5

    facts_for_mins: dict[str, Any] = {"catalog_candidates_for_script_libraries": catalog_candidates}
    
    # Se houver candidato do catálogo com correspondência de bibliotecas (best_catalog_image com match_score > 0),
    # priorizar essa imagem especializada em relação a seleções genéricas (ex: python:3.11-slim)
    if best_catalog_image and float(best_catalog_image.get("match_score") or 0.0) > 0:
        docker_image = str(best_catalog_image.get("image_name") or "")
        image_source = "catalog_match"
    else:
        docker_image = str(
            k8s_spec.get("image")
            or out.get("docker_image")
            or librarian_out.get("best_image")
            or best_image_name
            or "python:3.11-slim"
        )
        image_source = "ai"

    if not _image_exists_in_catalog(docker_image):
        docker_image = best_image_name or "python:3.11-slim"
        image_source = "catalog_guardrail"

    cat_min_cpu, cat_min_ram, cat_min_disk = _min_cpu_ram_for_catalog_image(docker_image, facts_for_mins, best_catalog_image)

    has_k8s_resources = any(
        k8s_spec.get(k) for k in ("cpu_request", "cpu_limit", "memory_request", "memory_limit")
    )
    if has_k8s_resources:
        cpu_threads, ram_gb = _suggested_from_k8s_job_spec(
            k8s_spec, cpu_floor, ram_floor, cat_min_cpu, cat_min_ram
        )
    else:
        try:
            cpu_threads = float(out.get("suggested_cpu_threads", max(cpu_floor, cat_min_cpu)))
        except (TypeError, ValueError):
            cpu_threads = float(max(cpu_floor, cat_min_cpu))
        try:
            ram_gb = float(out.get("suggested_ram_gb", max(ram_floor, cat_min_ram)))
        except (TypeError, ValueError):
            ram_gb = float(max(ram_floor, cat_min_ram))

    try:
        disk_gb = float(out.get("suggested_disk_gb", cat_min_disk))
    except (TypeError, ValueError):
        disk_gb = cat_min_disk

    cpu_threads, ram_gb, disk_gb, docker_image = _apply_library_guardrails(
        cpu_threads, ram_gb, disk_gb, docker_image, merged_detected
    )

    # Clamp determinístico pelos mínimos da imagem do catálogo (ex: tensorflow -> 4.0 CPU / 6.0 GB RAM / 12.0 GB Disco)
    selected_catalog_row = None
    for row in catalog_candidates:
        if row.get("image_name") == docker_image:
            selected_catalog_row = row
            break
    if selected_catalog_row:
        cpu_threads = max(cpu_threads, float(selected_catalog_row.get("min_cpu") or 1.0))
        ram_gb = max(ram_gb, float(selected_catalog_row.get("min_ram_gb") or 0.5))
        disk_gb = max(disk_gb, float(selected_catalog_row.get("min_disk_gb") or 1.0))
    elif best_catalog_image:
        cpu_threads = max(cpu_threads, float(best_catalog_image.get("min_cpu") or 1.0))
        ram_gb = max(ram_gb, float(best_catalog_image.get("min_ram_gb") or 0.5))
        disk_gb = max(disk_gb, float(best_catalog_image.get("min_disk_gb") or 1.0))

    cpu_threads = max(1.0, min(cpu_threads, 8.0))
    ram_gb = max(0.25, min(ram_gb, 8.0))
    disk_gb = max(0.5, min(disk_gb, 50.0))

    # Atualiza limits e requests no k8s_spec para refletir a alocação real de hardware derivada
    from context_server.services.k8s_job_executor import _float_cpu_to_quantity, _float_ram_to_quantity
    k8s_spec["cpu_limit"] = _float_cpu_to_quantity(cpu_threads)
    k8s_spec["memory_limit"] = _float_ram_to_quantity(ram_gb)
    if not k8s_spec.get("cpu_request"):
        k8s_spec["cpu_request"] = _float_cpu_to_quantity(max(0.5, cpu_threads * 0.5))
    if not k8s_spec.get("memory_request"):
        k8s_spec["memory_request"] = _float_ram_to_quantity(max(0.5, ram_gb * 0.5))

    try:
        confidence_score = float(out.get("confidence_score"))
        if confidence_score < 0 or confidence_score > 1:
            raise ValueError("confidence fora da faixa")
    except Exception:
        confidence_score = _compute_confidence_score(
            catalog_match_score=float((best_catalog_image or {}).get("match_score") or 0.0),
            telemetry_samples=int((telemetry_summary or {}).get("samples") or 0),
            has_ast_libraries=bool(libraries_from_ast),
            used_catalog_image=_image_exists_in_catalog(docker_image),
        )

    ai_raw = dict(out) if isinstance(out, dict) else {}
    ai_raw["hybrid_context"] = {
        "catalog_candidate": best_catalog_image,
        "catalog_candidates": catalog_candidates,
        "telemetry_summary": telemetry_summary,
        "image_source": image_source,
    }
    ai_raw["agent_reasoning"] = agent_reasoning
    ai_raw["multi_agent_trace"] = {
        "analyst_output": trace.get("analyst_output"),
        "librarian_output": trace.get("librarian_output"),
        "node_ranker": trace.get("node_ranker"),
        "strategist_summary": {
            "k8s_job_spec": k8s_spec,
            "suggested_disk_gb": out.get("suggested_disk_gb"),
            "docker_image_resolved": docker_image,
            "category": out.get("category"),
            "confidence_score": out.get("confidence_score"),
            "decision_rationale": out.get("decision_rationale"),
            "suggested_cpu_threads_derived": cpu_threads,
            "suggested_ram_gb_derived": ram_gb,
        },
    }

    final_host = ""
    ns_final = k8s_spec.get("nodeSelector") if isinstance(k8s_spec.get("nodeSelector"), dict) else {}
    if isinstance(ns_final, dict):
        final_host = str(ns_final.get("kubernetes.io/hostname") or ns_final.get("hostname") or "").strip()
    if ranked_for_enforce or final_host:
        log_sched_inspector_decision(
            ranked_for_enforce,
            final_host,
            was_corrected=selector_correction is not None,
        )

    log_planejamento(
        task_id,
        detected,
        analyst_out.get("task_type"),
        k8s_spec=k8s_spec,
    )

    return {
        "requirements_txt": requirements_txt,
        "suggested_cpu_threads": cpu_threads,
        "suggested_ram_gb": ram_gb,
        "suggested_disk_gb": disk_gb,
        "detected_libraries": detected,
        "category": str(
            out.get("category")
            or _task_type_to_category(analyst_out.get("task_type"))
            or "geral"
        ),
        "docker_image": docker_image,
        "confidence_score": confidence_score,
        "image_source": image_source,
        "catalog_match_score": float((best_catalog_image or {}).get("match_score") or 0.0),
        "telemetry_samples": int((telemetry_summary or {}).get("samples") or 0),
        "catalog_candidate": best_catalog_image,
        "telemetry_summary": telemetry_summary,
        "agent_reasoning": agent_reasoning,
        "ai_raw_response": json.dumps(ai_raw, ensure_ascii=False),
        "used_fallback": False,
    }
