#!/usr/bin/env python3
"""
Sensibilidade da temperatura do LLM (analise offline, sem cluster).

Chama os agentes reais do broker (Analista e Strategist, com os prompts do
ai_service) em varias temperaturas e compara as respostas. Nao altera o codigo
de producao: a temperatura e sobrescrita por um wrapper de `_call_ollama`.

Por temperatura e por script de avaliacao, repete N vezes e mede:
  - validade do JSON do Analista (chaves, task_type, complexidade 1..10);
  - distribuicao de task_type e complexidade;
  - camada preferida derivada (preferred_layer_for_workload, floors de cold start);
  - concordancia com a camada nominal do script (edge/fog/cloud);
  - variabilidade: numero de saidas distintas entre as N repeticoes;
  - validade da especificacao do Strategist (k8s_job_spec completo, imagem no catalogo).

Uso:
  python scripts/eval_llm_temperature.py
  python scripts/eval_llm_temperature.py --reps 10 --temps 0 0.5 1.0
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import statistics as st
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

os.environ.setdefault("OLLAMA_URL", "http://127.0.0.1:11434")
os.environ.setdefault("OLLAMA_MODEL", "LiquidAI/lfm2.5-1.2b-instruct")

from _runlog import start_run_log  # noqa: E402
from context_server.services import ai_service  # noqa: E402
from context_server.services.compute_layer import preferred_layer_for_workload  # noqa: E402

EVAL_DIR = ROOT / "scripts" / "eval_tasks"
# script -> camada nominal (None = sem camada nominal definida)
SCRIPTS = {
    "task_edge_light.py": "edge",
    "task_fog_cpu.py": "fog",
    "task_cloud_ml.py": "cloud",
    "task_exp1_memory.py": None,
}
TASK_TYPES = {"CPU-bound", "IO-bound", "ML-heavy"}
ALLOWED_IMAGES = ["python:3.11-slim", "python:3.9-slim"]
# Librarian de cold start (sem historico), identico ao caminho real da 1a execucao.
LIBRARIAN_COLD = {
    "best_image": "python:3.11-slim",
    "piso_hardware": {"cpu_cores_floor": 1.0, "ram_gb_floor": 0.5},
    "telemetry_basis": "Sem telemetria historica; piso minimo do catalogo.",
    "catalog_alignment": "python:3.11-slim",
}

_state = {"temperature": 0.0, "calls": []}
_orig_call = ai_service._call_ollama


def _patched_call(prompt, system=None, json_mode=True, temperature=0.0):
    t0 = time.monotonic()
    raw = _orig_call(prompt, system=system, json_mode=json_mode, temperature=_state["temperature"])
    _state["calls"].append({"latency_s": round(time.monotonic() - t0, 3), "raw": raw})
    return raw


ai_service._call_ollama = _patched_call


def _analyst_valid(d) -> tuple[bool, str]:
    if not isinstance(d, dict):
        return False, "json_invalido"
    if d.get("task_type") not in TASK_TYPES:
        return False, "task_type_invalido"
    try:
        c = int(d.get("complexity_score"))
    except (TypeError, ValueError):
        return False, "complexidade_invalida"
    if not 1 <= c <= 10:
        return False, "complexidade_fora_de_1_10"
    return True, "ok"


def _strategist_valid(d) -> tuple[bool, str]:
    if not isinstance(d, dict):
        return False, "json_invalido"
    spec = d.get("k8s_job_spec")
    if not isinstance(spec, dict):
        return False, "sem_k8s_job_spec"
    for k in ("image", "cpu_request", "cpu_limit", "memory_request", "memory_limit"):
        if not spec.get(k):
            return False, f"falta_{k}"
    if str(spec.get("image")) not in ALLOWED_IMAGES:
        return False, "imagem_fora_do_catalogo"
    if not isinstance(d.get("requirements_txt", ""), str):
        return False, "requirements_nao_texto"
    return True, "ok"


def _digest(obj) -> str:
    return hashlib.sha1(json.dumps(obj, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:10]


def run_one(script_text: str, libs: list[str], temperature: float) -> dict:
    _state["temperature"] = temperature
    _state["calls"] = []
    analyst = ai_service._run_analyst_agent(script_text, libs)
    a_ok, a_why = _analyst_valid(analyst)
    out = {
        "analyst_valid": a_ok,
        "analyst_reason": a_why,
        "analyst_latency_s": _state["calls"][-1]["latency_s"] if _state["calls"] else None,
        "analyst": analyst if isinstance(analyst, dict) else None,
    }
    if a_ok:
        out["task_type"] = analyst["task_type"]
        out["complexity"] = int(analyst["complexity_score"])
        out["preferred_layer"] = preferred_layer_for_workload(
            task_type=out["task_type"],
            complexity_score=out["complexity"],
            required_memory_gb=0.5,
            required_cpu_threads=1.0,
        )
        out["analyst_digest"] = _digest(
            {"t": out["task_type"], "c": out["complexity"], "l": sorted(analyst.get("detected_libraries") or [])}
        )
        _state["calls"] = []
        strat = ai_service._run_strategist_agent(
            analyst=analyst,
            librarian=LIBRARIAN_COLD,
            history_str="",
            libraries_from_ast=libs,
            best_image_name="python:3.11-slim",
            best_image_min_cpu=1.0,
            best_image_min_ram=0.5,
            allowed_image_names=ALLOWED_IMAGES,
        )
        s_ok, s_why = _strategist_valid(strat)
        out.update(
            strategist_valid=s_ok,
            strategist_reason=s_why,
            strategist_latency_s=_state["calls"][-1]["latency_s"] if _state["calls"] else None,
        )
        if s_ok:
            spec = strat["k8s_job_spec"]
            out["strategist_image"] = spec.get("image")
            out["memory_limit"] = spec.get("memory_limit")
            out["cpu_limit"] = spec.get("cpu_limit")
            out["strategist_digest"] = _digest({"i": spec.get("image"), "m": spec.get("memory_limit"),
                                                "c": spec.get("cpu_limit"),
                                                "r": (strat.get("requirements_txt") or "").strip()})
    else:
        out["strategist_valid"] = False
        out["strategist_reason"] = "analista_invalido"
    return out


def summarize(rows: list[dict], nominal: str | None) -> dict:
    n = len(rows)
    ok = [r for r in rows if r["analyst_valid"]]
    s_ok = [r for r in rows if r.get("strategist_valid")]
    cx = [r["complexity"] for r in ok]
    layers = Counter(r["preferred_layer"] for r in ok)
    return {
        "n": n,
        "analyst_valid_rate": round(len(ok) / n, 3),
        "strategist_valid_rate": round(len(s_ok) / n, 3),
        "task_type": dict(Counter(r["task_type"] for r in ok)),
        "complexity_mean": round(st.mean(cx), 2) if cx else None,
        "complexity_sd": round(st.pstdev(cx), 2) if len(cx) > 1 else 0.0,
        "complexity_values": sorted(Counter(cx).items()),
        "preferred_layer": dict(layers),
        "agree_nominal": (round(sum(1 for r in ok if r["preferred_layer"] == nominal) / n, 3)
                          if nominal else None),
        "distinct_analyst_outputs": len({r["analyst_digest"] for r in ok}),
        "distinct_strategist_outputs": len({r["strategist_digest"] for r in s_ok}),
        "invalid_reasons": dict(Counter([r["analyst_reason"] for r in rows if not r["analyst_valid"]]
                                        + [r["strategist_reason"] for r in rows
                                           if r["analyst_valid"] and not r.get("strategist_valid")])),
        "latency_analyst_s_mean": round(st.mean([r["analyst_latency_s"] for r in rows if r.get("analyst_latency_s")]), 2),
    }


def main() -> int:
    p = argparse.ArgumentParser(description="Sensibilidade da temperatura do LLM (offline)")
    p.add_argument("--temps", type=float, nargs="+", default=[0.0, 0.5, 1.0])
    p.add_argument("--reps", type=int, default=10)
    args = p.parse_args()
    log_path = start_run_log("llm_temperatura", "analise")

    print("=" * 64)
    print(f"Sensibilidade da temperatura | modelo={ai_service.OLLAMA_MODEL}")
    print(f"temps={args.temps} reps={args.reps} scripts={list(SCRIPTS)}")
    print("=" * 64, flush=True)

    # aquecimento do modelo (carga em memoria) para nao distorcer a 1a latencia
    _state["temperature"] = 0.0
    ai_service._call_ollama("responda {}", system="Responda somente JSON.", json_mode=True)

    results = []
    summary = {}
    for t in args.temps:
        for name, nominal in SCRIPTS.items():
            text = (EVAL_DIR / name).read_text(encoding="utf-8")
            libs = ai_service._extract_imported_libraries(text)
            rows = []
            for i in range(1, args.reps + 1):
                r = run_one(text, libs, t)
                r.update(temperature=t, script=name, rep=i)
                rows.append(r)
                print(
                    f"  T={t} {name} #{i}: analista={r['analyst_reason']} "
                    f"tipo={r.get('task_type')} cx={r.get('complexity')} "
                    f"camada={r.get('preferred_layer')} strat={r.get('strategist_reason')}",
                    flush=True,
                )
            results.extend(rows)
            summary[f"T={t}|{name}"] = summarize(rows, nominal)
            print(f"== T={t} {name}: {json.dumps(summary[f'T={t}|{name}'], ensure_ascii=False)}", flush=True)

    out_dir = ROOT / "results" / "metrics_export" / "llm_temperatura"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"llm_temperatura_{int(time.time())}.json"
    out.write_text(
        json.dumps(
            {
                "experiment": "llm_temperature_sensitivity",
                "model": ai_service.OLLAMA_MODEL,
                "temps": args.temps,
                "reps": args.reps,
                "layer_floors": {"required_memory_gb": 0.5, "required_cpu_threads": 1.0},
                "summary": summary,
                "results": results,
                "at": datetime.now(timezone.utc).isoformat(),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"\nJSON: {out}\nLOG: {log_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
