#!/usr/bin/env python3
"""Mede latência do pipeline multi-agente (mesmo script da Fase 1).

Uso (dentro do container context-server):
  python /tmp/bench_ai_planning.py
"""
from __future__ import annotations

import json
import time
from pathlib import Path

SCRIPT = '''#!/usr/bin/env python3
"""Probe de avaliação: usa `requests` (catálogo → python:3.11-slim) sem rede externa."""
import requests

def main():
    # Só precisa da lib instalada na imagem; não depende de internet.
    print("requests_version", requests.__version__)
    print("ok")

if __name__ == "__main__":
    main()
'''

RUNS = 4


def main() -> None:
    from context_server.services.ai_service import OLLAMA_MODEL, analyze_task_requirements

    print(f"model={OLLAMA_MODEL}")
    print(f"runs={RUNS}")
    results = []
    for i in range(1, RUNS + 1):
        t0 = time.perf_counter()
        out = analyze_task_requirements(SCRIPT, history=[], task_id=f"bench-lfm-{i}")
        elapsed = time.perf_counter() - t0
        fallback = bool(out.get("used_fallback", True))
        row = {
            "run": i,
            "planning_s": round(elapsed, 3),
            "used_fallback": fallback,
            "docker_image": out.get("docker_image"),
            "category": out.get("category"),
            "detected_libraries": out.get("detected_libraries"),
            "cpu": out.get("suggested_cpu_threads"),
            "ram_gb": out.get("suggested_ram_gb"),
        }
        results.append(row)
        print(json.dumps(row, ensure_ascii=False))

    times = [r["planning_s"] for r in results]
    summary = {
        "model": OLLAMA_MODEL,
        "n": len(times),
        "planning_s_mean": round(sum(times) / len(times), 3),
        "planning_s_min": min(times),
        "planning_s_max": max(times),
        "runs": results,
    }
    out_path = Path("/app/data/bench_ai_planning_lfm25.json")
    try:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"wrote={out_path}")
    except OSError as exc:
        print(f"write_failed={exc}")
    print(json.dumps({k: summary[k] for k in ("model", "n", "planning_s_mean", "planning_s_min", "planning_s_max")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
