#!/usr/bin/env python3
"""
Exporta métricas de avaliação do protótipo TCC a partir do SQLite local.

Uso:
  python scripts/export_evaluation_metrics.py
  python scripts/export_evaluation_metrics.py --db data/nodes.db --out exports/metrics

Gera:
  - evaluation_tasks.csv  (uma linha por tarefa com latências e telemetria)
  - evaluation_summary.txt (taxas de sucesso, médias, cobertura de campos)
"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _f(v):
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _delta(a, b):
    fa, fb = _f(a), _f(b)
    if fa is None or fb is None:
        return None
    return round(fb - fa, 3)


def _ts(v):
    fv = _f(v)
    if fv is None:
        return ""
    try:
        return datetime.fromtimestamp(fv, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except (OSError, OverflowError, ValueError):
        return str(fv)


def load_rows(db_path: Path) -> list[dict]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    # Colunas novas podem não existir em DBs muito antigos — SELECT * + defaults.
    cur = conn.execute(
        """
        SELECT t.*, tr.status_final, tr.duration_seconds, tr.completed_at,
               tr.cpu_peak_percent, tr.ram_peak_mb, tr.prediction_delta_ram,
               tr.detected_libraries AS result_detected_libraries
        FROM tasks t
        LEFT JOIN task_results tr ON tr.task_id = t.task_id
        ORDER BY t.submitted_at ASC
        """
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def enrich(row: dict) -> dict:
    submitted = _f(row.get("submitted_at"))
    planning = _f(row.get("planning_completed_at"))
    dispatched = _f(row.get("dispatched_at"))
    completed = _f(row.get("completed_at"))
    duration = _f(row.get("duration_seconds"))

    realloc_attempts = row.get("reallocation_attempts")
    if realloc_attempts is None:
        realloc_attempts = 0
        raw = row.get("ai_raw_response") or ""
        try:
            data = json.loads(raw) if str(raw).strip() else {}
            if isinstance(data, dict):
                block = data.get("reallocation") or {}
                if isinstance(block, dict):
                    realloc_attempts = int(block.get("attempts") or 0)
        except (json.JSONDecodeError, TypeError, ValueError):
            pass

    planning_latency_s = _delta(submitted, planning)
    queue_wait_s = _delta(planning, dispatched) if planning and dispatched else None
    submit_to_dispatch_s = _delta(submitted, dispatched)
    e2e_s = _delta(submitted, completed)
    # Tempo de resposta observado ≈ e2e; execução pura = duration_seconds

    success = 1 if str(row.get("status_final") or row.get("status") or "").lower() in (
        "concluida",
        "concluída",
    ) else 0
    if row.get("status_final") is None and str(row.get("status") or "") not in (
        "concluida",
        "falha",
        "cancelada",
        "erro_agente",
    ):
        success = ""  # ainda em andamento

    return {
        "task_id": row.get("task_id"),
        "filename": row.get("filename"),
        "user_id": row.get("user_id"),
        "status": row.get("status"),
        "status_final": row.get("status_final") or "",
        "success": success,
        "allocated_node_id": row.get("allocated_node_id") or "",
        "preferred_compute_layer": row.get("preferred_compute_layer") or "",
        "allocated_compute_layer": row.get("allocated_compute_layer") or "",
        "docker_image": row.get("docker_image") or "",
        "ai_category": row.get("ai_category") or "",
        "required_cpu_threads": row.get("required_cpu_threads"),
        "required_memory_gb": row.get("required_memory_gb"),
        "reallocation_attempts": realloc_attempts,
        "submitted_at": _ts(submitted),
        "planning_completed_at": _ts(planning),
        "dispatched_at": _ts(dispatched),
        "completed_at": _ts(completed),
        "planning_latency_s": planning_latency_s,
        "queue_wait_s": queue_wait_s,
        "submit_to_dispatch_s": submit_to_dispatch_s,
        "e2e_latency_s": e2e_s,
        "execution_duration_s": duration,
        "cpu_peak_percent": row.get("cpu_peak_percent"),
        "ram_peak_mb": row.get("ram_peak_mb"),
        "prediction_delta_ram_gb": row.get("prediction_delta_ram"),
        "ai_detected_libraries": row.get("ai_detected_libraries") or "",
    }


def summarize(rows: list[dict]) -> str:
    lines = []
    lines.append("=== Resumo de avaliacao TCC ===")
    lines.append(f"Gerado em: {datetime.now().isoformat(timespec='seconds')}")
    lines.append(f"Total de tarefas: {len(rows)}")

    with_result = [r for r in rows if r.get("status_final")]
    concluded = [r for r in with_result if r.get("success") == 1]
    failed = [r for r in with_result if str(r.get("status_final")).lower() == "falha"]
    lines.append(f"Com resultado: {len(with_result)}")
    lines.append(f"Concluídas: {len(concluded)}")
    lines.append(f"Falhas: {len(failed)}")
    if with_result:
        rate = 100.0 * len(concluded) / len(with_result)
        lines.append(f"Taxa de sucesso (entre finalizadas): {rate:.1f}%")

    def _avg(field, subset=None):
        src = subset if subset is not None else rows
        vals = [_f(r.get(field)) for r in src]
        vals = [v for v in vals if v is not None]
        if not vals:
            return None, 0
        return round(statistics.mean(vals), 3), len(vals)

    for label, field in (
        ("Latência planejamento (s)", "planning_latency_s"),
        ("Espera na fila (s)", "queue_wait_s"),
        ("Submit->despacho (s)", "submit_to_dispatch_s"),
        ("Latencia ponta-a-ponta (s)", "e2e_latency_s"),
        ("Duracao execucao (s)", "execution_duration_s"),
        ("Pico CPU (%)", "cpu_peak_percent"),
        ("Pico RAM (MB)", "ram_peak_mb"),
        ("Delta RAM previsto-real (GB)", "prediction_delta_ram_gb"),
    ):
        avg, n = _avg(field, concluded if "Latencia" in label or "Duracao" in label or "Pico" in label or "Delta" in label else rows)
        lines.append(f"{label}: média={avg if avg is not None else 'n/d'} (n={n})")

    # Cobertura de campos críticos
    def cov(field):
        if not rows:
            return 0.0
        filled = sum(1 for r in rows if r.get(field) not in (None, "", []))
        return 100.0 * filled / len(rows)

    lines.append("")
    lines.append("--- Cobertura de campos (todas as tarefas) ---")
    for field in (
        "docker_image",
        "preferred_compute_layer",
        "allocated_compute_layer",
        "planning_completed_at",
        "dispatched_at",
        "cpu_peak_percent",
        "ram_peak_mb",
        "prediction_delta_ram_gb",
    ):
        lines.append(f"{field}: {cov(field):.0f}%")

    realloc = [r for r in rows if int(r.get("reallocation_attempts") or 0) > 0]
    lines.append(f"Tarefas com realocação: {len(realloc)}")

    by_layer: dict[str, int] = {}
    for r in rows:
        layer = r.get("allocated_compute_layer") or "(vazio)"
        by_layer[layer] = by_layer.get(layer, 0) + 1
    lines.append("Alocações por camada: " + ", ".join(f"{k}={v}" for k, v in sorted(by_layer.items())))

    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description="Exporta métricas de avaliação do TCC")
    parser.add_argument("--db", default=str(_ROOT / "data" / "nodes.db"))
    parser.add_argument("--out", default=str(_ROOT / "exports" / "metrics"))
    args = parser.parse_args()

    db_path = Path(args.db)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not db_path.is_file():
        raise SystemExit(f"Banco não encontrado: {db_path}")

    raw = load_rows(db_path)
    enriched = [enrich(r) for r in raw]

    csv_path = out_dir / "evaluation_tasks.csv"
    if enriched:
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(enriched[0].keys()), delimiter=";")
            writer.writeheader()
            writer.writerows(enriched)

    summary = summarize(enriched)
    summary_path = out_dir / "evaluation_summary.txt"
    summary_path.write_text(summary, encoding="utf-8")
    print(summary)
    print(f"CSV: {csv_path}")
    print(f"Resumo: {summary_path}")


if __name__ == "__main__":
    main()
