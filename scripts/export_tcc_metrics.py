"""Auditoria e exportação de métricas do protótipo TCC a partir do SQLite local."""

from __future__ import annotations

import csv
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "data" / "nodes.db"
OUT_DIR = ROOT / "results" / "metrics_export"


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _parse_ai(raw: str | None) -> dict:
    if not raw or not str(raw).strip():
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, TypeError):
        return {}


def _layer_pref(ai: dict) -> str | None:
    realloc = ai.get("reallocation") if isinstance(ai.get("reallocation"), dict) else {}
    # preferência pode estar no ranker trace
    trace = ai.get("multi_agent_trace") if isinstance(ai.get("multi_agent_trace"), dict) else {}
    nr = trace.get("node_ranker") if isinstance(trace.get("node_ranker"), dict) else {}
    for key in ("preferred_layer", "preferred_compute_layer"):
        if nr.get(key):
            return str(nr.get(key))
    analyst = trace.get("analyst_output") if isinstance(trace.get("analyst_output"), dict) else {}
    return None


def audit(conn: sqlite3.Connection) -> dict:
    tasks = [dict(r) for r in conn.execute("SELECT * FROM tasks")]
    results = {
        r["task_id"]: dict(r)
        for r in conn.execute("SELECT * FROM task_results")
    }
    nodes = [dict(r) for r in conn.execute("SELECT * FROM nodes")]

    by_status: dict[str, int] = {}
    for t in tasks:
        by_status[t["status"]] = by_status.get(t["status"], 0) + 1

    with_result = sum(1 for t in tasks if t["task_id"] in results)
    with_cpu = sum(
        1
        for tid, r in results.items()
        if r.get("cpu_peak_percent") is not None
    )
    with_ram = sum(
        1
        for tid, r in results.items()
        if r.get("ram_peak_mb") is not None
    )
    with_delta = sum(
        1
        for tid, r in results.items()
        if r.get("prediction_delta_ram") is not None
    )
    k8s_hostname = sum(
        1
        for t in tasks
        if (t.get("allocated_node_id") or "").startswith("k8s:")
        and not (t.get("allocated_node_id") or "")[4:] == t["task_id"]
    )
    k8s_uuid_style = sum(
        1
        for t in tasks
        if (t.get("allocated_node_id") or "").startswith("k8s:")
        and (t.get("allocated_node_id") or "")[4:] == t["task_id"]
    )
    realloc_n = 0
    for t in tasks:
        ai = _parse_ai(t.get("ai_raw_response"))
        if isinstance(ai.get("reallocation"), dict) and ai["reallocation"].get("attempts"):
            realloc_n += 1

    layers = {}
    for n in nodes:
        layer = n.get("compute_layer") or "(null)"
        layers[layer] = layers.get(layer, 0) + 1

    gaps = []
    if with_cpu < with_result:
        gaps.append(
            f"Telemetria CPU ausente em {with_result - with_cpu}/{with_result} resultados"
        )
    if with_ram < with_result:
        gaps.append(
            f"Telemetria RAM ausente em {with_result - with_ram}/{with_result} resultados"
        )
    if with_delta < with_result:
        gaps.append(
            f"prediction_delta_ram ausente em {with_result - with_delta}/{with_result} resultados"
        )
    if k8s_uuid_style:
        gaps.append(
            f"{k8s_uuid_style} alocações K8s no formato legado k8s:{{task_id}} (sem hostname)"
        )
    stuck = by_status.get("executando", 0) + by_status.get("alocada", 0) + by_status.get("na_fila", 0)
    if stuck:
        gaps.append(f"{stuck} tarefa(s) em estado intermediário (executando/alocada/na_fila)")

    # Campos essenciais para monografia
    required_fields = {
        "submitted_at": sum(1 for t in tasks if t.get("submitted_at")),
        "allocated_node_id": sum(1 for t in tasks if t.get("allocated_node_id")),
        "docker_image": sum(1 for t in tasks if t.get("docker_image")),
        "required_cpu_threads": sum(1 for t in tasks if t.get("required_cpu_threads") is not None),
        "required_memory_gb": sum(1 for t in tasks if t.get("required_memory_gb") is not None),
        "ai_raw_response": sum(1 for t in tasks if t.get("ai_raw_response")),
        "duration_seconds": sum(1 for r in results.values() if r.get("duration_seconds") is not None),
        "completed_at": sum(1 for r in results.values() if r.get("completed_at")),
    }

    return {
        "tasks_total": len(tasks),
        "results_total": len(results),
        "by_status": by_status,
        "with_result": with_result,
        "telemetry_cpu": with_cpu,
        "telemetry_ram": with_ram,
        "prediction_delta_ram": with_delta,
        "k8s_with_hostname": k8s_hostname,
        "k8s_legacy_uuid": k8s_uuid_style,
        "reallocation_tasks": realloc_n,
        "nodes": len(nodes),
        "nodes_by_layer": layers,
        "field_coverage": required_fields,
        "gaps": gaps,
    }


def export_rows(conn: sqlite3.Connection, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    csv_path = out_dir / f"tcc_metrics_{stamp}.csv"

    rows = conn.execute(
        """
        SELECT
            t.task_id,
            t.status,
            t.task_type,
            t.filename,
            t.user_id,
            t.submitted_at,
            t.allocated_node_id,
            t.docker_image,
            t.ai_category,
            t.required_cpu_threads,
            t.required_memory_gb,
            t.required_disk_gb,
            t.planning_completed_at,
            t.dispatched_at,
            t.preferred_compute_layer,
            t.allocated_compute_layer,
            t.ai_raw_response,
            tr.status_final,
            tr.duration_seconds,
            tr.completed_at,
            tr.cpu_peak_percent,
            tr.ram_peak_mb,
            tr.prediction_delta_ram,
            tr.detected_libraries
        FROM tasks t
        LEFT JOIN task_results tr ON tr.task_id = t.task_id
        ORDER BY t.submitted_at DESC
        """
    )

    fieldnames = [
        "task_id",
        "status",
        "status_final",
        "task_type",
        "filename",
        "user_id",
        "submitted_at_iso",
        "planning_completed_at_iso",
        "dispatched_at_iso",
        "completed_at_iso",
        "planning_s",
        "queue_wait_s",
        "submit_to_dispatch_s",
        "latency_submit_to_complete_s",
        "duration_seconds",
        "allocated_node_id",
        "allocated_hostname",
        "compute_layer_node",
        "preferred_layer",
        "allocated_layer",
        "docker_image",
        "ai_category",
        "required_cpu_threads",
        "required_memory_gb",
        "required_disk_gb",
        "cpu_peak_percent",
        "ram_peak_mb",
        "prediction_delta_ram",
        "reallocation_attempts",
        "excluded_hostnames",
        "detected_libraries",
    ]

    # mapa node_id/hostname -> layer
    layer_by_key: dict[str, str] = {}
    for n in conn.execute("SELECT node_id, hostname, compute_layer FROM nodes"):
        if n["compute_layer"]:
            layer_by_key[str(n["node_id"] or "")] = n["compute_layer"]
            layer_by_key[str(n["hostname"] or "")] = n["compute_layer"]

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        w.writeheader()
        for r in rows:
            ai = _parse_ai(r["ai_raw_response"])
            realloc = ai.get("reallocation") if isinstance(ai.get("reallocation"), dict) else {}
            alloc = (r["allocated_node_id"] or "")
            host = alloc[4:] if alloc.startswith("k8s:") else alloc
            sub = r["submitted_at"]
            comp = r["completed_at"]
            latency = None
            if sub is not None and comp is not None:
                try:
                    latency = round(float(comp) - float(sub), 3)
                except (TypeError, ValueError):
                    latency = None

            def _iso(ts):
                if ts is None:
                    return ""
                try:
                    return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()
                except (TypeError, ValueError, OSError):
                    return ""

            preferred = r["preferred_compute_layer"] if "preferred_compute_layer" in r.keys() else None
            if not preferred:
                trace = ai.get("multi_agent_trace") if isinstance(ai.get("multi_agent_trace"), dict) else {}
                nr = trace.get("node_ranker") if isinstance(trace.get("node_ranker"), dict) else {}
                preferred = nr.get("preferred_layer") or nr.get("preferred_compute_layer")
                if not preferred:
                    dm = ai.get("dispatch_meta") if isinstance(ai.get("dispatch_meta"), dict) else {}
                    preferred = dm.get("preferred_layer")
                summ = trace.get("strategist_summary") if isinstance(trace.get("strategist_summary"), dict) else {}
                if not preferred and isinstance(summ.get("dispatch_meta"), dict):
                    preferred = summ["dispatch_meta"].get("preferred_layer")

            plan_ts = r["planning_completed_at"] if "planning_completed_at" in r.keys() else None
            disp_ts = r["dispatched_at"] if "dispatched_at" in r.keys() else None
            alloc_layer = (
                r["allocated_compute_layer"] if "allocated_compute_layer" in r.keys() else None
            ) or layer_by_key.get(host) or ""

            def _delta(a, b):
                if a is None or b is None:
                    return ""
                try:
                    return round(float(b) - float(a), 3)
                except (TypeError, ValueError):
                    return ""

            w.writerow(
                {
                    "task_id": r["task_id"],
                    "status": r["status"],
                    "status_final": r["status_final"] or "",
                    "task_type": r["task_type"] or "",
                    "filename": r["filename"] or "",
                    "user_id": r["user_id"] or "",
                    "submitted_at_iso": _iso(sub),
                    "planning_completed_at_iso": _iso(plan_ts),
                    "dispatched_at_iso": _iso(disp_ts),
                    "completed_at_iso": _iso(comp),
                    "planning_s": _delta(sub, plan_ts),
                    "queue_wait_s": _delta(plan_ts, disp_ts),
                    "submit_to_dispatch_s": _delta(sub, disp_ts),
                    "latency_submit_to_complete_s": latency if latency is not None else "",
                    "duration_seconds": r["duration_seconds"] if r["duration_seconds"] is not None else "",
                    "allocated_node_id": alloc,
                    "allocated_hostname": host,
                    "compute_layer_node": layer_by_key.get(host) or layer_by_key.get(alloc) or "",
                    "preferred_layer": preferred or "",
                    "allocated_layer": alloc_layer or "",
                    "docker_image": r["docker_image"] or "",
                    "ai_category": r["ai_category"] or "",
                    "required_cpu_threads": r["required_cpu_threads"] if r["required_cpu_threads"] is not None else "",
                    "required_memory_gb": r["required_memory_gb"] if r["required_memory_gb"] is not None else "",
                    "required_disk_gb": r["required_disk_gb"] if r["required_disk_gb"] is not None else "",
                    "cpu_peak_percent": r["cpu_peak_percent"] if r["cpu_peak_percent"] is not None else "",
                    "ram_peak_mb": r["ram_peak_mb"] if r["ram_peak_mb"] is not None else "",
                    "prediction_delta_ram": r["prediction_delta_ram"] if r["prediction_delta_ram"] is not None else "",
                    "reallocation_attempts": realloc.get("attempts") or 0,
                    "excluded_hostnames": ",".join(realloc.get("excluded_hostnames") or []),
                    "detected_libraries": r["detected_libraries"] or "",
                }
            )

    summary_path = out_dir / f"tcc_metrics_audit_{stamp}.json"
    summary = audit(conn)
    summary["csv"] = str(csv_path)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return csv_path


def main(argv: list[str]) -> int:
    db = Path(argv[1]) if len(argv) > 1 else DEFAULT_DB
    if not db.is_file():
        print(f"DB não encontrado: {db}", file=sys.stderr)
        return 1
    conn = _connect(db)
    summary = audit(conn)
    print("=== AUDITORIA MÉTRICAS TCC ===")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    csv_path = export_rows(conn, OUT_DIR)
    print(f"\nCSV exportado: {csv_path}")
    print(f"JSON: {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
