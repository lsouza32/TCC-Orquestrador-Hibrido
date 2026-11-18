import json
import time

from flask import Blueprint, Response, jsonify, request, stream_with_context

from context_server import config
from context_server.database import crud
from context_server.logging_config import get_logger

admin_bp = Blueprint("admin_routes", __name__)
logger = get_logger(__name__)


def _parse_libraries(raw_value):
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        return [str(x) for x in raw_value]
    text = str(raw_value).strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [str(x) for x in parsed]
    except json.JSONDecodeError:
        return []
    return []


def _current_user_id():
    return (
        request.headers.get("X-User-Id")
        or request.args.get("user_id")
        or request.args.get("username")
    )


def _user_is_admin(user_id):
    if not user_id:
        return False
    user_access = (config.LOCAL_USERS_ACCESS or {}).get(user_id)
    if isinstance(user_access, dict):
        return bool(user_access.get("is_admin"))
    return bool(user_access)


def _require_admin():
    user_id = _current_user_id()
    if not user_id:
        return None, jsonify({"message": "Identificação de usuário ausente. Informe X-User-Id."}), 401
    if not _user_is_admin(user_id):
        return None, jsonify({"message": "Acesso negado. Perfil de administrador obrigatório."}), 403
    return user_id, None


def _safe_json_object(raw_text):
    if not raw_text:
        return {}
    try:
        parsed = json.loads(raw_text)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass
    return {}


def _clean_log(val):
    if not val:
        return ""
    if isinstance(val, str):
        if (val.startswith("b'") and val.endswith("'")) or (val.startswith('b"') and val.endswith('"')):
            try:
                import codecs
                content = val[2:-1]
                decoded = codecs.escape_decode(bytes(content, "utf-8"))[0].decode("utf-8", errors="replace")
                return decoded
            except Exception:
                pass
    return val


def _build_dashboard_data(limit=40):
    conn = crud.get_db_connection(config.DATABASE_PATH)
    try:
        rows = conn.execute(
            """
            SELECT
                t.task_id,
                t.status,
                t.user_id,
                t.submitted_at,
                t.allocated_node_id,
                t.required_cpu_threads,
                t.required_memory_gb,
                t.required_disk_gb,
                t.docker_image,
                t.preferred_compute_layer,
                t.allocated_compute_layer,
                t.reallocation_attempts,
                t.filename,
                t.task_type,
                t.ai_detected_libraries,
                t.ai_category,
                t.ai_raw_response,
                t.ai_confidence_score,
                t.ai_image_source,
                t.ai_catalog_match_score,
                t.ai_telemetry_samples,
                tr.status_final,
                tr.completed_at,
                tr.cpu_peak_percent,
                tr.ram_peak_mb,
                tr.detected_libraries,
                tr.stdout_log,
                tr.stderr_log
            FROM tasks t
            LEFT JOIN task_results tr ON tr.task_id = t.task_id
            ORDER BY t.submitted_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

        latest_tasks = []
        for row in rows:
            item = dict(row)
            required_ram_gb = item.get("required_memory_gb")
            required_ram_mb = (required_ram_gb or 0) * 1024
            ram_peak_mb = item.get("ram_peak_mb")
            cpu_peak_percent = item.get("cpu_peak_percent")

            # Heurística simples: marca sobredimensionado quando uso real fica abaixo de 60% do alocado.
            ram_ratio = None
            oversized = False
            if required_ram_mb and ram_peak_mb is not None:
                ram_ratio = float(ram_peak_mb) / float(required_ram_mb)
                oversized = ram_ratio < 0.6

            ai_raw = _safe_json_object(item.get("ai_raw_response"))
            ai_detected = _parse_libraries(item.get("detected_libraries")) or _parse_libraries(item.get("ai_detected_libraries"))
            ai_category = item.get("ai_category") or ai_raw.get("category") or "geral"
            if not ai_raw:
                ai_raw = {
                    "detected_libraries": ai_detected,
                    "category": ai_category,
                }

            latest_tasks.append(
                {
                    "task_id": item.get("task_id"),
                    "status": item.get("status"),
                    "user_id": item.get("user_id"),
                    "submitted_at": item.get("submitted_at"),
                    "completed_at": item.get("completed_at"),
                    "allocated_node_id": item.get("allocated_node_id"),
                    "filename": item.get("filename"),
                    "task_type": item.get("task_type"),
                    "docker_image": item.get("docker_image"),
                    "preferred_compute_layer": item.get("preferred_compute_layer"),
                    "allocated_compute_layer": item.get("allocated_compute_layer"),
                    "reallocation_attempts": item.get("reallocation_attempts"),
                    "reallocation": ai_raw.get("reallocation", {}),
                    "stdout_log": _clean_log(item.get("stdout_log")),
                    "stderr_log": _clean_log(item.get("stderr_log")),
                    "ai_analysis": {
                        "detected_libraries": ai_detected,
                        "category": ai_category,
                        "confidence_score": item.get("ai_confidence_score"),
                        "image_source": item.get("ai_image_source"),
                        "catalog_match_score": item.get("ai_catalog_match_score"),
                        "telemetry_samples": item.get("ai_telemetry_samples"),
                        "full_response": ai_raw,
                    },
                    "performance": {
                        "cpu_peak_percent": cpu_peak_percent,
                        "ram_peak_mb": ram_peak_mb,
                        "allocated_cpu_threads": item.get("required_cpu_threads"),
                        "allocated_ram_mb": required_ram_mb if required_ram_mb else None,
                        "ram_efficiency_ratio": ram_ratio,
                        "is_oversized": oversized,
                    },
                    "result_status": item.get("status_final"),
                }
            )
    finally:
        conn.close()

    active_nodes = crud.get_active_nodes(config.DATABASE_PATH)
    nodes_status = []
    for node in active_nodes:
        nodes_status.append(
            {
                "node_id": node.get("node_id"),
                "hostname": node.get("hostname"),
                "compute_layer": node.get("compute_layer"),
                "is_active": bool(node.get("is_active")),
                "resources": {
                    "cpu_total_threads": node.get("cpu_total_threads"),
                    "cpu_reserved_threads": node.get("cpu_reserved_threads"),
                    "memory_total_gb": node.get("memory_total_gb"),
                    "memory_reserved_gb": node.get("memory_reserved_gb"),
                    "disk_total_gb": node.get("disk_total_gb"),
                    "disk_reserved_gb": node.get("disk_reserved_gb"),
                },
            }
        )

    return {
        "generated_at": time.time(),
        "latest_tasks": latest_tasks,
        "nodes_status": nodes_status,
        "summary": {
            "tasks_returned": len(latest_tasks),
            "active_nodes": len(nodes_status),
        },
    }


@admin_bp.route("/admin/dashboard_data", methods=["GET"])
def admin_dashboard_data():
    _, error_response = _require_admin()
    if error_response is not None:
        return error_response
    try:
        return jsonify(_build_dashboard_data()), 200
    except Exception as exc:
        logger.exception("Erro ao montar /admin/dashboard_data: %s", exc)
        return jsonify({"message": f"Erro ao montar dashboard: {exc}"}), 500


@admin_bp.route("/admin/dashboard_stream", methods=["GET"])
def admin_dashboard_stream():
    _, error_response = _require_admin()
    if error_response is not None:
        return error_response

    def generate():
        yield "data: " + json.dumps({"type": "connected"}) + "\n\n"
        while True:
            payload = _build_dashboard_data(limit=25)
            yield "data: " + json.dumps({"type": "dashboard_update", "payload": payload}) + "\n\n"
            time.sleep(6)

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
