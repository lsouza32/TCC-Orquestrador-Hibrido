import time
import requests

from flask import Blueprint, jsonify, request

from context_server import config
from context_server.database import crud
from context_server.queue_processor import process_queue_job
from context_server.logging_config import get_logger, log_success, log_error_with_context

nodes_bp = Blueprint('nodes_routes', __name__)
logger = get_logger(__name__)


def _heartbeat_fresh(node: dict, now: float) -> bool:
    last_reg = node.get("last_registered_at")
    max_age = float(getattr(config, "NODE_LAST_REGISTER_MAX_AGE_SECONDS", 65))
    try:
        return last_reg is not None and (now - float(last_reg)) <= max_age
    except (TypeError, ValueError):
        return False


# --- Modulo de Registro de Nos (API Endpoint) ---
@nodes_bp.route('/register_node', methods=['POST'])
def register_node():
    data = request.json
    required_fields = ['node_id', 'ip_address', 'agent_port',
                       'cpu_total_threads', 'memory_total_gb', 'disk_total_gb']
    if not data or not all(k in data for k in required_fields):
        return jsonify({"message": "Dados incompletos para registro. Campos esperados: node_id, ip_address, agent_port, cpu_total_threads, memory_total_gb, disk_total_gb."}), 400

    node_id = data['node_id']
    ip_address = data['ip_address']
    agent_port = data['agent_port']
    hostname = data.get('hostname', None)
    cpu_total_threads = data['cpu_total_threads']
    memory_total_gb = data['memory_total_gb']
    disk_total_gb = data['disk_total_gb']
    compute_layer = data.get('compute_layer')

    try:
        crud.register_or_update_node_static_info(
            config.DATABASE_PATH,
            node_id, ip_address, agent_port, hostname, cpu_total_threads,
            memory_total_gb, disk_total_gb, time.time(), True, "Registrado/Ativo",
            compute_layer=compute_layer,
        )
        log_success(logger, f"No '{node_id}' registrado/atualizado", f"IP: {ip_address}, Porta: {agent_port}")
        # Processa a fila de tarefas sempre que um no envia heartbeat (pode haver tarefas para alocar)
        try:
            process_queue_job(config.DATABASE_PATH)
        except Exception as queue_err:
            log_error_with_context(logger, queue_err, "Processar fila apos registro de no")
        return jsonify({"message": f"No '{node_id}' registrado/atualizado com sucesso!"}), 200
    except Exception as e:
        log_error_with_context(logger, e, f"Registro de no {node_id}")
        return jsonify({"message": f"Erro ao registrar no: {e}"}), 500


# --- Modulo de Consulta de Nos (API Endpoints) ---
@nodes_bp.route('/nodes', methods=['GET'])
def get_nodes():
    try:
        nodes = crud.get_all_nodes(config.DATABASE_PATH)
        logger.debug(f"Consulta de todos os nos: {len(nodes)} encontrados")
        return jsonify(nodes), 200
    except Exception as e:
        log_error_with_context(logger, e, "Consulta de todos os nos")
        return jsonify({"message": f"Erro ao buscar nos: {e}"}), 500


@nodes_bp.route('/nodes/active', methods=['GET'])
def get_active_nodes():
    """Retorna nos marcados como ativos no banco (atualizados pelo job de monitoramento)."""
    try:
        nodes = crud.get_active_nodes(config.DATABASE_PATH)
        logger.debug(f"Consulta de nos ativos: {len(nodes)} encontrados")
        return jsonify(nodes), 200
    except Exception as e:
        log_error_with_context(logger, e, "Consulta de nos ativos")
        return jsonify({"message": f"Erro ao buscar nos ativos: {e}"}), 500


@nodes_bp.route('/nodes/status_live', methods=['GET'])
def get_nodes_status_live():
    """
    Consulta em tempo real o status de cada no registrado, chamando o endpoint /status
    do agente. Se o callback falhar mas houver heartbeat recente (/register_node),
    considera o no online (comum quando o IP anunciado e inacessivel do container).
    """
    try:
        nodes_db = crud.get_all_nodes(config.DATABASE_PATH)
        result = []
        now = time.time()

        for node in nodes_db:
            if (node.get("provider_type") or "node_agent") == "kubernetes":
                result.append({
                    **dict(node),
                    "is_reachable": True,
                    "live_status_message": node.get("current_status_message") or "Kubernetes (API)",
                    "live_checked_at": now,
                })
                continue

            ip = node.get('ip_address')
            port = node.get('agent_port')
            fresh = _heartbeat_fresh(node, now)

            if not ip or not port:
                result.append({
                    **dict(node),
                    "is_reachable": fresh,
                    "live_status_message": "Online (heartbeat)" if fresh else "IP ou porta ausente",
                    "live_checked_at": now,
                })
                continue

            url = config.build_agent_api_url(ip, port, '/status')
            try:
                resp = requests.get(url, timeout=4)
                if resp.status_code == 200:
                    data = resp.json()
                    hw = data.get("hardware_usage") or {}
                    result.append({
                        **dict(node),
                        "is_reachable": True,
                        "live_status_message": "Online",
                        "live_checked_at": now,
                        "cpu_available_percent": hw.get("cpu_available_percent"),
                        "memory_available_gb": hw.get("memory_available_gb"),
                        "disk_available_gb": hw.get("disk_available_gb"),
                        "cpu_used_percent": hw.get("cpu_used_percent"),
                        "memory_used_percent": hw.get("memory_used_percent"),
                        "disk_used_percent": hw.get("disk_used_percent"),
                        "reserved_resources": hw.get("reserved_resources"),
                    })
                elif fresh:
                    result.append({
                        **dict(node),
                        "is_reachable": True,
                        "live_status_message": f"Online (heartbeat; /status HTTP {resp.status_code})",
                        "live_checked_at": now,
                    })
                else:
                    result.append({
                        **dict(node),
                        "is_reachable": False,
                        "live_status_message": f"HTTP {resp.status_code}",
                        "live_checked_at": now,
                    })
            except requests.exceptions.Timeout:
                result.append({
                    **dict(node),
                    "is_reachable": fresh,
                    "live_status_message": (
                        "Online (heartbeat; callback Timeout)" if fresh else "Timeout"
                    ),
                    "live_checked_at": now,
                })
            except requests.exceptions.ConnectionError:
                result.append({
                    **dict(node),
                    "is_reachable": fresh,
                    "live_status_message": (
                        "Online (heartbeat; sem callback)" if fresh else "Sem conexao"
                    ),
                    "live_checked_at": now,
                })
            except Exception as e:
                result.append({
                    **dict(node),
                    "is_reachable": fresh,
                    "live_status_message": (
                        "Online (heartbeat)" if fresh else str(e)[:60]
                    ),
                    "live_checked_at": now,
                })

        logger.debug(
            "status_live: %s nos alcancaveis de %s",
            len([n for n in result if n.get('is_reachable')]),
            len(result),
        )
        return jsonify(result), 200
    except Exception as e:
        log_error_with_context(logger, e, "Consulta status_live dos nos")
        return jsonify({"message": f"Erro ao consultar nos em tempo real: {e}"}), 500
