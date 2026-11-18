import io
import json
import os
import queue
import threading
import time
import uuid
import zipfile
import requests
from flask import Blueprint, Response, jsonify, request, send_file, stream_with_context
from werkzeug.utils import secure_filename

# Importações de módulos do projeto
from context_server import config
from context_server.database import crud
from context_server.logging_config import get_logger, log_task_submission, log_node_allocation, log_error_with_context, log_success
from context_server.realtime import sse_hub
from context_server.services.task_export import export_task_result
from context_server.services.ai_service import analyze_task_requirements
from context_server.services.tcc_metrics import compute_prediction_delta_ram, log_execucao_fim, log_planejamento
from context_server.services.k8s_job_executor import delete_kubernetes_job_for_task_id, is_kubernetes_allocation
from context_server.database import crud as db_crud
from context_server.queue_processor import process_queue_job

# Blueprint para agrupar rotas de tarefas; registre-o no app principal:
#   app.register_blueprint(tasks_bp)
tasks_bp = Blueprint("tasks_routes", __name__)

logger = get_logger(__name__)


def _parse_libraries_list(raw) -> list:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x) for x in raw]
    s = str(raw).strip()
    if not s:
        return []
    try:
        v = json.loads(s)
        return [str(x) for x in v] if isinstance(v, list) else []
    except json.JSONDecodeError:
        return []


def _publish_task_update(user_id: str, task_id: str, status: str, **extra) -> None:
    payload = {"type": "task_update", "task_id": task_id, "status": status}
    payload.update(extra)
    sse_hub.publish(user_id, payload)


def _run_ai_analysis_in_background(task_id: str, user_id: str, task_type: str, filename: str, file_path: str, fallback_requirements: dict) -> None:
    """
    Executa análise de IA em thread e, ao finalizar, coloca a tarefa na fila.
    """
    cpu_threads = fallback_requirements.get("cpu_threads", fallback_requirements.get("required_cpu_threads", 2.0))
    ram_gb = fallback_requirements.get("ram_gb", fallback_requirements.get("required_memory_gb", 0.5))
    disk_gb = fallback_requirements.get("disk_gb", fallback_requirements.get("required_disk_gb", 1.0))
    docker_image = fallback_requirements.get("docker_image", "python:3.9-slim")
    ai_detected_libraries_raw = "[]"
    ai_category = "geral"
    ai_raw_response = ""
    ai_confidence_score = None
    ai_image_source = "fallback_initial"
    ai_catalog_match_score = None
    ai_telemetry_samples = None
    used_ai_fallback = True

    script_content = ""
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            script_content = f.read()
    except Exception as e:
        logger.warning("Não foi possível ler script %s para análise da IA: %s", task_id, e)

    try:
        if script_content:
            history = []
            try:
                import ast as _ast
                libs_ast = []
                try:
                    tree = _ast.parse(script_content or "")
                    seen = set()
                    for node in _ast.walk(tree):
                        if isinstance(node, _ast.Import):
                            for alias in node.names:
                                name = (alias.name or "").split(".")[0].strip()
                                if name and name not in seen:
                                    seen.add(name)
                                    libs_ast.append(name)
                        elif isinstance(node, _ast.ImportFrom):
                            module = (node.module or "").split(".")[0].strip()
                            if module and module not in seen:
                                seen.add(module)
                                libs_ast.append(module)
                except SyntaxError:
                    libs_ast = []
                if libs_ast:
                    history = db_crud.get_task_result_samples_for_libraries(
                        config.DATABASE_PATH, libs_ast, limit=20
                    )
                if not history:
                    history = db_crud.get_recent_task_results_for_calibration(
                        config.DATABASE_PATH, limit=20
                    )
            except Exception as hist_err:
                logger.warning("Histórico de calibração indisponível: %s", hist_err)
                history = db_crud.get_recent_task_results_for_calibration(config.DATABASE_PATH, limit=20)
            ai_result = analyze_task_requirements(script_content, history=history, task_id=task_id)
            requirements_txt = ai_result.get("requirements_txt") or ""
            cpu_threads = ai_result.get("suggested_cpu_threads", cpu_threads)
            ram_gb = ai_result.get("suggested_ram_gb", ram_gb)
            disk_gb = ai_result.get("suggested_disk_gb", disk_gb)
            docker_image = ai_result.get("docker_image", docker_image)
            ai_detected_libraries_raw = ai_result.get("detected_libraries") or "[]"
            ai_category = str(ai_result.get("category") or "geral")
            ai_raw_response = ai_result.get("ai_raw_response") or ""
            ai_confidence_score = ai_result.get("confidence_score")
            ai_image_source = ai_result.get("image_source") or "ai"
            ai_catalog_match_score = ai_result.get("catalog_match_score")
            ai_telemetry_samples = ai_result.get("telemetry_samples")
            used_ai_fallback = bool(ai_result.get("used_fallback", True))
            if used_ai_fallback:
                logger.warning("⚠️ IA indisponível para tarefa %s; usando fallback.", task_id)
                log_planejamento(
                    task_id,
                    ai_detected_libraries_raw,
                    ai_category,
                    ai_raw_response=ai_raw_response,
                )
            else:
                logger.info(
                    "🤖 IA finalizada para %s | categoria=%s CPU=%s RAM=%s disco=%s imagem=%s",
                    task_id, ai_category, cpu_threads, ram_gb, disk_gb, docker_image
                )
                reasoning = (ai_result.get("agent_reasoning") or "").strip()
                if reasoning:
                    logger.info("🤖 [%s] Síntese dos agentes (Analista/Librarian/Strategist):\n%s", task_id, reasoning)
                raw_ai = ai_result.get("ai_raw_response") or ""
                if raw_ai:
                    try:
                        blob = json.loads(raw_ai)
                        trace = blob.get("multi_agent_trace")
                        if trace:
                            logger.info(
                                "🤖 [%s] JSON por agente (multi_agent_trace):\n%s",
                                task_id,
                                json.dumps(trace, ensure_ascii=False, indent=2),
                            )
                    except json.JSONDecodeError:
                        pass

        preferred_layer = None
        try:
            from context_server.services.compute_layer import preferred_layer_for_workload

            preferred_layer = preferred_layer_for_workload(
                task_type=ai_category,
                ai_category=ai_category,
                required_memory_gb=float(ram_gb or 0.5),
                required_cpu_threads=float(cpu_threads or 1.0),
            )
            # Tenta complexity do multi_agent_trace se existir
            if ai_raw_response:
                try:
                    blob = json.loads(ai_raw_response)
                    trace = (blob.get("multi_agent_trace") or {}) if isinstance(blob, dict) else {}
                    analyst = (trace.get("analyst_output") or {}) if isinstance(trace, dict) else {}
                    if isinstance(analyst, dict):
                        preferred_layer = preferred_layer_for_workload(
                            task_type=analyst.get("task_type") or ai_category,
                            ai_category=ai_category,
                            complexity_score=analyst.get("complexity_score"),
                            required_memory_gb=float(ram_gb or 0.5),
                            required_cpu_threads=float(cpu_threads or 1.0),
                        )
                except (json.JSONDecodeError, TypeError, ValueError):
                    pass
        except Exception as layer_err:
            logger.debug("preferred_compute_layer indisponível: %s", layer_err)

        db_crud.update_task_planning(
            config.DATABASE_PATH,
            task_id,
            "na_fila",
            cpu_threads,
            ram_gb,
            disk_gb,
            requirements_txt,
            docker_image,
            ai_detected_libraries_raw,
            ai_category,
            ai_raw_response,
            ai_confidence_score=ai_confidence_score,
            ai_image_source=ai_image_source,
            ai_catalog_match_score=ai_catalog_match_score,
            ai_telemetry_samples=ai_telemetry_samples,
            preferred_compute_layer=preferred_layer,
        )

        detected_libraries_list = _parse_libraries_list(ai_detected_libraries_raw)
        _publish_task_update(
            user_id,
            task_id,
            "na_fila",
            filename=filename,
            task_type=task_type,
            required_cpu_threads=cpu_threads,
            required_memory_gb=ram_gb,
            required_disk_gb=disk_gb,
            docker_image=docker_image,
            ai_detected_libraries=detected_libraries_list,
            ai_category=ai_category,
            ai_confidence_score=ai_confidence_score,
            ai_image_source=ai_image_source,
            ai_catalog_match_score=ai_catalog_match_score,
            ai_telemetry_samples=ai_telemetry_samples,
            used_ai_fallback=used_ai_fallback,
        )

        try:
            process_queue_job(config.DATABASE_PATH)
        except Exception as queue_err:
            log_error_with_context(logger, queue_err, f"Processar fila após análise da tarefa {task_id}")
    except Exception as e:
        log_error_with_context(logger, e, f"Análise em background da tarefa {task_id}")
        try:
            db_crud.update_task_status(config.DATABASE_PATH, task_id, "falha_alocacao")
            _publish_task_update(user_id, task_id, "falha_alocacao")
        except Exception as update_error:
            log_error_with_context(logger, update_error, f"Atualizar falha da tarefa {task_id} após erro de análise")


@tasks_bp.route("/submit_task", methods=["POST"])
def submit_task():
    """
    Submete uma tarefa para execução distribuída.

    Todas as tarefas entram na fila (status 'na_fila'). A fila é processada
    quando o broker recebe o heartbeat (registro) de um nó em /register_node,
    que tenta alocar e executar as tarefas em espera.
    """
    logger.info("📋 Requisição para /submit_task recebida")

    # --- 1) Validação básica do formulário ---
    if "file" not in request.files or "userId" not in request.form:
        logger.warning("Dados incompletos na requisição de submissão")
        return jsonify({"message": "Dados incompletos na requisição."}), 400

    file = request.files["file"]
    # taskType opcional: se não enviado ou "auto", a IA define requisitos; fallback usa config "auto"
    task_type = (request.form.get("taskType") or "auto").strip().lower() or "auto"
    if task_type not in config.TASK_REQUIREMENTS:
        task_type = "auto"
    user_id = request.form["userId"]

    if file.filename == "":
        logger.warning("Nenhum arquivo selecionado para a submissão")
        return jsonify({"message": "Nenhum arquivo selecionado."}), 400

    # --- 2) Preparação de metadados e persistência do arquivo ---
    task_id = str(uuid.uuid4())
    filename = secure_filename(file.filename)

    # Garante que a pasta de uploads exista (idempotente)
    os.makedirs(config.UPLOAD_FOLDER, exist_ok=True)

    # Salva no disco com prefixo do task_id para evitar colisões
    file_path_on_context_server = os.path.join(
        config.UPLOAD_FOLDER, f"{task_id}_{filename}"
    )
    file.save(file_path_on_context_server)

    # --- 3) Validação de tipo (fallback de requisitos se a IA falhar) ---
    fallback_requirements = config.TASK_REQUIREMENTS.get(task_type)
    if not fallback_requirements:
        logger.error(f"Tipo de tarefa '{task_type}' desconhecido. Requisitos não encontrados.")
        return jsonify({"message": f"Tipo de tarefa '{task_type}' desconhecido."}), 400
    fallback_requirements = fallback_requirements.copy()

    # Log estruturado da submissão
    log_task_submission(logger, task_id, task_type, filename)

    # --- 4) Registro imediato da tarefa (não bloqueia com IA) ---
    submitted_at = time.time()
    fallback_cpu = fallback_requirements["cpu_threads"]
    fallback_ram = fallback_requirements["ram_gb"]
    fallback_disk = fallback_requirements["disk_gb"]
    fallback_image = fallback_requirements.get("docker_image", "python:3.9-slim")
    try:
        crud.create_task(
            config.DATABASE_PATH,
            task_id,
            task_type,
            filename,
            file_path_on_context_server,
            "analisando_ia",
            submitted_at,
            user_id,
            required_cpu_threads=fallback_cpu,
            required_memory_gb=fallback_ram,
            required_disk_gb=fallback_disk,
            generated_requirements_txt="",
            docker_image=fallback_image,
            ai_detected_libraries="[]",
            ai_category="geral",
            ai_raw_response="",
            ai_confidence_score=None,
            ai_image_source="pending_analysis",
            ai_catalog_match_score=None,
            ai_telemetry_samples=None,
        )
        logger.info("Tarefa %s registrada para análise assíncrona da IA.", task_id)
    except Exception as e:
        log_error_with_context(logger, e, f"Salvar tarefa {task_id} no banco de dados")
        return jsonify({"message": f"Erro interno ao processar a tarefa: {e}"}), 500

    _publish_task_update(
        user_id,
        task_id,
        "analisando_ia",
        filename=filename,
        task_type=task_type,
        submitted_at=submitted_at,
        required_cpu_threads=fallback_cpu,
        required_memory_gb=fallback_ram,
        required_disk_gb=fallback_disk,
        docker_image=fallback_image,
        ai_detected_libraries=[],
        ai_category="geral",
        ai_confidence_score=None,
        ai_image_source="pending_analysis",
        ai_catalog_match_score=None,
        ai_telemetry_samples=None,
    )

    thread = threading.Thread(
        target=_run_ai_analysis_in_background,
        args=(task_id, user_id, task_type, filename, file_path_on_context_server, fallback_requirements),
        daemon=True,
    )
    thread.start()

    return jsonify(
        {
            "message": "Tarefa recebida. A análise de IA e alocação serão processadas em segundo plano.",
            "task_id": task_id,
            "task_type": task_type,
            "filename": filename,
            "status": "analisando_ia",
            "submitted_at": submitted_at,
            "required_cpu_threads": fallback_cpu,
            "required_memory_gb": fallback_ram,
            "required_disk_gb": fallback_disk,
            "docker_image": fallback_image,
            "detected_libraries": [],
            "category": "geral",
            "confidence_score": None,
            "image_source": "pending_analysis",
            "catalog_match_score": None,
            "telemetry_samples": None,
            "used_ai_fallback": None,
        }
    ), 200


@tasks_bp.route("/task_completed", methods=["POST"])
def task_completed():
    """
    Endpoint para o Node Agent reportar o resultado final de uma tarefa.
    
    Recebe os dados de conclusão da tarefa e salva na tabela task_results.
    
    JSON esperado:
    {
        "task_id": "uuid-da-tarefa",
        "status_final": "concluida|falha|erro_agente",
        "duration_seconds": 1.2,
        "stdout_log": "Saída padrão do script (opcional)",
        "stderr_log": "Saída de erro do script (opcional)",
        "cpu_peak_percent": 85.5,
        "ram_peak_mb": 512.0,
        "detected_libraries": "[\"numpy\", \"pandas\"]"
    }
    
    Retorna JSON com confirmação do salvamento.
    """
    logger.info("📋 Requisição para /task_completed recebida")
    
    try:
        # Validação dos dados recebidos
        data = request.json
        if not data:
            logger.warning("Dados JSON não fornecidos na requisição de finalização")
            return jsonify({"message": "Dados JSON não fornecidos."}), 400
        
        # Campos obrigatórios
        required_fields = ['task_id', 'status_final', 'duration_seconds']
        if not all(field in data for field in required_fields):
            missing_fields = [field for field in required_fields if field not in data]
            logger.warning(f"Campos obrigatórios ausentes: {missing_fields}")
            return jsonify({
                "message": f"Campos obrigatórios ausentes: {', '.join(missing_fields)}"
            }), 400
        
        task_id = data['task_id']
        status_final = data['status_final']
        duration_seconds = data['duration_seconds']
        stdout_log = data.get('stdout_log')
        stderr_log = data.get('stderr_log')
        cpu_peak_percent = data.get('cpu_peak_percent')
        ram_peak_mb = data.get('ram_peak_mb')
        detected_libraries = data.get('detected_libraries')
        if detected_libraries is not None and not isinstance(detected_libraries, str):
            detected_libraries = json.dumps(detected_libraries) if detected_libraries else ""

        task_row_libs = crud.get_task_by_id(config.DATABASE_PATH, task_id)
        if (not detected_libraries or not str(detected_libraries).strip()) and task_row_libs:
            ai_libs = task_row_libs.get("ai_detected_libraries")
            if ai_libs:
                detected_libraries = (
                    ai_libs
                    if isinstance(ai_libs, str)
                    else json.dumps(ai_libs, ensure_ascii=False)
                )
        
        # Validação dos valores
        if status_final not in ['concluida', 'falha', 'erro_agente', 'cancelada']:
            logger.warning(f"Status final inválido: {status_final}")
            return jsonify({
                "message": f"Status final inválido: {status_final}. Valores aceitos: concluida, falha, erro_agente, cancelada"
            }), 400
        
        if not isinstance(duration_seconds, (int, float)) or duration_seconds < 0:
            logger.warning(f"Duração inválida: {duration_seconds}")
            return jsonify({
                "message": "Duração deve ser um número positivo"
            }), 400
        
        task_row_metrics = crud.get_task_by_id(config.DATABASE_PATH, task_id)
        suggested_ram = (task_row_metrics or {}).get("required_memory_gb")
        delta_ram = compute_prediction_delta_ram(suggested_ram, ram_peak_mb)
        log_execucao_fim(
            task_id,
            status_final,
            duration_seconds,
            cpu_peak_percent=cpu_peak_percent,
            ram_peak_mb=ram_peak_mb,
            detected_libraries=detected_libraries,
            suggested_ram_gb=suggested_ram,
            prediction_delta_ram=delta_ram,
        )

        # Salva o resultado da tarefa no banco de dados (incluindo telemetria do container)
        crud.save_task_result(
            config.DATABASE_PATH,
            task_id,
            status_final,
            duration_seconds,
            stdout_log or "",
            stderr_log or "",
            cpu_peak_percent=cpu_peak_percent,
            ram_peak_mb=ram_peak_mb,
            detected_libraries=detected_libraries,
            prediction_delta_ram=delta_ram,
        )
        
        # Atualiza o status da tarefa na tabela tasks
        crud.update_task_status(config.DATABASE_PATH, task_id, status_final)

        task_row_pub = crud.get_task_by_id(config.DATABASE_PATH, task_id)
        if task_row_pub and task_row_pub.get("user_id"):
            _publish_task_update(
                task_row_pub["user_id"],
                task_id,
                status_final,
                duration_seconds=duration_seconds,
            )

        # Libera reservas SQLite apenas para execução legada via Node Agent (não Kubernetes)
        task_record = crud.get_task_with_requirements(config.DATABASE_PATH, task_id)
        if task_record and task_record.get("allocated_node_id") and not is_kubernetes_allocation(
            task_record.get("allocated_node_id")
        ):
            try:
                crud.release_node_resources(
                    config.DATABASE_PATH,
                    task_record["allocated_node_id"],
                    task_record["required_cpu_threads"],
                    task_record["required_memory_gb"],
                    task_record["required_disk_gb"],
                )
            except Exception as release_error:
                log_error_with_context(
                    logger,
                    release_error,
                    f"Liberar recursos do nó {task_record['allocated_node_id']} para tarefa {task_id}",
                )

        # Exportação: apenas para concluída/falha/erro (não para cancelada)
        if status_final not in ("cancelada",):
            try:
                task_row = crud.get_task_by_id(config.DATABASE_PATH, task_id)
                result_row = crud.get_task_result(config.DATABASE_PATH, task_id)
                if task_row and result_row:
                    export_task_result(config.EXPORTS_BASE, task_id, task_row, result_row)
            except Exception as export_err:
                log_error_with_context(logger, export_err, f"Exportar resultado da tarefa {task_id}")
        
        # Log estruturado do resultado
        logger.info(f"✅ RESULTADO SALVO | Tarefa: {task_id} | Status: {status_final} | Duração: {duration_seconds}s")
        
        if stdout_log:
            logger.debug(f"📤 Saída da tarefa {task_id}: {stdout_log[:100]}...")
        
        if stderr_log:
            logger.debug(f"🚨 Erro da tarefa {task_id}: {stderr_log[:100]}...")
        
        return jsonify({
            "message": "Resultado da tarefa salvo com sucesso.",
            "task_id": task_id,
            "status_final": status_final,
            "duration_seconds": duration_seconds,
            "telemetry_ack": True,
            "cpu_peak_percent": cpu_peak_percent,
            "ram_peak_mb": ram_peak_mb,
        }), 200
        
    except Exception as e:
        log_error_with_context(logger, e, f"Salvar resultado da tarefa {data.get('task_id', 'desconhecida')}")
        return jsonify({
            "message": f"Erro interno ao salvar resultado da tarefa: {str(e)}"
        }), 500


@tasks_bp.route("/user_tasks/stream", methods=["GET"])
def user_tasks_stream():
    """
    SSE: eventos `task_update` para o usuário autenticado no query param user_id.
    """
    user_id = request.args.get("user_id")
    if not user_id:
        return jsonify({"message": "user_id é obrigatório."}), 400

    q = sse_hub.subscribe(user_id)

    def generate():
        try:
            yield "data: " + json.dumps({"type": "connected"}, ensure_ascii=False) + "\n\n"
            while True:
                try:
                    msg = q.get(timeout=25)
                    yield f"data: {msg}\n\n"
                except queue.Empty:
                    yield ": ping\n\n"
        finally:
            sse_hub.unsubscribe(user_id, q)

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@tasks_bp.route("/user_tasks", methods=["GET"])
def get_user_tasks():
    """
    Endpoint para listar todas as tarefas de um usuário específico.
    
    Query Parameters:
        user_id (str): ID do usuário para filtrar as tarefas.
    
    Retorna JSON com lista de tarefas e seus resultados (se existirem).
    """
    logger.info("📋 Requisição para /user_tasks recebida")
    
    try:
        user_id = request.args.get('user_id')
        if not user_id:
            logger.warning("user_id não fornecido na requisição")
            return jsonify({"message": "user_id é obrigatório."}), 400
        
        # Busca todas as tarefas do usuário com seus resultados
        tasks = crud.get_user_tasks_with_results(config.DATABASE_PATH, user_id)
        
        logger.info(f"📊 Encontradas {len(tasks)} tarefas para o usuário {user_id}")
        
        # Formata os dados para resposta
        formatted_tasks = []
        for task in tasks:
            ai_libs = _parse_libraries_list(task.get("ai_detected_libraries"))
            formatted_task = {
                "task_id": task['task_id'],
                "task_type": task['task_type'],
                "filename": task['filename'],
                "status": task['status'],
                "submitted_at": task['submitted_at'],
                "allocated_node_id": task['allocated_node_id'],
                "user_id": task.get("user_id"),
                "required_cpu_threads": task.get("required_cpu_threads"),
                "required_memory_gb": task.get("required_memory_gb"),
                "required_disk_gb": task.get("required_disk_gb"),
                "docker_image": task.get("docker_image"),
                "ai_detected_libraries": ai_libs,
                "ai_category": task.get("ai_category"),
                "ai_raw_response": task.get("ai_raw_response"),
                "ai_confidence_score": task.get("ai_confidence_score"),
                "ai_image_source": task.get("ai_image_source"),
                "ai_catalog_match_score": task.get("ai_catalog_match_score"),
                "ai_telemetry_samples": task.get("ai_telemetry_samples"),
                "result": None
            }
            
            # Se há resultado, adiciona os dados do resultado
            if task['status_final']:
                formatted_task['result'] = {
                    "status_final": task['status_final'],
                    "duration_seconds": task['duration_seconds'],
                    "stdout_log": task['stdout_log'],
                    "stderr_log": task['stderr_log'],
                    "completed_at": task['completed_at'],
                    "cpu_peak_percent": task.get('cpu_peak_percent'),
                    "ram_peak_mb": task.get('ram_peak_mb'),
                    "detected_libraries": task.get('detected_libraries'),
                }
            
            formatted_tasks.append(formatted_task)
        
        return jsonify({
            "message": f"Tarefas do usuário {user_id} recuperadas com sucesso.",
            "user_id": user_id,
            "total_tasks": len(formatted_tasks),
            "tasks": formatted_tasks
        }), 200
        
    except Exception as e:
        log_error_with_context(logger, e, f"Buscar tarefas do usuário {request.args.get('user_id', 'desconhecido')}")
        return jsonify({
            "message": f"Erro interno ao buscar tarefas do usuário: {str(e)}"
        }), 500


@tasks_bp.route("/cancel_task", methods=["POST"])
def cancel_task():
    """
    Cancela uma tarefa (na fila, pendente, alocada ou em execução).
    Body: task_id, user_id. Se estiver executando, pede ao nó para interromper.
    """
    data = request.get_json() or {}
    task_id = data.get("task_id")
    user_id = data.get("user_id")
    if not task_id or not user_id:
        return jsonify({"message": "task_id e user_id são obrigatórios."}), 400

    task_row = crud.get_task_by_id(config.DATABASE_PATH, task_id)
    if not task_row:
        return jsonify({"message": "Tarefa não encontrada."}), 404
    if task_row.get("user_id") != user_id:
        return jsonify({"message": "Acesso negado a esta tarefa."}), 403

    status = task_row.get("status")
    if status in ("concluida", "falha", "erro_agente", "cancelada", "falha_alocacao", "falha_envio"):
        return jsonify({"message": f"Tarefa já está em estado final ({status})."}), 400

    if status in ("analisando_ia", "na_fila", "pendente", "alocada"):
        crud.update_task_status(config.DATABASE_PATH, task_id, "cancelada")
        _publish_task_update(user_id, task_id, "cancelada")
        if status == "alocada" and task_row.get("allocated_node_id") and not is_kubernetes_allocation(
            task_row.get("allocated_node_id")
        ):
            node_id = task_row.get("allocated_node_id")
            try:
                crud.release_node_resources(
                    config.DATABASE_PATH,
                    node_id,
                    task_row.get("required_cpu_threads") or 0,
                    task_row.get("required_memory_gb") or 0,
                    task_row.get("required_disk_gb") or 0,
                )
            except Exception as e:
                log_error_with_context(logger, e, f"Liberar recursos ao cancelar tarefa {task_id}")
        logger.info(f"Tarefa {task_id} cancelada pelo usuário (status era {status}).")
        return jsonify({"message": "Tarefa cancelada.", "task_id": task_id}), 200

    if status == "executando":
        alloc = task_row.get("allocated_node_id")
        if is_kubernetes_allocation(alloc):
            delete_kubernetes_job_for_task_id(task_id)
        elif alloc:
            node_info = crud.get_node_details(config.DATABASE_PATH, alloc)
            if node_info:
                cancel_url = config.build_agent_api_url(
                    node_info["ip_address"], node_info["agent_port"], "/cancel_task"
                )
                try:
                    r = requests.post(cancel_url, json={"task_id": task_id}, timeout=5)
                    if r.status_code != 200:
                        logger.warning(f"Nó {alloc} respondeu {r.status_code} ao cancelar tarefa {task_id}")
                except Exception as e:
                    log_error_with_context(logger, e, f"Chamar nó para cancelar tarefa {task_id}")
        crud.update_task_status(config.DATABASE_PATH, task_id, "cancelada")
        _publish_task_update(user_id, task_id, "cancelada")
        if alloc and not is_kubernetes_allocation(alloc):
            try:
                crud.release_node_resources(
                    config.DATABASE_PATH,
                    alloc,
                    task_row.get("required_cpu_threads") or 0,
                    task_row.get("required_memory_gb") or 0,
                    task_row.get("required_disk_gb") or 0,
                )
            except Exception as e:
                log_error_with_context(logger, e, f"Liberar recursos ao cancelar tarefa {task_id}")
        logger.info(f"Tarefa {task_id} cancelada pelo usuário (estava executando).")
        return jsonify({"message": "Tarefa cancelada. O nó foi solicitado a interromper a execução.", "task_id": task_id}), 200

    return jsonify({"message": "Estado da tarefa não permite cancelamento."}), 400


@tasks_bp.route("/task_artifacts/<task_id>", methods=["POST"])
def upload_task_artifacts(task_id):
    """
    Recebe arquivos de artefato (ex.: matriz de confusão PNG) enviados pelo nó após conclusão da tarefa.
    Os arquivos são salvos em exports_base/task_id/ e passam a fazer parte do ZIP de exportação.
    """
    task_row = crud.get_task_by_id(config.DATABASE_PATH, task_id)
    if not task_row:
        return jsonify({"message": "Tarefa não encontrada."}), 404

    exports_base = getattr(config, "EXPORTS_BASE", None)
    if not exports_base:
        exports_base = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "exports")
    task_dir = os.path.join(exports_base, task_id)
    try:
        os.makedirs(task_dir, exist_ok=True)
    except OSError as e:
        log_error_with_context(logger, e, f"Criar diretório de artefatos para tarefa {task_id}")
        return jsonify({"message": "Erro ao criar diretório de exportação."}), 500

    if "files" not in request.files and "file" not in request.files:
        return jsonify({"message": "Nenhum arquivo enviado."}), 400

    uploaded = []
    files = request.files.getlist("files") if "files" in request.files else [request.files.get("file")]
    for f in files:
        if not f or f.filename == "":
            continue
        name = secure_filename(f.filename)
        if not name:
            continue
        path = os.path.join(task_dir, name)
        try:
            f.save(path)
            uploaded.append(name)
        except OSError as e:
            log_error_with_context(logger, e, f"Salvar artefato {name} para tarefa {task_id}")

    if uploaded:
        logger.info("Artefatos da tarefa %s recebidos: %s", task_id, uploaded)
    return jsonify({"message": "Artefatos recebidos.", "uploaded": uploaded}), 200


@tasks_bp.route("/task_export/<task_id>", methods=["GET"])
def download_task_export(task_id):
    """
    Retorna um ZIP com os arquivos de exportação da tarefa (relatório, planilha, stdout/stderr e artefatos como matriz de confusão).
    Query: user_id (obrigatório) para validar que a tarefa pertence ao usuário.
    """
    user_id = request.args.get("user_id")
    if not user_id:
        return jsonify({"message": "user_id é obrigatório."}), 400

    task_row = crud.get_task_by_id(config.DATABASE_PATH, task_id)
    if not task_row:
        return jsonify({"message": "Tarefa não encontrada."}), 404
    if task_row.get("user_id") != user_id:
        return jsonify({"message": "Acesso negado a esta tarefa."}), 403

    result_row = crud.get_task_result(config.DATABASE_PATH, task_id)
    if not result_row:
        return jsonify({"message": "Tarefa sem resultado para exportar."}), 404

    exports_base = getattr(config, "EXPORTS_BASE", None)
    if not exports_base:
        exports_base = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "exports")
    task_dir = os.path.join(exports_base, task_id)

    if not os.path.isdir(task_dir):
        try:
            export_task_result(config.EXPORTS_BASE, task_id, task_row, result_row)
        except Exception as e:
            log_error_with_context(logger, e, f"Gerar exportação da tarefa {task_id}")
            return jsonify({"message": "Falha ao gerar exportação."}), 500
        if not os.path.isdir(task_dir):
            return jsonify({"message": "Exportação não disponível."}), 404

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name in os.listdir(task_dir):
            path = os.path.join(task_dir, name)
            if os.path.isfile(path):
                with open(path, "rb") as f:
                    zf.writestr(name, f.read())
    buf.seek(0)
    safe_id = secure_filename(task_id)[:36] or "tarefa"
    return send_file(
        buf,
        mimetype="application/zip",
        as_attachment=True,
        download_name=f"tarefa_{safe_id}.zip",
    )
