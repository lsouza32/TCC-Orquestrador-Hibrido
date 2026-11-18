import json
import sqlite3
import time
from context_server.database.crud import get_db_connection
from context_server.services.k8s_quantity import (
    parse_cpu_quantity_to_cores,
    parse_memory_quantity_to_mb,
)

# Limiar de estrangulamento: pico real >= 85% do limite alocado.
NEAR_CEILING_RATIO = 0.85
# status_final considerados sucesso para taxa de sucesso de imagem.
_SUCCESS_STATUSES = frozenset({"concluida", "concluída", "success", "succeeded", "ok"})

# --- Funções CRUD para inserir Tarefas ---
def create_task(db_path, task_id, task_type, filename, file_path, status, submitted_at, user_id, allocated_node_id=None,
                required_cpu_threads=None, required_memory_gb=None, required_disk_gb=None, generated_requirements_txt=None,
                docker_image=None, ai_detected_libraries=None, ai_category=None, ai_raw_response=None,
                ai_confidence_score=None, ai_image_source=None, ai_catalog_match_score=None, ai_telemetry_samples=None):
    conn = get_db_connection(db_path)
    try:
        ai_libs = ai_detected_libraries if ai_detected_libraries is not None else ""
        if isinstance(ai_libs, (list, tuple)):
            ai_libs = json.dumps(list(ai_libs), ensure_ascii=False)
        conn.execute('''
            INSERT INTO tasks (
                task_id, task_type, filename, file_path, status, submitted_at, user_id, allocated_node_id,
                required_cpu_threads, required_memory_gb, required_disk_gb, generated_requirements_txt, docker_image,
                ai_detected_libraries, ai_category, ai_raw_response, ai_confidence_score,
                ai_image_source, ai_catalog_match_score, ai_telemetry_samples
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (task_id, task_type, filename, file_path, status, submitted_at, user_id, allocated_node_id,
              required_cpu_threads, required_memory_gb, required_disk_gb, generated_requirements_txt or "", docker_image or "",
              ai_libs, ai_category or "", ai_raw_response or "", ai_confidence_score,
              ai_image_source or "", ai_catalog_match_score, ai_telemetry_samples))
        conn.commit()
    except sqlite3.Error as e:
        conn.rollback()
        raise Exception(f"Erro no DB ao criar tarefa: {e}")
    finally:
        conn.close()
        
def update_task_planning(
    db_path,
    task_id,
    status,
    required_cpu_threads,
    required_memory_gb,
    required_disk_gb,
    generated_requirements_txt,
    docker_image,
    ai_detected_libraries,
    ai_category,
    ai_raw_response,
    ai_confidence_score=None,
    ai_image_source=None,
    ai_catalog_match_score=None,
    ai_telemetry_samples=None,
    preferred_compute_layer=None,
):
    """
    Atualiza planejamento técnico da tarefa após análise assíncrona da IA.
    Grava planning_completed_at para métrica de latência de planejamento.
    """
    conn = get_db_connection(db_path)
    try:
        ai_libs = ai_detected_libraries if ai_detected_libraries is not None else ""
        if isinstance(ai_libs, (list, tuple)):
            ai_libs = json.dumps(list(ai_libs), ensure_ascii=False)
        planning_ts = time.time()
        conn.execute(
            '''
            UPDATE tasks
            SET status = ?,
                required_cpu_threads = ?,
                required_memory_gb = ?,
                required_disk_gb = ?,
                generated_requirements_txt = ?,
                docker_image = ?,
                ai_detected_libraries = ?,
                ai_category = ?,
                ai_raw_response = ?,
                ai_confidence_score = ?,
                ai_image_source = ?,
                ai_catalog_match_score = ?,
                ai_telemetry_samples = ?,
                planning_completed_at = ?,
                preferred_compute_layer = COALESCE(?, preferred_compute_layer)
            WHERE task_id = ?
            ''',
            (
                status,
                required_cpu_threads,
                required_memory_gb,
                required_disk_gb,
                generated_requirements_txt or "",
                docker_image or "",
                ai_libs,
                ai_category or "",
                ai_raw_response or "",
                ai_confidence_score,
                ai_image_source or "",
                ai_catalog_match_score,
                ai_telemetry_samples,
                planning_ts,
                preferred_compute_layer,
                task_id,
            ),
        )
        conn.commit()
    except sqlite3.Error as e:
        conn.rollback()
        raise Exception(f"Erro no DB ao atualizar planejamento da tarefa {task_id}: {e}")
    finally:
        conn.close()


def update_task_allocation(
    db_path,
    task_id,
    status,
    allocated_node_id=None,
    *,
    allocated_compute_layer=None,
    preferred_compute_layer=None,
    mark_dispatched: bool = False,
):
    """
    Atualiza o status e o ID do nó alocado para uma tarefa.
    Quando mark_dispatched=True (ou status='executando'), grava dispatched_at.
    """
    conn = get_db_connection(db_path)
    try:
        sets = ["status = ?", "allocated_node_id = ?"]
        params = [status, allocated_node_id]
        if mark_dispatched or status == "executando":
            sets.append("dispatched_at = ?")
            params.append(time.time())
        if allocated_compute_layer is not None:
            sets.append("allocated_compute_layer = ?")
            params.append(allocated_compute_layer)
        if preferred_compute_layer is not None:
            sets.append("preferred_compute_layer = ?")
            params.append(preferred_compute_layer)
        params.append(task_id)
        conn.execute(
            f"UPDATE tasks SET {', '.join(sets)} WHERE task_id = ?",
            tuple(params),
        )
        conn.commit()
    except sqlite3.Error as e:
        conn.rollback()
        raise Exception(f"Erro no DB ao atualizar alocação da tarefa {task_id}: {e}")
    finally:
        conn.close()


def update_task_status(db_path, task_id, status):
    """
    Atualiza apenas o status de uma tarefa no banco de dados.
    
    Args:
        db_path (str): Caminho para o DB.
        task_id (str): ID da tarefa.
        status (str): Novo status da tarefa (e.g., 'executando', 'concluida', 'falha').
    Raises:
        Exception: Se ocorrer um erro durante a operação no DB.
    """
    conn = get_db_connection(db_path)
    try:
        conn.execute('''
            UPDATE tasks 
            SET status = ?
            WHERE task_id = ?
        ''', (status, task_id))
        conn.commit()
    except sqlite3.Error as e:
        conn.rollback()
        raise Exception(f"Erro no DB ao atualizar status da tarefa {task_id}: {e}")
    finally:
        conn.close()


def save_task_result(db_path, task_id, status_final, duration_seconds, stdout_log=None, stderr_log=None,
                     cpu_peak_percent=None, ram_peak_mb=None, detected_libraries=None,
                     prediction_delta_ram=None):
    """
    Salva o resultado final de uma tarefa na tabela task_results.
    
    Args:
        db_path (str): Caminho para o banco de dados.
        task_id (str): ID único da tarefa.
        status_final (str): Status final da tarefa ('concluida', 'falha', 'erro_agente').
        duration_seconds (float): Tempo total de execução da tarefa em segundos.
        stdout_log (str, optional): Saída padrão (logs de sucesso) do script.
        stderr_log (str, optional): Saída de erro padrão (logs de falha/erro) do sistema.
        cpu_peak_percent (float, optional): Pico de uso de CPU durante a tarefa.
        ram_peak_mb (float, optional): Pico de uso de memória RAM em MB.
        detected_libraries (str, optional): JSON ou string com bibliotecas detectadas pela IA.
        prediction_delta_ram (float, optional): Delta GB (RAM estimada IA − pico real).
    
    Raises:
        Exception: Se ocorrer um erro durante a operação no banco de dados.
    """
    conn = get_db_connection(db_path)
    try:
        completed_at = time.time()
        det_lib = detected_libraries if detected_libraries is not None else ""
        
        conn.execute('''
            INSERT INTO task_results (
                task_id, status_final, duration_seconds, stdout_log, stderr_log, completed_at,
                cpu_peak_percent, ram_peak_mb, detected_libraries, prediction_delta_ram
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (task_id, status_final, duration_seconds, stdout_log or "", stderr_log or "", completed_at,
              cpu_peak_percent, ram_peak_mb, det_lib, prediction_delta_ram))
        
        conn.commit()
        
    except sqlite3.Error as e:
        conn.rollback()
        raise Exception(f"Erro no DB ao salvar resultado da tarefa {task_id}: {e}")
    finally:
        conn.close()


def get_task_result(db_path, task_id):
    """
    Recupera o resultado de uma tarefa específica.
    
    Args:
        db_path (str): Caminho para o banco de dados.
        task_id (str): ID único da tarefa.
    
    Returns:
        dict: Dicionário com os dados do resultado da tarefa ou None se não encontrado.
    """
    conn = get_db_connection(db_path)
    try:
        cursor = conn.execute('''
            SELECT task_id, status_final, duration_seconds, stdout_log, stderr_log, completed_at,
                   cpu_peak_percent, ram_peak_mb, detected_libraries, prediction_delta_ram
            FROM task_results
            WHERE task_id = ?
        ''', (task_id,))
        
        result = cursor.fetchone()
        if result:
            return dict(result)
        return None
        
    except sqlite3.Error as e:
        raise Exception(f"Erro no DB ao buscar resultado da tarefa {task_id}: {e}")
    finally:
        conn.close()


def get_all_task_results(db_path):
    """
    Recupera todos os resultados de tarefas.
    
    Args:
        db_path (str): Caminho para o banco de dados.
    
    Returns:
        list: Lista de dicionários com os dados de todos os resultados de tarefas.
    """
    conn = get_db_connection(db_path)
    try:
        cursor = conn.execute('''
            SELECT task_id, status_final, duration_seconds, stdout_log, stderr_log, completed_at,
                   cpu_peak_percent, ram_peak_mb, detected_libraries, prediction_delta_ram
            FROM task_results
            ORDER BY completed_at DESC
        ''')
        
        results = cursor.fetchall()
        return [dict(row) for row in results]
        
    except sqlite3.Error as e:
        raise Exception(f"Erro no DB ao buscar todos os resultados de tarefas: {e}")
    finally:
        conn.close()


def get_user_tasks_with_results(db_path, user_id):
    """
    Recupera todas as tarefas de um usuário com seus resultados (se existirem).
    
    Args:
        db_path (str): Caminho para o banco de dados.
        user_id (str): ID do usuário.
    
    Returns:
        list: Lista de dicionários com os dados das tarefas e seus resultados.
    """
    conn = get_db_connection(db_path)
    try:
        cursor = conn.execute('''
            SELECT 
                t.task_id,
                t.task_type,
                t.filename,
                t.status,
                t.submitted_at,
                t.allocated_node_id,
                t.user_id,
                t.required_cpu_threads,
                t.required_memory_gb,
                t.required_disk_gb,
                t.docker_image,
                t.ai_detected_libraries,
                t.ai_category,
                t.ai_raw_response,
                t.ai_confidence_score,
                t.ai_image_source,
                t.ai_catalog_match_score,
                t.ai_telemetry_samples,
                tr.status_final,
                tr.duration_seconds,
                tr.stdout_log,
                tr.stderr_log,
                tr.completed_at,
                tr.cpu_peak_percent,
                tr.ram_peak_mb,
                tr.detected_libraries
            FROM tasks t
            LEFT JOIN task_results tr ON t.task_id = tr.task_id
            WHERE t.user_id = ?
            ORDER BY t.submitted_at DESC
        ''', (user_id,))
        
        results = cursor.fetchall()
        return [dict(row) for row in results]
        
    except sqlite3.Error as e:
        raise Exception(f"Erro no DB ao buscar tarefas do usuário {user_id}: {e}")
    finally:
        conn.close()


def get_task_with_requirements(db_path, task_id):
    conn = get_db_connection(db_path)
    try:
        cursor = conn.execute('''
            SELECT task_id, allocated_node_id,
                   COALESCE(required_cpu_threads, 0) AS required_cpu_threads,
                   COALESCE(required_memory_gb, 0) AS required_memory_gb,
                   COALESCE(required_disk_gb, 0) AS required_disk_gb
            FROM tasks
            WHERE task_id = ?
        ''', (task_id,))
        row = cursor.fetchone()
        return dict(row) if row else None
    except sqlite3.Error as e:
        raise Exception(f"Erro no DB ao buscar requisitos da tarefa {task_id}: {e}")
    finally:
        conn.close()


def get_task_by_id(db_path, task_id):
    """
    Recupera todos os campos de uma tarefa pelo task_id.
    """
    conn = get_db_connection(db_path)
    try:
        cursor = conn.execute('SELECT * FROM tasks WHERE task_id = ?', (task_id,))
        row = cursor.fetchone()
        return dict(row) if row else None
    except sqlite3.Error as e:
        raise Exception(f"Erro no DB ao buscar tarefa {task_id}: {e}")
    finally:
        conn.close()


def get_recent_task_results_for_calibration(db_path, limit=20):
    """
    Recupera resultados recentes com telemetria (cpu_peak, ram_peak, detected_libraries)
    para uso na calibração/estimativa de requisitos pela IA.
    """
    conn = get_db_connection(db_path)
    try:
        cursor = conn.execute('''
            SELECT task_id, cpu_peak_percent, ram_peak_mb, detected_libraries
            FROM task_results
            WHERE cpu_peak_percent IS NOT NULL AND ram_peak_mb IS NOT NULL
            ORDER BY completed_at DESC
            LIMIT ?
        ''', (limit,))
        return [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        raise Exception(f"Erro ao buscar histórico para calibração: {e}")
    finally:
        conn.close()


def get_completed_task_counts_by_node_for_image(db_path, docker_image: str) -> dict[str, int]:
    """
    Agrega quantas tarefas concluídas com sucesso rodaram em cada nó com a mesma docker_image.
    Usado como proxy de warm start / afinidade de imagem no ranking de nós.

    Normaliza ``allocated_node_id`` no formato ``k8s:{hostname}`` para a chave ``hostname``,
    alinhada ao ``node_id``/``hostname`` dos nós no NodeRanker.
    """
    if not docker_image or not str(docker_image).strip():
        return {}
    conn = get_db_connection(db_path)
    try:
        cursor = conn.execute(
            """
            SELECT t.allocated_node_id AS node_id, COUNT(*) AS cnt
            FROM tasks t
            INNER JOIN task_results tr ON tr.task_id = t.task_id
            WHERE t.allocated_node_id IS NOT NULL
              AND t.docker_image = ?
              AND tr.status_final = 'concluida'
            GROUP BY t.allocated_node_id
            """,
            (str(docker_image).strip(),),
        )
        out: dict[str, int] = {}
        for row in cursor.fetchall():
            nid = row["node_id"]
            if not nid:
                continue
            raw = str(nid).strip()
            # k8s:{hostname} → hostname; UUID legado k8s:{uuid} também normaliza (warm fraco).
            key = raw[4:] if raw.startswith("k8s:") else raw
            key = key.strip()
            if not key:
                continue
            out[key] = out.get(key, 0) + int(row["cnt"])
            # Mantém também a chave bruta para compatibilidade.
            if raw != key:
                out[raw] = out.get(raw, 0) + int(row["cnt"])
        return out
    except sqlite3.Error as e:
        raise Exception(f"Erro ao agregar warm start por nó/imagem: {e}")
    finally:
        conn.close()


def enqueue_task_for_reallocation(db_path, task_id: str, ai_raw_response: str) -> None:
    """
    Devolve a tarefa à fila após falha de execução/agendamento.
    Limpa allocated_node_id e persiste estado de realocação em ai_raw_response.
    """
    if not task_id:
        return
    attempts = 0
    try:
        data = json.loads(ai_raw_response or "{}")
        if isinstance(data, dict):
            block = data.get("reallocation") or {}
            if isinstance(block, dict):
                attempts = int(block.get("attempts") or 0)
    except (json.JSONDecodeError, TypeError, ValueError):
        attempts = 0

    conn = get_db_connection(db_path)
    try:
        conn.execute(
            '''
            UPDATE tasks
            SET status = ?,
                allocated_node_id = NULL,
                ai_raw_response = ?,
                reallocation_attempts = ?,
                dispatched_at = NULL,
                allocated_compute_layer = NULL
            WHERE task_id = ?
            ''',
            ("na_fila", ai_raw_response or "", attempts, task_id),
        )
        conn.commit()
    except sqlite3.Error as e:
        conn.rollback()
        raise Exception(f"Erro no DB ao reenfileirar tarefa {task_id}: {e}")
    finally:
        conn.close()


def persist_effective_k8s_dispatch(
    db_path,
    task_id: str,
    effective_spec: dict,
    *,
    required_cpu_threads=None,
    required_memory_gb=None,
    dispatch_meta=None,
):
    """
    Persiste o k8s_job_spec efetivamente despachado (nodeSelector + limits pós-clamp)
    em ai_raw_response, para near_ceiling / RL usarem os limites reais do Job.
    Opcionalmente alinha required_cpu_threads / required_memory_gb aos limits aplicados.
    """
    if not task_id or not isinstance(effective_spec, dict):
        return
    conn = get_db_connection(db_path)
    try:
        cursor = conn.execute(
            "SELECT ai_raw_response FROM tasks WHERE task_id = ?",
            (task_id,),
        )
        row = cursor.fetchone()
        raw = (row["ai_raw_response"] if row else "") or ""
        try:
            data = json.loads(raw) if str(raw).strip() else {}
        except (TypeError, json.JSONDecodeError):
            data = {}
        if not isinstance(data, dict):
            data = {}

        data["k8s_job_spec"] = dict(effective_spec)
        trace = data.get("multi_agent_trace")
        if not isinstance(trace, dict):
            trace = {}
        summ = trace.get("strategist_summary")
        if not isinstance(summ, dict):
            summ = {}
        summ["k8s_job_spec"] = dict(effective_spec)
        if dispatch_meta is not None:
            summ["dispatch_meta"] = dispatch_meta
            data["dispatch_meta"] = dispatch_meta
        trace["strategist_summary"] = summ
        data["multi_agent_trace"] = trace

        new_raw = json.dumps(data, ensure_ascii=False)
        sets = ["ai_raw_response = ?"]
        params = [new_raw]
        if required_cpu_threads is not None:
            sets.append("required_cpu_threads = ?")
            params.append(float(required_cpu_threads))
        if required_memory_gb is not None:
            sets.append("required_memory_gb = ?")
            params.append(float(required_memory_gb))
        params.append(task_id)
        conn.execute(
            f"UPDATE tasks SET {', '.join(sets)} WHERE task_id = ?",
            tuple(params),
        )
        conn.commit()
    except sqlite3.Error as e:
        conn.rollback()
        raise Exception(f"Erro ao persistir k8s_job_spec efetivo da tarefa {task_id}: {e}")
    finally:
        conn.close()


def get_protected_docker_images_for_node(db_path, node_id: str) -> set[str]:
    """
    Imagens que não devem ser removidas: tarefas ainda não concluídas/canceladas neste nó.
    """
    if not node_id or not str(node_id).strip():
        return set()
    conn = get_db_connection(db_path)
    try:
        cursor = conn.execute(
            """
            SELECT DISTINCT TRIM(t.docker_image) AS docker_image
            FROM tasks t
            WHERE t.allocated_node_id = ?
              AND TRIM(COALESCE(t.docker_image, '')) != ''
              AND t.status IN (
                  'na_fila', 'pendente', 'alocada', 'em_execucao',
                  'analisando_ia', 'executando', 'em_execução'
              )
            """,
            (str(node_id).strip(),),
        )
        return {str(row["docker_image"]).strip() for row in cursor.fetchall() if row["docker_image"]}
    except sqlite3.Error as e:
        raise Exception(f"Erro ao listar imagens protegidas no nó {node_id!r}: {e}")
    finally:
        conn.close()


def get_stale_docker_images_for_node(db_path, node_id: str, stale_seconds: float) -> list[str]:
    """
    Imagens (docker_image) usadas neste nó cuja última tarefa concluída com sucesso
    foi há mais de stale_seconds.
    """
    if not node_id or not str(node_id).strip():
        return []
    cutoff = float(time.time()) - float(stale_seconds)
    conn = get_db_connection(db_path)
    try:
        cursor = conn.execute(
            """
            SELECT t.docker_image AS docker_image
            FROM tasks t
            INNER JOIN task_results tr ON tr.task_id = t.task_id
            WHERE t.allocated_node_id = ?
              AND TRIM(COALESCE(t.docker_image, '')) != ''
              AND tr.status_final = 'concluida'
            GROUP BY t.docker_image
            HAVING MAX(tr.completed_at) < ?
            """,
            (str(node_id).strip(), cutoff),
        )
        return [str(row["docker_image"]).strip() for row in cursor.fetchall() if row["docker_image"]]
    except sqlite3.Error as e:
        raise Exception(f"Erro ao listar imagens obsoletas no nó {node_id!r}: {e}")
    finally:
        conn.close()


def get_tasks_in_queue(db_path):
    """
    Recupera tarefas com status 'na_fila' (ordem por submitted_at) para processamento.
    Retorna lista de dicts com todos os campos da tabela tasks.
    """
    conn = get_db_connection(db_path)
    try:
        cursor = conn.execute(
            'SELECT * FROM tasks WHERE status = ? ORDER BY submitted_at ASC',
            ('na_fila',)
        )
        return [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        raise Exception(f"Erro no DB ao buscar fila: {e}")
    finally:
        conn.close()


def _parse_libs_text(raw):
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x).strip().lower() for x in raw if str(x).strip()]
    text = str(raw).strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [str(x).strip().lower() for x in parsed if str(x).strip()]
    except json.JSONDecodeError:
        pass
    tokens = [p.strip().lower() for p in text.replace(";", ",").split(",")]
    return [t for t in tokens if t]


def _percentile(values, pct):
    if not values:
        return None
    sorted_vals = sorted(float(v) for v in values)
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    k = (len(sorted_vals) - 1) * (pct / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    if f == c:
        return sorted_vals[f]
    d0 = sorted_vals[f] * (c - k)
    d1 = sorted_vals[c] * (k - f)
    return d0 + d1


def get_docker_images_catalog(db_path):
    conn = get_db_connection(db_path)
    try:
        cursor = conn.execute(
            '''
            SELECT id, image_name, libraries, min_ram_gb, min_cpu, description
            FROM docker_images_catalog
            ORDER BY min_ram_gb DESC, min_cpu DESC, image_name ASC
            '''
        )
        rows = [dict(r) for r in cursor.fetchall()]
        for row in rows:
            row["libraries_list"] = _parse_libs_text(row.get("libraries"))
        return rows
    except sqlite3.Error as e:
        raise Exception(f"Erro ao buscar catálogo Docker: {e}")
    finally:
        conn.close()


def find_catalog_candidates_for_libraries(db_path, libraries, limit=5):
    libs = [str(x).strip().lower() for x in (libraries or []) if str(x).strip()]
    catalog = get_docker_images_catalog(db_path)
    if not libs:
        return []

    # Map de aliases comuns para garantir match entre imports AST e tags no catálogo
    alias_map = {
        "cv2": {"cv2", "opencv-python", "opencv-python-headless"},
        "opencv-python": {"cv2", "opencv-python", "opencv-python-headless"},
        "opencv-python-headless": {"cv2", "opencv-python", "opencv-python-headless"},
        "sklearn": {"sklearn", "scikit-learn"},
        "scikit-learn": {"sklearn", "scikit-learn"},
        "pil": {"pil", "pillow"},
        "pillow": {"pil", "pillow"},
        "bs4": {"bs4", "beautifulsoup4"},
        "beautifulsoup4": {"bs4", "beautifulsoup4"},
    }

    target = set()
    for lib in libs:
        target.update(alias_map.get(lib, {lib}))

    ranked = []
    for row in catalog:
        image_libs = set(row.get("libraries_list") or [])
        if not image_libs:
            continue
        overlap = target.intersection(image_libs)
        if not overlap:
            continue
        score = len(overlap) / max(len(libs), 1)
        ranked.append({
            "image_name": row["image_name"],
            "libraries": row.get("libraries_list", []),
            "min_ram_gb": row.get("min_ram_gb"),
            "min_cpu": row.get("min_cpu"),
            "description": row.get("description"),
            "matched_libraries": sorted(list(overlap)),
            "match_score": round(score, 4),
        })

    ranked.sort(
        key=lambda x: (
            x["match_score"],
            float(x.get("min_ram_gb") or 0),
            float(x.get("min_cpu") or 0),
        ),
        reverse=True,
    )
    return ranked[:limit]


def get_telemetry_summary_for_libraries(db_path, libraries, limit=120):
    libs = {str(x).strip().lower() for x in (libraries or []) if str(x).strip()}
    if not libs:
        return {
            "samples": 0,
            "cpu_peak_percent_avg": None,
            "cpu_peak_percent_p95": None,
            "ram_peak_mb_avg": None,
            "ram_peak_mb_p95": None,
            "matched_task_ids": [],
        }

    conn = get_db_connection(db_path)
    try:
        cursor = conn.execute(
            '''
            SELECT t.task_id, t.ai_detected_libraries, tr.detected_libraries, tr.cpu_peak_percent, tr.ram_peak_mb
            FROM tasks t
            INNER JOIN task_results tr ON tr.task_id = t.task_id
            WHERE tr.cpu_peak_percent IS NOT NULL OR tr.ram_peak_mb IS NOT NULL
            ORDER BY tr.completed_at DESC
            LIMIT ?
            ''',
            (limit,),
        )
        rows = cursor.fetchall()
        cpu_vals = []
        ram_vals = []
        matched_ids = []
        for row in rows:
            row_dict = dict(row)
            libs_from_results = set(_parse_libs_text(row_dict.get("detected_libraries")))
            libs_from_plan = set(_parse_libs_text(row_dict.get("ai_detected_libraries")))
            merged = libs_from_results.union(libs_from_plan)
            if not merged.intersection(libs):
                continue
            matched_ids.append(row_dict.get("task_id"))
            if row_dict.get("cpu_peak_percent") is not None:
                cpu_vals.append(float(row_dict["cpu_peak_percent"]))
            if row_dict.get("ram_peak_mb") is not None:
                ram_vals.append(float(row_dict["ram_peak_mb"]))

        cpu_avg = round(sum(cpu_vals) / len(cpu_vals), 2) if cpu_vals else None
        ram_avg = round(sum(ram_vals) / len(ram_vals), 2) if ram_vals else None
        return {
            "samples": len(matched_ids),
            "cpu_peak_percent_avg": cpu_avg,
            "cpu_peak_percent_p95": round(_percentile(cpu_vals, 95), 2) if cpu_vals else None,
            "ram_peak_mb_avg": ram_avg,
            "ram_peak_mb_p95": round(_percentile(ram_vals, 95), 2) if ram_vals else None,
            "matched_task_ids": matched_ids[:20],
        }
    except sqlite3.Error as e:
        raise Exception(f"Erro ao buscar telemetria por bibliotecas: {e}")
    finally:
        conn.close()


def _extract_k8s_limits_from_ai_raw(ai_raw_response):
    """
    Extrai cpu_limit/memory_limit do JSON em ai_raw_response (raiz ou multi_agent_trace).
    Retorna (cpu_limit_raw, memory_limit_raw) ou (None, None).
    """
    raw = ai_raw_response or ""
    if not str(raw).strip():
        return None, None
    try:
        data = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return None, None
    if not isinstance(data, dict):
        return None, None
    spec = data.get("k8s_job_spec")
    if not isinstance(spec, dict):
        trace = data.get("multi_agent_trace") or {}
        if isinstance(trace, dict):
            summ = trace.get("strategist_summary") or trace.get("strategist_output") or {}
            if isinstance(summ, dict):
                spec = summ.get("k8s_job_spec")
    if not isinstance(spec, dict):
        return None, None
    return spec.get("cpu_limit"), spec.get("memory_limit")


def _is_success_status(status_final):
    s = str(status_final or "").strip().lower()
    return s in _SUCCESS_STATUSES


def get_task_result_samples_for_libraries(db_path, libraries, limit=25):
    """
    Amostras brutas de task_results (com join em tasks) filtradas por interseção
    de bibliotecas com as libs informadas. Usado pelo agente Librarian (somente dados reais do DB).
    """
    libs = {str(x).strip().lower() for x in (libraries or []) if str(x).strip()}
    if not libs:
        return []

    conn = get_db_connection(db_path)
    try:
        cursor = conn.execute(
            '''
            SELECT t.task_id, t.ai_detected_libraries, t.docker_image, t.ai_raw_response,
                   tr.detected_libraries, tr.cpu_peak_percent, tr.ram_peak_mb,
                   tr.status_final, tr.completed_at
            FROM tasks t
            INNER JOIN task_results tr ON tr.task_id = t.task_id
            WHERE tr.cpu_peak_percent IS NOT NULL OR tr.ram_peak_mb IS NOT NULL
               OR tr.status_final IS NOT NULL
            ORDER BY tr.completed_at DESC
            LIMIT ?
            ''',
            (max(limit * 4, 80),),
        )
        rows = cursor.fetchall()
        samples = []
        for row in rows:
            row_dict = dict(row)
            libs_from_results = set(_parse_libs_text(row_dict.get("detected_libraries")))
            libs_from_plan = set(_parse_libs_text(row_dict.get("ai_detected_libraries")))
            merged = libs_from_results.union(libs_from_plan)
            if not merged.intersection(libs):
                continue
            cpu_lim_raw, mem_lim_raw = _extract_k8s_limits_from_ai_raw(row_dict.get("ai_raw_response"))
            mem_lim_mb = parse_memory_quantity_to_mb(mem_lim_raw)
            cpu_lim_cores = parse_cpu_quantity_to_cores(cpu_lim_raw)
            samples.append({
                "task_id": row_dict.get("task_id"),
                "docker_image": row_dict.get("docker_image") or "",
                "status_final": row_dict.get("status_final"),
                "cpu_peak_percent": row_dict.get("cpu_peak_percent"),
                "ram_peak_mb": row_dict.get("ram_peak_mb"),
                "cpu_limit": cpu_lim_raw,
                "memory_limit": mem_lim_raw,
                "cpu_limit_cores": cpu_lim_cores,
                "memory_limit_mb": mem_lim_mb,
                "detected_libraries": row_dict.get("detected_libraries"),
                "ai_detected_libraries": row_dict.get("ai_detected_libraries"),
                "completed_at": row_dict.get("completed_at"),
            })
            if len(samples) >= limit:
                break
        return samples
    except sqlite3.Error as e:
        raise Exception(f"Erro ao buscar amostras task_results por bibliotecas: {e}")
    finally:
        conn.close()


def get_docker_image_success_stats_for_libraries(db_path, libraries, limit=200):
    """
    Taxa de sucesso por docker_image entre execuções cujo conjunto de bibliotecas
    intersecta as libs informadas (isoladas ou em conjunto).
    """
    libs = {str(x).strip().lower() for x in (libraries or []) if str(x).strip()}
    if not libs:
        return []

    conn = get_db_connection(db_path)
    try:
        cursor = conn.execute(
            '''
            SELECT t.task_id, t.docker_image, t.ai_detected_libraries,
                   tr.detected_libraries, tr.status_final, tr.completed_at
            FROM tasks t
            INNER JOIN task_results tr ON tr.task_id = t.task_id
            WHERE tr.status_final IS NOT NULL
            ORDER BY tr.completed_at DESC
            LIMIT ?
            ''',
            (max(int(limit), 50),),
        )
        by_image = {}
        for row in cursor.fetchall():
            row_dict = dict(row)
            libs_from_results = set(_parse_libs_text(row_dict.get("detected_libraries")))
            libs_from_plan = set(_parse_libs_text(row_dict.get("ai_detected_libraries")))
            merged = libs_from_results.union(libs_from_plan)
            if not merged.intersection(libs):
                continue
            image = str(row_dict.get("docker_image") or "").strip() or "(sem_imagem)"
            bucket = by_image.setdefault(image, {"docker_image": image, "successes": 0, "total": 0})
            bucket["total"] += 1
            if _is_success_status(row_dict.get("status_final")):
                bucket["successes"] += 1

        ranked = []
        for bucket in by_image.values():
            total = int(bucket["total"])
            successes = int(bucket["successes"])
            rate = (successes / total) if total else 0.0
            ranked.append({
                "docker_image": bucket["docker_image"],
                "successes": successes,
                "total": total,
                "success_rate": round(rate, 4),
            })
        ranked.sort(key=lambda x: (x["success_rate"], x["total"], x["successes"]), reverse=True)
        return ranked
    except sqlite3.Error as e:
        raise Exception(f"Erro ao calcular success_rate por imagem/libs: {e}")
    finally:
        conn.close()


def get_resource_ceiling_signals_for_libraries(db_path, libraries, limit=120, near_ratio=None):
    """
    Compara picos reais (cpu_peak_percent, ram_peak_mb) com limites alocados no
    k8s_job_spec histórico. Marca near_ceiling quando pico >= near_ratio do limite.

    CPU: cpu_peak_percent já é relativo ao limit do container — near se >= near_ratio*100.
    RAM: ram_peak_mb vs memory_limit parseado para MB (via k8s_quantity).
    Amostras sem limit parseável são excluídas do sinal de teto (não assumem teto).
    """
    ratio = float(NEAR_CEILING_RATIO if near_ratio is None else near_ratio)
    libs = {str(x).strip().lower() for x in (libraries or []) if str(x).strip()}
    empty = {
        "samples": 0,
        "near_ceiling_count": 0,
        "near_ceiling_ratio": 0.0,
        "near_ceiling": False,
        "cpu_near_ceiling_count": 0,
        "ram_near_ceiling_count": 0,
        "samples_detail": [],
    }
    if not libs:
        return empty

    conn = get_db_connection(db_path)
    try:
        cursor = conn.execute(
            '''
            SELECT t.task_id, t.docker_image, t.ai_detected_libraries, t.ai_raw_response,
                   tr.detected_libraries, tr.cpu_peak_percent, tr.ram_peak_mb,
                   tr.status_final, tr.completed_at
            FROM tasks t
            INNER JOIN task_results tr ON tr.task_id = t.task_id
            WHERE tr.cpu_peak_percent IS NOT NULL OR tr.ram_peak_mb IS NOT NULL
            ORDER BY tr.completed_at DESC
            LIMIT ?
            ''',
            (max(int(limit), 40),),
        )
        details = []
        near_count = 0
        cpu_near = 0
        ram_near = 0
        evaluable = 0
        for row in cursor.fetchall():
            row_dict = dict(row)
            libs_from_results = set(_parse_libs_text(row_dict.get("detected_libraries")))
            libs_from_plan = set(_parse_libs_text(row_dict.get("ai_detected_libraries")))
            merged = libs_from_results.union(libs_from_plan)
            if not merged.intersection(libs):
                continue

            cpu_lim_raw, mem_lim_raw = _extract_k8s_limits_from_ai_raw(row_dict.get("ai_raw_response"))
            mem_lim_mb = parse_memory_quantity_to_mb(mem_lim_raw)
            cpu_peak = row_dict.get("cpu_peak_percent")
            ram_peak = row_dict.get("ram_peak_mb")

            cpu_near_flag = False
            ram_near_flag = False
            sample_evaluable = False

            # cpu_peak_percent é % do limit do pod/container — compara direto ao limiar.
            if cpu_peak is not None and cpu_lim_raw is not None and parse_cpu_quantity_to_cores(cpu_lim_raw) is not None:
                sample_evaluable = True
                try:
                    cpu_near_flag = float(cpu_peak) >= (ratio * 100.0)
                except (TypeError, ValueError):
                    cpu_near_flag = False

            if ram_peak is not None and mem_lim_mb is not None and mem_lim_mb > 0:
                sample_evaluable = True
                try:
                    ram_near_flag = float(ram_peak) >= (ratio * float(mem_lim_mb))
                except (TypeError, ValueError):
                    ram_near_flag = False

            if not sample_evaluable:
                continue

            evaluable += 1
            near_flag = bool(cpu_near_flag or ram_near_flag)
            if near_flag:
                near_count += 1
            if cpu_near_flag:
                cpu_near += 1
            if ram_near_flag:
                ram_near += 1

            details.append({
                "task_id": row_dict.get("task_id"),
                "docker_image": row_dict.get("docker_image") or "",
                "cpu_peak_percent": cpu_peak,
                "ram_peak_mb": ram_peak,
                "cpu_limit": cpu_lim_raw,
                "memory_limit": mem_lim_raw,
                "memory_limit_mb": mem_lim_mb,
                "near_ceiling": near_flag,
                "cpu_near_ceiling": cpu_near_flag,
                "ram_near_ceiling": ram_near_flag,
            })
            if len(details) >= 40:
                break

        near_ratio_out = (near_count / evaluable) if evaluable else 0.0
        return {
            "samples": evaluable,
            "near_ceiling_count": near_count,
            "near_ceiling_ratio": round(near_ratio_out, 4),
            "near_ceiling": near_count > 0,
            "cpu_near_ceiling_count": cpu_near,
            "ram_near_ceiling_count": ram_near,
            "threshold_ratio": ratio,
            "samples_detail": details[:20],
        }
    except sqlite3.Error as e:
        raise Exception(f"Erro ao calcular sinais de teto de recursos: {e}")
    finally:
        conn.close()