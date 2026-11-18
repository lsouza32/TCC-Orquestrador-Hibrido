import time
import requests
import os
from werkzeug.utils import secure_filename

from context_server import config
from context_server.database import crud
from context_server.logging_config import get_logger, log_error_with_context
from context_server.services.k8s_job_executor import extract_k8s_job_spec
from context_server.services.k8s_quantity import parse_cpu_quantity_to_cores, parse_memory_quantity_to_gb

logger = get_logger(__name__)

# --- Lógica de Alocação de Nó (Função Principal) ---
def allocate_node_to_task(task_id, task_type, task_requirements=None):
    """
    task_requirements: dict opcional com cpu_threads, ram_gb, disk_gb (da IA ou da tarefa).
    Se não for passado, usa config.TASK_REQUIREMENTS[task_type].
    """
    logger.debug(f"Iniciando algoritmo de alocação para a tarefa {task_id}")
    
    if not task_requirements:
        task_requirements = config.TASK_REQUIREMENTS.get(task_type)
    if not task_requirements:
        logger.error(f"Tipo de tarefa '{task_type}' desconhecido. Requisitos não encontrados.")
        return None, None
    task_requirements = dict(task_requirements)

    db_path = config.DATABASE_PATH
    now = time.time()
    active_nodes = crud.get_active_nodes(db_path)
    if not active_nodes:
        logger.warning("Nenhum nó ativo encontrado para alocação")
        return None, None
        
    for node in active_nodes:
        node_id = node['node_id']
        node_ip = node['ip_address']
        agent_port = node['agent_port']
        
        total_threads = node.get('cpu_total_threads') or 0
        total_ram_gb = node.get('memory_total_gb') or 0
        total_disk_gb = node.get('disk_total_gb') or 0

        if (total_threads < task_requirements['cpu_threads'] or
            total_ram_gb < task_requirements['ram_gb'] or
            total_disk_gb < task_requirements['disk_gb']):
            logger.debug(f"Nó {node_id} descartado por capacidade total insuficiente")
            continue
        
        # 1. Obter recursos disponíveis em tempo real do nó
        agent_status_url = config.build_agent_api_url(node_ip, agent_port, '/status')
        try:
            response = requests.get(agent_status_url, timeout=3)
            if response.status_code != 200:
                # Se o nó está "na lista", mas não responde corretamente, não vale continuar
                # tentando alocação com ele neste ciclo.
                logger.warning(f"Nó {node_id} respondeu status HTTP {response.status_code} em {agent_status_url}")
                continue

            try:
                agent_data = response.json()
            except ValueError:
                logger.warning(f"Nó {node_id}: /status retornou JSON inválido")
                continue

            hardware_usage = agent_data.get("hardware_usage", {})
            available_cpu_percent = hardware_usage.get("cpu_available_percent")
            available_ram_gb = hardware_usage.get("memory_available_gb")
            available_disk_gb = hardware_usage.get("disk_available_gb")
            
            if None in (available_cpu_percent, available_ram_gb, available_disk_gb):
                logger.warning(f"Nó {node_id} respondeu status incompleto: {hardware_usage}")
                continue  # tenta próximo nó
            
            try:
                available_threads = (float(total_threads) * float(available_cpu_percent)) / 100.0
            except (TypeError, ValueError):
                logger.warning(f"Nó {node_id}: não foi possível calcular available_threads")
                continue

            reserved_cpu = node.get('cpu_reserved_threads') or 0
            reserved_ram = node.get('memory_reserved_gb') or 0
            reserved_disk = node.get('disk_reserved_gb') or 0

            effective_threads = min(available_threads, max(total_threads - reserved_cpu, 0))
            effective_ram = min(available_ram_gb, max(total_ram_gb - reserved_ram, 0))
            effective_disk = min(available_disk_gb, max(total_disk_gb - reserved_disk, 0))

            # Log detalhado apenas em DEBUG
            logger.debug(
                f"Checando nó {node_id}: "
                f"CPU disp efetiva: {effective_threads:.2f}/{total_threads}, "
                f"RAM disp efetiva: {effective_ram}GB, Disco disp efetivo: {effective_disk}GB"
            )
            
            # 2. Aplicar a lógica de filtragem (First Fit simples)
            if (effective_threads >= task_requirements['cpu_threads'] and
                effective_ram >= task_requirements['ram_gb'] and
                effective_disk >= task_requirements['disk_gb']):
                
                logger.info(f"✅ Nó {node_id} atende aos requisitos! Alocando...")
                return node_id, task_requirements
                
        except requests.exceptions.RequestException as e:
            logger.warning(f"Erro ao obter status do agente {node_id}: {e}")
            # Para erros de conexão (ex.: connection refused), desativamos o nó
            # imediatamente para não ficar tentando em ciclos seguintes.
            if isinstance(e, requests.exceptions.ConnectionError):
                try:
                    crud.update_node_status(
                        db_path,
                        node_id,
                        False,
                        now,
                        f"Falha no GET /status: {str(e)[:80]}",
                    )
                except Exception:
                    # Não interrompe a alocação por causa disso.
                    logger.debug(f"Falha ao atualizar is_active=false para o nó {node_id}", exc_info=True)
                
    logger.warning("Nenhum nó disponível atende aos requisitos da tarefa")
    return None, None

# --- Função para enviar o arquivo da tarefa para o nó ---
def send_file_to_node(node_id, file_path_on_context_server, original_filename):
    node_info = crud.get_node_details(config.DATABASE_PATH, node_id)
    if not node_info:
        logger.error(f"Nó {node_id} não encontrado no banco de dados. Falha no envio do arquivo")
        return None
    
    file_upload_url = config.build_agent_api_url(node_info['ip_address'], node_info['agent_port'], '/upload_file')
    logger.debug(f"Enviando arquivo '{original_filename}' para o nó {node_id}")
    
    try:
        with open(file_path_on_context_server, 'rb') as f:
            files = {'file': (original_filename, f, 'application/octet-stream')}
            response = requests.post(file_upload_url, files=files, timeout=30)
            
            if response.status_code == 200:
                agent_response = response.json()
                logger.info(f"📁 Arquivo enviado com sucesso para o nó {node_id}")
                return agent_response.get('file_path')
            else:
                logger.error(f"Falha ao enviar arquivo para o nó {node_id}: {response.text}")
                return None
    except Exception as e:
        log_error_with_context(logger, e, f"Envio de arquivo para nó {node_id}")
        return None

def build_resource_requirements_for_node_agent(task_row: dict, base_requirements: dict) -> dict:
    """
    Mescla requisitos legados com hints do k8s_job_spec (imagem, CPU/RAM, bibliotecas IA).
    O node_agent mapeia cpu_threads/ram_gb/docker_image e limites opcionais para docker run.
    """
    rr = dict(base_requirements or {})
    if not isinstance(task_row, dict):
        return rr
    if task_row.get("docker_image"):
        rr["docker_image"] = task_row["docker_image"]
    ai_libs = task_row.get("ai_detected_libraries")
    if ai_libs:
        rr["detected_libraries"] = ai_libs
    k8s = extract_k8s_job_spec(task_row)
    if k8s.get("image"):
        rr["docker_image"] = k8s["image"]
    cpu_lim = parse_cpu_quantity_to_cores(k8s.get("cpu_limit"))
    if cpu_lim is not None:
        rr["cpu_limit"] = cpu_lim
    mem_lim = parse_memory_quantity_to_gb(k8s.get("memory_limit"))
    if mem_lim is not None:
        rr["memory_limit"] = f"{max(64, int(mem_lim * 1024))}m" if mem_lim < 1 else f"{int(mem_lim)}g"
    return rr


# --- Função para enviar as instruções de execução da tarefa para o nó ---
def send_task_to_node(node_id, task_id, filename_on_node, resource_requirements=None, requirements_txt=None, task_row=None):
    """
    Envia a requisição de execução para o Node Agent (POST /execute_task).

    Args:
        filename_on_node: Nome do arquivo no nó (ex: "script.py"), não o path completo.
        resource_requirements: dict com cpu_threads, ram_gb, disk_gb, docker_image.
        requirements_txt: Conteúdo do requirements.txt gerado pela IA (opcional).

    Payload enviado ao nó: task_id, command_string, filename_on_node, resource_requirements, requirements_txt.
    """
    
    node_info = crud.get_node_details(config.DATABASE_PATH, node_id)
    if not node_info:
        logger.error(f"Nó {node_id} não encontrado no banco de dados. Falha no envio da tarefa")
        return False

    # Nome do arquivo no nó (deve ser o mesmo com que o nó salvou no /upload_file)
    script_path_on_node = os.path.join("tasks", os.path.basename(filename_on_node)) 
    
    # Monta o comando final que o Agente irá executar.
    # O Agente saberá que este é o caminho base para montar o Docker.
    command_string_to_send = f"python3 {script_path_on_node}" 
    
    # 2. Montar a URL de destino
    execution_url = config.build_agent_api_url(node_info['ip_address'], node_info['agent_port'], '/execute_task')
    
    if task_row:
        resource_requirements = build_resource_requirements_for_node_agent(
            task_row, resource_requirements or {}
        )

    payload = {
        "task_id": task_id,
        "command_string": command_string_to_send, # O Agente usará isso para montar o 'docker run'
        "filename_on_node": filename_on_node,  # Mantido para compatibilidade
        "resource_requirements": resource_requirements,
        "requirements_txt": requirements_txt or "",
    }
    
    # Log da imagem Docker que será usada
    docker_image = resource_requirements.get('docker_image') if resource_requirements else None
    if docker_image:
        logger.info(f"🐳 Imagem Docker para tarefa {task_id}: {docker_image}")

    logger.debug(f"Enviando tarefa {task_id} para execução Docker no nó {node_id}")
    logger.debug(f"Comando Docker: {command_string_to_send}")

    try:
        response = requests.post(execution_url, json=payload, timeout=10)
        if response.status_code == 200:
            logger.info(f"🚀 Tarefa {task_id} enviada com sucesso para execução Docker no nó {node_id}")
            return True
        else:
            logger.error(f"Falha ao enviar a tarefa para o nó {node_id}: {response.text}")
            return False
    except requests.exceptions.RequestException as e:
        log_error_with_context(logger, e, f"Envio de tarefa para nó {node_id}")
        return False