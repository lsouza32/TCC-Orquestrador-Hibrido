import subprocess  # Comandos externos (ping).
import time
import platform    # Para escolher sintaxe do ping (Windows vs Linux).
import requests

# Importa o módulo 'crud' do pacote 'context_server.database'.
# Isso permite acessar as funções de interação com o banco de dados que definimos em crud.py.
from context_server.database import crud 
from context_server.logging_config import get_logger, log_monitoring_summary, log_node_status
from context_server import config

# Configura o logger para este módulo
logger = get_logger(__name__, "WARNING")  # Nível WARNING para reduzir spam

def monitor_nodes_job(db_path):
    """
    Função principal do job de monitoramento de nós.
    Executada periodicamente pelo APScheduler no Context Server.
    
    Para cada nó:
    1. Faz um ping para verificar conectividade básica.
    2. Se o ping for OK, tenta obter recursos disponíveis do agente do nó via HTTP.
    3. Atualiza o banco de dados com o status de atividade e os recursos disponíveis.
    
    Args:
        db_path (str): O caminho completo para o arquivo do banco de dados SQLite.
    """
    try:
        # 1. Recupera todos os nós registrados no banco de dados.
        nodes_to_monitor = crud.get_all_nodes(db_path)
        total_nodes = len(nodes_to_monitor)
        active_nodes = 0 
        
        for node_row in nodes_to_monitor:
            node_id = node_row['node_id']       # ID único do nó.
            ip_address = node_row['ip_address'] # Endereço IP do nó.
            agent_port = node_row['agent_port'] # Porta do agente no nó.

            # Nós sincronizados pelo Watcher K8s: sem ping nem HTTP ao Node Agent
            if (node_row.get("provider_type") or "node_agent") == "kubernetes":
                continue

            is_active = False          # Status de atividade (inicialmente falso).
            status_message = "Desconhecido" # Mensagem de status detalhada.
            
            # Variáveis para recursos disponíveis, inicializadas como None (ou seus valores antigos do DB)
            # para que não sejam atualizadas se a checagem falhar.
            cpu_available_percent = None
            memory_available_gb = None
            disk_available_gb = None
            last_resource_check = None
            
            # --- 1. Ping para verificar conectividade (sintaxe Windows vs Linux) ---
            try:
                if platform.system() == 'Windows':
                    # Windows: -n 1 = um pacote, -w 2000 = timeout em ms
                    ping_command = ["ping", "-n", "1", "-w", "2000", ip_address]
                else:
                    # Linux/macOS: -c 1 = um pacote, -W 2 = timeout em segundos
                    ping_command = ["ping", "-c", "1", "-W", "2", ip_address]

                ping_output = subprocess.run(
                    ping_command,
                    capture_output=True,
                    text=True,
                    timeout=5,
                    check=False,
                )

                if ping_output.returncode == 0:
                    is_active = True
                    status_message = "Ativo (ping OK)"
                else:
                    # Ping falhou: ainda assim respeita heartbeat recente do /register_node
                    # (IP anunciado errado / NAT / broker em Docker sem rota de volta).
                    last_reg = node_row.get("last_registered_at")
                    max_age = float(getattr(config, "NODE_LAST_REGISTER_MAX_AGE_SECONDS", 65))
                    try:
                        if last_reg is not None and (time.time() - float(last_reg)) <= max_age:
                            is_active = True
                            status_message = "Ativo (heartbeat)"
                        else:
                            is_active = False
                            err = (ping_output.stderr or ping_output.stdout or "").strip() or "Ping falhou"
                            status_message = err[:80]
                    except (TypeError, ValueError):
                        is_active = False
                        err = (ping_output.stderr or ping_output.stdout or "").strip() or "Ping falhou"
                        status_message = err[:80]
            except subprocess.TimeoutExpired:
                last_reg = node_row.get("last_registered_at")
                max_age = float(getattr(config, "NODE_LAST_REGISTER_MAX_AGE_SECONDS", 65))
                try:
                    if last_reg is not None and (time.time() - float(last_reg)) <= max_age:
                        is_active = True
                        status_message = "Ativo (heartbeat)"
                    else:
                        is_active = False
                        status_message = "Timeout no ping"
                except (TypeError, ValueError):
                    is_active = False
                    status_message = "Timeout no ping"
            except Exception as e:
                is_active = False
                status_message = f"Erro: {str(e)}"[:80]

            # --- 2. Sempre atualizar status no banco (ativo ou inativo) ---
            crud.update_node_status(db_path, node_id, is_active, time.time(), status_message)
            
            if is_active:
                active_nodes += 1
                # Log apenas mudanças de status ou erros
                if status_message != "Ativo (ping OK)":
                    log_node_status(logger, node_id, status_message)
            
            # --- 3. Se o nó está ativo, tentar obter recursos disponíveis do agente ---
            if is_active:
                # Condição para nós 'normais' (não Android) por enquanto
                # A diferenciação por ID é uma forma simples para o protótipo.
                if not node_id.startswith('android-'):
                    # Se só temos heartbeat (sem rota de volta), não punir falha de /status.
                    heartbeat_only = status_message == "Ativo (heartbeat)"
                    agent_status_url = config.build_agent_api_url(ip_address, agent_port, '/status')
                    try:
                        response = requests.get(agent_status_url, timeout=5) # Timeout para requisição HTTP
                        if response.status_code == 200:
                            agent_data = response.json()
                            hardware_usage = agent_data.get("hardware_usage", {})
                            
                            # Coleta os recursos disponíveis para nós normais
                            cpu_available_percent = hardware_usage.get("cpu_available_percent")
                            memory_available_gb = hardware_usage.get("memory_available_gb")
                            disk_available_gb = hardware_usage.get("disk_available_gb")
                            cpu_used_percent = hardware_usage.get("cpu_used_percent")
                            memory_used_percent = hardware_usage.get("memory_used_percent")
                            disk_used_pct = hardware_usage.get("disk_used_percent")
                            last_resource_check = time.time() # Timestamp da checagem de recursos
                            
                            # Atualiza os recursos disponíveis (+ % usados) no DB
                            crud.update_node_resources(
                                db_path, 
                                node_id, 
                                cpu_available_percent, 
                                memory_available_gb, 
                                disk_available_gb, 
                                last_resource_check,
                                cpu_used_percent=cpu_used_percent,
                                memory_used_percent=memory_used_percent,
                                disk_used_percent=disk_used_pct,
                            )
                            disk_used_pct = hardware_usage.get("disk_used_percent")
                            if disk_used_pct is not None:
                                try:
                                    if float(disk_used_pct) >= float(config.DISK_CRITICAL_USED_PERCENT):
                                        from context_server.services.k8s_cleanup import clean_unused_images_job

                                        clean_unused_images_job(
                                            db_path,
                                            node_id=node_id,
                                            aggressive=True,
                                        )
                                except (TypeError, ValueError):
                                    pass
                            # Log detalhado apenas em DEBUG
                            logger.debug(f"Recursos de {node_id} atualizados: CPU: {cpu_available_percent}%, RAM: {memory_available_gb}GB, Disco: {disk_available_gb}GB")
                            if heartbeat_only:
                                crud.update_node_status(
                                    db_path, node_id, True, time.time(), "Ativo (ping/callback OK)"
                                )
                        else:
                            msg = f"Ping OK, /status falhou HTTP {response.status_code}"
                            logger.warning(f"Agente de {node_id} retornou status {response.status_code}: {response.text}")
                            if not heartbeat_only:
                                crud.update_node_status(db_path, node_id, False, time.time(), msg)
                                is_active = False
                                active_nodes = max(active_nodes - 1, 0)
                                log_node_status(logger, node_id, msg)
                    except requests.exceptions.Timeout:
                        msg = "Ping OK, timeout no /status"
                        logger.warning(f"Timeout ao consultar recursos de {node_id}")
                        if not heartbeat_only:
                            crud.update_node_status(db_path, node_id, False, time.time(), msg)
                            is_active = False
                            active_nodes = max(active_nodes - 1, 0)
                            log_node_status(logger, node_id, msg)
                    except requests.exceptions.ConnectionError:
                        msg = "Ping OK, sem conexão no /status"
                        logger.warning(f"Erro de conexão ao consultar recursos de {node_id}")
                        if not heartbeat_only:
                            crud.update_node_status(db_path, node_id, False, time.time(), msg)
                            is_active = False
                            active_nodes = max(active_nodes - 1, 0)
                            log_node_status(logger, node_id, msg)
                    except Exception as e:
                        msg = f"Ping OK, erro no /status: {str(e)[:60]}"
                        logger.error(f"Erro inesperado ao obter recursos de {node_id}: {str(e)}")
                        if not heartbeat_only:
                            crud.update_node_status(db_path, node_id, False, time.time(), msg)
                            is_active = False
                            active_nodes = max(active_nodes - 1, 0)
                            log_node_status(logger, node_id, msg)
                else:
                    # Para nós Android, apenas loga que a coleta de recursos detalhada será implementada depois
                    logger.debug(f"Nó Android {node_id}: Coleta detalhada de recursos será implementada futuramente.")
        
        # Log resumido do monitoramento
        log_monitoring_summary(logger, active_nodes, total_nodes)
            
    except Exception as e:
        logger.error(f"Erro geral no job de monitoramento: {e}")