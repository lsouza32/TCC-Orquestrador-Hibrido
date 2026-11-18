import sqlite3
import time

from context_server import config
from context_server.database.crud import get_db_connection

# --- Funções CRUD (Create, Read, Update, Delete) para Nós ---

def register_or_update_node_static_info(
    db_path,
    node_id,
    ip_address,
    agent_port,
    hostname,
    cpu_total_threads,
    memory_total_gb,
    disk_total_gb,
    last_registered_at,
    is_active,
    current_status_message,
    provider_type="node_agent",
    compute_layer=None,
):
    """
    Registra um novo nó no banco de dados ou atualiza as informações estáticas e de registro
    de um nó existente. Esta função NÃO sobrescreve os dados de recursos disponíveis.

    Args:
        provider_type: 'node_agent' (voluntário) ou 'kubernetes' (worker sincronizado pela API).
        compute_layer: 'edge' | 'fog' | 'cloud' (camada Edge/Fog/Cloud do TCC).
    """
    from context_server.services.compute_layer import resolve_compute_layer

    layer = resolve_compute_layer(
        explicit=compute_layer,
        hostname=hostname or node_id,
        cpu_total_threads=cpu_total_threads,
        memory_total_gb=memory_total_gb,
    )

    conn = get_db_connection(db_path)
    try:
        # Primeiro, verifica se o nó já existe
        cursor = conn.execute("SELECT node_id FROM nodes WHERE node_id = ?", (node_id,))
        existing_node = cursor.fetchone()

        if existing_node:
            conn.execute(
                """
                UPDATE nodes
                SET ip_address = ?, agent_port = ?, hostname = ?,
                    cpu_total_threads = ?, memory_total_gb = ?, disk_total_gb = ?,
                    last_registered_at = ?, is_active = ?, current_status_message = ?,
                    provider_type = ?, compute_layer = ?
                WHERE node_id = ?
                """,
                (
                    ip_address,
                    agent_port,
                    hostname,
                    cpu_total_threads,
                    memory_total_gb,
                    disk_total_gb,
                    last_registered_at,
                    is_active,
                    current_status_message,
                    provider_type or "node_agent",
                    layer,
                    node_id,
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO nodes (
                    node_id, ip_address, agent_port, hostname,
                    cpu_total_threads, memory_total_gb, disk_total_gb,
                    last_registered_at, is_active, current_status_message,
                    cpu_available_percent, memory_available_gb, disk_available_gb, last_resource_check,
                    provider_type, compute_layer
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    node_id,
                    ip_address,
                    agent_port,
                    hostname,
                    cpu_total_threads,
                    memory_total_gb,
                    disk_total_gb,
                    last_registered_at,
                    is_active,
                    current_status_message,
                    None,
                    None,
                    None,
                    None,
                    provider_type or "node_agent",
                    layer,
                ),
            )
        conn.commit()
    except sqlite3.Error as e:
        conn.rollback()
        raise Exception(
            f"Erro no DB ao registrar/atualizar nó {node_id!r} (provider_type={provider_type!r}): {e}"
        )
    finally:
        conn.close()
        
def update_node_status(db_path, node_id, is_active, last_checked_at, current_status_message):
    """
    Atualiza o status de atividade e a mensagem de status de um nó existente.
    Esta função é usada principalmente pelo job de monitoramento de ping.
    
    Args:
        db_path (str): Caminho para o DB.
        node_id (str): Identificador único do nó a ser atualizado.
        is_active (bool): Novo status de atividade (True/False).
        last_checked_at (float): Timestamp da última checagem.
        current_status_message (str): Nova mensagem de status.
    Raises:
        Exception: Se ocorrer um erro durante a operação no DB.
    """
    conn = get_db_connection(db_path)
    try:
        # Atualiza os campos 'is_active', 'last_checked_at' e 'current_status_message'
        # para o nó com o 'node_id' especificado.
        conn.execute('''
            UPDATE nodes 
            SET is_active = ?, last_checked_at = ?, current_status_message = ?
            WHERE node_id = ?
        ''', (is_active, last_checked_at, current_status_message, node_id))
        conn.commit() # Confirma a transação.
    except sqlite3.Error as e:
        conn.rollback() # Reverte a transação em caso de erro.
        raise Exception(f"Erro no DB ao atualizar status do nó: {e}") # Levanta uma exceção para ser tratada.
    finally:
        conn.close() # Garante que a conexão seja sempre fechada.



def update_node_resources(
    db_path,
    node_id,
    cpu_available_percent,
    memory_available_gb,
    disk_available_gb,
    last_resource_check,
    cpu_used_percent=None,
    memory_used_percent=None,
    disk_used_percent=None,
):
    """
    Atualiza os recursos DISPONÍVEIS (e opcionalmente % usados) de um nó.
    """
    conn = get_db_connection(db_path)
    try:
        # Atualiza available sempre; used_* só quando fornecidos (não zera com NULL acidental).
        conn.execute('''
            UPDATE nodes
            SET cpu_available_percent = ?,
                memory_available_gb = ?,
                disk_available_gb = ?,
                last_resource_check = ?,
                cpu_used_percent = COALESCE(?, cpu_used_percent),
                memory_used_percent = COALESCE(?, memory_used_percent),
                disk_used_percent = COALESCE(?, disk_used_percent)
            WHERE node_id = ?
        ''', (
            cpu_available_percent,
            memory_available_gb,
            disk_available_gb,
            last_resource_check,
            cpu_used_percent,
            memory_used_percent,
            disk_used_percent,
            node_id,
        ))
        conn.commit()
    except sqlite3.Error as e:
        conn.rollback()
        raise Exception(f"Erro no DB ao atualizar recursos do nó: {e}")
    finally:
        conn.close()



def get_all_nodes(db_path):
    """
    Recupera todos os nós registrados no banco de dados.
    
    Args:
        db_path (str): Caminho para o DB.
        
    Returns:
        list: Uma lista de dicionários, onde cada dicionário representa um nó.
    """
    conn = get_db_connection(db_path)
    cursor = conn.execute('SELECT * FROM nodes') # Seleciona todas as colunas da tabela 'nodes'.
    nodes = [dict(row) for row in cursor.fetchall()] # Converte as linhas (objetos Row) em dicionários.
    conn.close()
    return nodes

def get_active_nodes(db_path):
    """
    Recupera apenas os nós que estão marcados como ativos (is_active = TRUE).
    
    Args:
        db_path (str): Caminho para o DB.
        
    Returns:
        list: Uma lista de dicionários, representando os nós ativos.
    """
    now = time.time()
    max_age = getattr(config, "NODE_LAST_REGISTER_MAX_AGE_SECONDS", 65)
    conn = get_db_connection(db_path)
    # Além de is_active=1, exigimos que o nó tenha feito heartbeat recentemente.
    # Isso reduz a janela em que um nó desligado continua elegível devido a atrasos
    # do job de monitoramento/ping.
    cursor = conn.execute(
        '''
        SELECT * FROM nodes
        WHERE is_active = 1
          AND COALESCE(last_registered_at, 0) >= ?
        ''',
        (now - max_age,),
    )
    nodes = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return nodes

def get_node_details(db_path, node_id):
    """
    Recupera os detalhes de um nó específico com base no seu 'node_id'.
    
    Args:
        db_path (str): Caminho para o DB.
        node_id (str): O ID do nó a ser buscado.
        
    Returns:
        dict or None: Um dicionário com os detalhes do nó, ou None se o nó não for encontrado.
    """
    conn = get_db_connection(db_path)
    cursor = conn.execute('SELECT * FROM nodes WHERE node_id = ?', (node_id,)) # Usa um placeholder (?) para segurança.
    node = cursor.fetchone() # Pega apenas a primeira linha correspondente.
    conn.close()
    return dict(node) if node else None # Converte para dicionário se encontrou, senão retorna None.


def reserve_node_resources(db_path, node_id, cpu_threads, memory_gb, disk_gb):
    """Incrementa os recursos reservados para um nó."""
    conn = get_db_connection(db_path)
    try:
        cursor = conn.execute(
            '''SELECT cpu_total_threads, memory_total_gb, disk_total_gb,
                      COALESCE(cpu_reserved_threads, 0),
                      COALESCE(memory_reserved_gb, 0),
                      COALESCE(disk_reserved_gb, 0)
               FROM nodes WHERE node_id = ?''',
            (node_id,)
        )
        row = cursor.fetchone()
        if row is None:
            raise Exception(f"Nó {node_id} não encontrado para reserva de recursos")

        total_cpu, total_mem, total_disk, res_cpu, res_mem, res_disk = row

        new_cpu = res_cpu + cpu_threads
        new_mem = res_mem + memory_gb
        new_disk = res_disk + disk_gb

        if (total_cpu is not None and new_cpu > total_cpu) \
           or (total_mem is not None and new_mem > total_mem) \
           or (total_disk is not None and new_disk > total_disk):
            raise Exception(
                f"Reserva excede capacidade do nó {node_id}: "
                f"cpu={new_cpu}/{total_cpu}, mem={new_mem}/{total_mem}, disk={new_disk}/{total_disk}"
            )

        conn.execute(
            '''UPDATE nodes
               SET cpu_reserved_threads = ?,
                   memory_reserved_gb = ?,
                   disk_reserved_gb = ?
               WHERE node_id = ?''',
            (new_cpu, new_mem, new_disk, node_id)
        )
        conn.commit()
    except sqlite3.Error as e:
        conn.rollback()
        raise Exception(f"Erro no DB ao reservar recursos do nó {node_id}: {e}")
    finally:
        conn.close()


def release_node_resources(db_path, node_id, cpu_threads, memory_gb, disk_gb):
    """Libera recursos reservados previamente para um nó."""
    conn = get_db_connection(db_path)
    try:
        cursor = conn.execute(
            '''SELECT COALESCE(cpu_reserved_threads, 0),
                      COALESCE(memory_reserved_gb, 0),
                      COALESCE(disk_reserved_gb, 0)
               FROM nodes WHERE node_id = ?''',
            (node_id,)
        )
        row = cursor.fetchone()
        if row is None:
            raise Exception(f"Nó {node_id} não encontrado para liberação de recursos")

        res_cpu, res_mem, res_disk = row

        new_cpu = max(res_cpu - cpu_threads, 0)
        new_mem = max(res_mem - memory_gb, 0)
        new_disk = max(res_disk - disk_gb, 0)

        conn.execute(
            '''UPDATE nodes
               SET cpu_reserved_threads = ?,
                   memory_reserved_gb = ?,
                   disk_reserved_gb = ?
               WHERE node_id = ?''',
            (new_cpu, new_mem, new_disk, node_id)
        )
        conn.commit()
    except sqlite3.Error as e:
        conn.rollback()
        raise Exception(f"Erro no DB ao liberar recursos do nó {node_id}: {e}")
    finally:
        conn.close()