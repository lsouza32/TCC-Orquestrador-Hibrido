import os
import json

# --- Caminhos do Banco de Dados e Uploads ---

# O DB agora ficará na pasta persistente montada pelo Docker
DATABASE_PATH = os.path.join('/app/data', 'nodes.db')
# Pasta para uploads: caminho absoluto dentro do container, para o volume
UPLOAD_FOLDER = os.path.join('/app/uploaded_tasks')
# Pasta base para exportação de resultados (relatórios e planilhas por tarefa)
EXPORTS_BASE = os.getenv('EXPORTS_BASE', os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'exports'))

# --- Configurações da Rede ---

# Endereço da API do Context Server
# ESSE ENDERECO AINDA DEVE SER ALTERADO MANUALMENTE NO NODE_AGENT
PORTA_SERVER = 5000
CONTEXT_SERVER_URL = f"http://192.168.0.50:{PORTA_SERVER}"
CONTEXT_SERVER_IP= "http://192.168.0.50"

# Protocolo padrão utilizado para comunicação com os agentes de nó
NODE_AGENT_SCHEME = "http"


def build_agent_api_url(ip_address: str, port: int, path: str) -> str:
    """Monta a URL para acessar um endpoint do agente de nó."""
    normalized_path = path if path.startswith('/') else f'/{path}'
    return f"{NODE_AGENT_SCHEME}://{ip_address}:{port}{normalized_path}"

# Porta onde o agente do nó vai rodar
# ESSE ENDERECO AINDA DEVE SER ALTERADO MANUALMENTE NO NODE_AGENT
NODE_AGENT_PORT = 5001

# --- Configurações do Sistema ---

# Define o intervalo em segundos para o job de monitoramento de ping.
MONITOR_INTERVAL_SECONDS = 30

# Janela máxima (em segundos) para um nó ser considerado "ativo" com base
# no último heartbeat/registro recebido via /register_node.
# Ajuda a remover da fila nós que ficaram "presos" como is_active=1 por atraso
# do monitoramento (ex.: server ficou sem rodar job por alguns segundos).
NODE_LAST_REGISTER_MAX_AGE_SECONDS = (MONITOR_INTERVAL_SECONDS * 2) + 5

# Limpeza de imagens Docker (node_agent) e avisos em nós Kubernetes
IMAGE_CLEANUP_STALE_DAYS = int(os.getenv("IMAGE_CLEANUP_STALE_DAYS", "3"))
DISK_CRITICAL_USED_PERCENT = float(os.getenv("DISK_CRITICAL_USED_PERCENT", "80"))
IMAGE_CLEANUP_CRITICAL_COOLDOWN_SECONDS = int(
    os.getenv("IMAGE_CLEANUP_CRITICAL_COOLDOWN_SECONDS", str(6 * 3600))
)

# Hostnames permitidos no nodeSelector quando o ranking híbrido retorna vazio
STRATEGIST_FALLBACK_HOSTNAMES = [
    h.strip()
    for h in os.getenv(
        "STRATEGIST_FALLBACK_HOSTNAMES",
        "worker,worker2,tcc-cluster-worker",
    ).split(",")
    if h.strip()
]

# --- Kubernetes (Kind / cluster local) ---
K8S_NAMESPACE = os.getenv("K8S_NAMESPACE", "default")

# Realocação pós-falha: tentativas extras em outro nó (0 = desliga).
# Conta cada reenvio à fila após Failed/Unschedulable; limite exaustão → status falha.
TASK_REALLOCATION_MAX_ATTEMPTS = int(os.getenv("TASK_REALLOCATION_MAX_ATTEMPTS", "3"))

# --- Configurações de API Externa ---

# Chave de API do Google Gemini
GOOGLE_GEMINI_API_KEY = os.getenv('GOOGLE_GEMINI_API_KEY', '')

# Mapeamento de Requisitos de Tarefas
# 'auto': usado quando a IA define os requisitos; estes são fallback se a IA falhar (Ollama indisponível, etc.)
# Imagem 'tcc-tf-ml:latest': build em cada nó com docker/tf-ml/Dockerfile
TASK_REQUIREMENTS = {
    'auto': {
        'cpu_threads': 2,
        'ram_gb': 0.5,
        'disk_gb': 1.0,
        'docker_image': 'python:3.9-slim'
    },
    'leve': {
        'cpu_threads': 2,
        'ram_gb': 0.5,  # 500MB
        'disk_gb': 1.0,
        'docker_image': 'python:3.9-slim'
    },
    'medio': {
        'cpu_threads': 2,
        'ram_gb': 3.0,  # 3GB
        'disk_gb': 5.0,
        'docker_image': 'jupyter/datascience-notebook'
    },
    'pesado': {
        'cpu_threads': 4,
        'ram_gb': 6.0,  # 6GB
        'disk_gb': 12.0,
        'docker_image': 'tensorflow/tensorflow:latest'
    }
}

# Controle de acesso local (sem banco de autenticação):
# pode ser sobrescrito via variável de ambiente LOCAL_USERS_ACCESS em JSON.
DEFAULT_LOCAL_USERS_ACCESS = {
    "lsouza": {"is_admin": True},
    "estg.lsouza": {"is_admin": True},
    "usertest": {"is_admin": False},
}


def _load_local_users_access():
    raw = os.getenv("LOCAL_USERS_ACCESS", "").strip()
    if not raw:
        return DEFAULT_LOCAL_USERS_ACCESS
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass
    return DEFAULT_LOCAL_USERS_ACCESS


LOCAL_USERS_ACCESS = _load_local_users_access()