from flask import Flask, jsonify, request
import psutil
import platform
import socket
import glob
import os
import time
import requests
import uuid # Para gerar IDs únicos
from apscheduler.schedulers.background import BackgroundScheduler 
import subprocess # 
import threading # 
from werkzeug.utils import secure_filename 
import logging
import re
from threading import Lock

app = Flask(__name__)

# Configuração de logging para o node agent
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | NODE-AGENT | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- Configurações do Agente ---
# Porta onde o agente do nó vai rodar.
NODE_PORT = int(os.getenv("NODE_AGENT_PORT", "5001"))
# O URL base do seu Context Server.
CONTEXT_SERVER_URL = os.getenv("CONTEXT_SERVER_URL", "http://192.168.0.50:5000").rstrip("/")
REGISTRATION_ENDPOINT = f"{CONTEXT_SERVER_URL}/register_node"
MONITOR_INTERVAL_SECONDS = 30 # Intervalo para enviar heartbeats de registro (e garantir que o CS nos veja).
# Camada Edge/Fog/Cloud (opcional). Ex.: COMPUTE_LAYER=edge
COMPUTE_LAYER = (os.getenv("COMPUTE_LAYER") or "").strip().lower() or None


def _parse_context_server_host(url: str) -> str:
    """Extrai hostname/IP do CONTEXT_SERVER_URL (sem porta/path)."""
    s = (url or "").strip()
    if "://" in s:
        s = s.split("://", 1)[1]
    s = s.split("/", 1)[0]
    if s.startswith("[") and "]" in s:
        return s[1:s.index("]")]
    return s.split(":", 1)[0]


def detect_advertise_ip(context_server_url: str) -> str:
    """
    IP que o Broker deve usar para callback (/status).

    Prioridade:
    1) NODE_IP / AGENT_ADVERTISE_IP (override manual)
    2) IP da rota local até o Context Server (evita Hyper-V/Docker 192.168.x.1 errado)
    3) gethostbyname(hostname) como último recurso
    """
    override = (os.getenv("NODE_IP") or os.getenv("AGENT_ADVERTISE_IP") or "").strip()
    if override:
        return override

    host = _parse_context_server_host(context_server_url)
    if host:
        try:
            # UDP não envia pacote; só descobre a interface de saída para o broker.
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                sock.connect((host, 5000))
                ip = sock.getsockname()[0]
                if ip and not ip.startswith("127."):
                    return ip
            finally:
                sock.close()
        except OSError:
            pass

    try:
        ip = socket.gethostbyname(socket.gethostname())
        if ip and not ip.startswith("127."):
            return ip
    except OSError:
        pass
    return "127.0.0.1"


NODE_IP = detect_advertise_ip(CONTEXT_SERVER_URL)

# Pasta onde as tarefas recebidas serão salvas no nó
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TASK_UPLOAD_FOLDER = os.path.join(BASE_DIR, 'tasks')
os.makedirs(TASK_UPLOAD_FOLDER, exist_ok=True)

# Controle de recursos reservados (tarefas em execução)
ACTIVE_TASK_RESERVATIONS = {}
RESERVATION_LOCK = Lock()
# Segundos de amostragem após o script terminar, antes de reportar ao Broker
TELEMETRY_POST_GRACE_SECONDS = max(3, int(os.getenv("NODE_AGENT_TELEMETRY_GRACE_SECONDS", "8")))
# Tentativas de POST /task_completed até o Broker confirmar (HTTP 200 + telemetry_ack)
TELEMETRY_REPORT_MAX_ATTEMPTS = max(1, int(os.getenv("NODE_AGENT_TELEMETRY_REPORT_ATTEMPTS", "5")))
TELEMETRY_REPORT_RETRY_SECONDS = max(1, int(os.getenv("NODE_AGENT_TELEMETRY_REPORT_RETRY_SECONDS", "3")))
# Processos em execução (task_id -> Popen) para permitir cancelamento
RUNNING_TASKS = {}
# Tarefas que o usuário pediu para cancelar (para reportar status cancelada)
CANCELLED_TASKS = set()


# Gera um ID único persistente para este nó.
# Tenta carregar de um arquivo; se não existir, gera um novo e salva.
NODE_ID_FILE = 'node_id.txt'
NODE_ID = None
if os.path.exists(NODE_ID_FILE):
    with open(NODE_ID_FILE, 'r') as f:
        NODE_ID = f.read().strip()
else:
    NODE_ID = str(uuid.uuid4())
    with open(NODE_ID_FILE, 'w') as f:
        f.write(NODE_ID)

# --- Funções para Coleta de Hardware ---
def get_total_hardware_info():
    """Coleta informações totais de hardware do nó."""
    mem_info = psutil.virtual_memory()
    disk_info = psutil.disk_usage('/') # Uso do disco raiz

    return {
        "cpu_total_threads": psutil.cpu_count(logical=True),
        "memory_total_gb": round(mem_info.total / (1024**3), 2),
        "disk_total_gb": round(disk_info.total / (1024**3), 2),
    }

def _effective_memory_usage(mem_info) -> tuple[float, float, float]:
    """
    Memória efetiva em uso (desconta buffers/caches reaproveitáveis pelo SO).

    Umem = (total - free - buffers - cached) / total
    Retorna (umem_fraction 0–1, used_percent 0–100, available_gb).
    """
    total = float(mem_info.total or 0)
    if total <= 0:
        return 0.0, 0.0, 0.0
    free = float(mem_info.free or 0)
    buffers = float(getattr(mem_info, "buffers", 0) or 0)
    cached = float(getattr(mem_info, "cached", 0) or 0)
    used_bytes = max(0.0, total - free - buffers - cached)
    umem = used_bytes / total
    available_gb = max(0.0, (total - used_bytes) / (1024 ** 3))
    return umem, round(umem * 100.0, 2), round(available_gb, 2)


def get_current_hardware_usage():
    """
    Coleta informações de uso/disponibilidade atual do hardware.
    Retorna a porcentagem de CPU DISPONÍVEL e memória/disco disponíveis em GB.
    """
    cpu_percent_usage = psutil.cpu_percent(interval=0.1) # Captura uso da CPU em um curto intervalo
    cpu_available_percent = 100.0 - cpu_percent_usage # Porcentagem de CPU disponível

    mem_info = psutil.virtual_memory()
    disk_info = psutil.disk_usage('/')
    _umem, memory_used_percent, memory_available_gb = _effective_memory_usage(mem_info)

    return {
        "cpu_available_percent": round(cpu_available_percent, 2), # Adicionado: Porcentagem de CPU DISPONÍVEL
        "cpu_used_percent": round(cpu_percent_usage, 2),         # Mantido: Porcentagem de CPU USADA
        "memory_available_gb": memory_available_gb,
        "memory_used_percent": memory_used_percent,
        "disk_available_gb": round(disk_info.free / (1024**3), 2),
        "disk_used_percent": disk_info.percent,                  # Porcentagem de disco USADA
    }

# --- Auto-Registro no Context Server ---
def register_with_context_server():
    """
    Envia as informações TOTAIS do nó para o Context Server para registro/atualização.
    Os recursos DISPONÍVEIS serão enviados através do endpoint /status quando solicitado.
    """
    logger.debug(f"Tentando registrar nó com ID: {NODE_ID} no Context Server")
    total_hw = get_total_hardware_info()
    registration_data = {
        "node_id": NODE_ID,
        "ip_address": NODE_IP,
        "agent_port": NODE_PORT,
        "hostname": socket.gethostname(),
        "cpu_total_threads": total_hw["cpu_total_threads"],
        "memory_total_gb": total_hw["memory_total_gb"],
        "disk_total_gb": total_hw["disk_total_gb"],
    }
    if COMPUTE_LAYER:
        registration_data["compute_layer"] = COMPUTE_LAYER
    
    try:
        response = requests.post(REGISTRATION_ENDPOINT, json=registration_data, timeout=5)
        if response.status_code == 200:
            logger.info(f"✅ Nó {NODE_ID} registrado/atualizado no Context Server")
            return True
        else:
            logger.warning(f"Erro no registro/atualização (Status: {response.status_code}): {response.text}")
            return False
    except requests.exceptions.ConnectionError:
        logger.warning(f"Erro de conexão com o Context Server em {CONTEXT_SERVER_URL}")
        return False
    except requests.exceptions.Timeout:
        logger.warning(f"Timeout ao tentar registrar no Context Server")
        return False
    except Exception as e:
        logger.error(f"Erro inesperado ao registrar no Context Server: {e}")
        return False

# --- API do Agente do Nó ---
@app.route('/status', methods=['GET'])
def get_agent_status():
    """
    Endpoint para o Context Server (ou outro consumidor) consultar o status e 
    os recursos LIVRES atuais deste nó.
    """
    try:
        current_hw_usage = get_current_hardware_usage()

        with RESERVATION_LOCK:
            total_reserved_cpu = sum(res.get('cpu_threads', 0) for res in ACTIVE_TASK_RESERVATIONS.values())
            total_reserved_mem = sum(res.get('ram_gb', 0) for res in ACTIVE_TASK_RESERVATIONS.values())
            total_reserved_disk = sum(res.get('disk_gb', 0) for res in ACTIVE_TASK_RESERVATIONS.values())

        total_threads = psutil.cpu_count(logical=True) or 1
        reserved_cpu_percent = (total_reserved_cpu / total_threads) * 100

        current_hw_usage["cpu_available_percent"] = max(current_hw_usage["cpu_available_percent"] - reserved_cpu_percent, 0)
        current_hw_usage["memory_available_gb"] = max(current_hw_usage["memory_available_gb"] - total_reserved_mem, 0)
        current_hw_usage["disk_available_gb"] = max(current_hw_usage["disk_available_gb"] - total_reserved_disk, 0)
        current_hw_usage["reserved_resources"] = {
            "cpu_threads": total_reserved_cpu,
            "memory_gb": total_reserved_mem,
            "disk_gb": total_reserved_disk,
        }
        
        return jsonify({
            "node_id": NODE_ID,
            "status": "ativo",
            "timestamp": time.time(),
            "system_info": {
                "os": platform.system(),
                "os_version": platform.release(),
                "architecture": platform.machine(),
                "hostname": socket.gethostname(),
                "ip_address": NODE_IP
            },
            "hardware_usage": current_hw_usage # Contém os recursos disponíveis e usados
        }), 200
    except Exception as e:
        logger.error(f"Erro ao coletar status do agente: {e}")
        return jsonify({"status": "erro", "message": str(e), "node_id": NODE_ID}), 500


def _docker_cli(args: list, timeout: int = 300) -> tuple[int, str, str]:
    try:
        p = subprocess.run(
            ["docker"] + list(args),
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        return p.returncode, (p.stdout or ""), (p.stderr or "")
    except FileNotFoundError:
        return 127, "", "docker não encontrado no PATH"
    except subprocess.TimeoutExpired:
        return 124, "", "timeout ao executar docker"


def _normalize_image_ref(ref: str) -> str:
    """Normaliza referência de imagem para comparação (strip, sem digest)."""
    r = str(ref or "").strip()
    if "@" in r:
        r = r.split("@", 1)[0]
    return r


def _docker_images_in_use() -> set[str]:
    """
    Imagens referenciadas por containers running/created (equivalente a proteção de pods ativos).
    """
    protected: set[str] = set()
    for status in ("running", "created"):
        code, out, _err = _docker_cli(
            ["ps", "-a", "--filter", f"status={status}", "--format", "{{.Image}}"],
            timeout=60,
        )
        if code != 0:
            continue
        for line in (out or "").splitlines():
            ref = _normalize_image_ref(line)
            if ref:
                protected.add(ref)
    return protected


def _protected_images_from_reservations() -> set[str]:
    with RESERVATION_LOCK:
        refs = {
            _normalize_image_ref(res.get("docker_image", ""))
            for res in ACTIVE_TASK_RESERVATIONS.values()
            if res.get("docker_image")
        }
    return {r for r in refs if r}


def _filter_rmi_safe(rmi_images: list) -> tuple[list[str], list[str]]:
    """Remove da lista de deleção imagens em uso (docker ps + reservas ativas)."""
    in_use = _docker_images_in_use()
    from_reservations = _protected_images_from_reservations()
    protected = in_use | from_reservations
    safe: list[str] = []
    skipped: list[str] = []
    for img in rmi_images:
        if not isinstance(img, str) or not str(img).strip():
            continue
        ref = _normalize_image_ref(img)
        if ref in protected:
            skipped.append(ref)
            continue
        safe.append(ref)
    return safe, skipped


@app.route('/docker_cleanup', methods=['POST'])
def docker_cleanup():
    """
    Chamado pelo Context Server: remove imagens obsoletas e opcionalmente prune leve.
    JSON: rmi_images (lista), prune_dangling (bool), prune_all_unused (bool, ignorado — sem prune -a).
    """
    data = request.get_json(silent=True) or {}
    rmi_images = data.get("rmi_images") or []
    if not isinstance(rmi_images, list):
        rmi_images = []
    prune_dangling = bool(data.get("prune_dangling", True))
    # F10: nunca prune -a genérico; limpeza agressiva = rmi iterado na lista filtrada
    prune_all_unused = False

    safe_rmi, skipped = _filter_rmi_safe(rmi_images)

    logger.info(
        "Pedido de limpeza Docker: rmi_solicitadas=%s rmi_seguras=%s protegidas=%s prune_dangling=%s",
        len(rmi_images),
        len(safe_rmi),
        len(skipped),
        prune_dangling,
    )
    steps: list[dict] = []
    if skipped:
        steps.append({"op": "skipped_protected", "images": skipped[:50], "count": len(skipped)})

    for img in safe_rmi:
        code, _out, err = _docker_cli(["rmi", "-f", img], timeout=180)
        steps.append({"op": "rmi", "image": img, "returncode": code, "stderr": (err or "")[:800]})

    if prune_dangling:
        code, _out, err = _docker_cli(["image", "prune", "-f"], timeout=400)
        steps.append({"op": "image_prune_f", "returncode": code, "stderr": (err or "")[:800]})

    return jsonify({"ok": True, "node_id": NODE_ID, "steps": steps, "skipped_protected": skipped}), 200

# --- ENDPOINT DE UPLOAD DE ARQUIVOS (RECEBE A TAREFA) ---
@app.route('/upload_file', methods=['POST'])
def upload_task_file():
    if 'file' not in request.files:
        logger.warning("❌ Nenhum arquivo 'file' na requisição")
        return jsonify({"message": "Nenhum arquivo enviado."}), 400
    
    file = request.files['file']
    filename = secure_filename(file.filename)
    file_path = os.path.join(TASK_UPLOAD_FOLDER, filename)
    file.save(file_path)
    
    # Verifica se o arquivo foi salvo corretamente
    file_size = os.path.getsize(file_path)
    
    logger.info(f"📁 ARQUIVO RECEBIDO COM SUCESSO")
    logger.info(f"📊 Nome: {filename} | Tamanho: {file_size} bytes | Local: {file_path}")
    
    return jsonify({
        "message": "Arquivo recebido com sucesso.",
        "file_path": file_path
    }), 200
    
# --- ENDPOINT DE EXECUÇÃO DE TAREFAS ---
# Contrato do payload (enviado pelo Context Server / orquestrador):
#   task_id, filename_on_node, command_string,
#   resource_requirements { cpu_threads, ram_gb, disk_gb, docker_image, cpu_limit, memory_limit, detected_libraries },
#   requirements_txt
# O agente retém o container (sem --rm) até POST /task_completed retornar telemetry_ack=true.
@app.route('/execute_task', methods=['POST'])
def execute_task():
    data = request.json
    task_id = data.get('task_id')
    filename_on_node = data.get('filename_on_node')
    command_string = data.get('command_string')
    resource_requirements = data.get('resource_requirements', {}) or {}
    docker_image = resource_requirements.get('docker_image', 'python:3.9-slim')
    requirements_txt = data.get('requirements_txt') or ""

    if not all([task_id, filename_on_node]):
        logger.warning(f"❌ Dados incompletos para execução da tarefa {task_id}")
        return jsonify({"message": "Dados incompletos para a execução da tarefa."}), 400

    # Se veio 'tasks/arquivo.py' (relativo) ou '/abs/caminho/arquivo.py' (absoluto), normalizamos:
    if os.path.isabs(filename_on_node):
        script_path = filename_on_node
    else:
        # Garante que não duplica 'tasks/'
        script_path = os.path.join(TASK_UPLOAD_FOLDER, os.path.basename(filename_on_node))

    # Verifica se o arquivo existe
    if not os.path.exists(script_path):
        logger.error(f"❌ Arquivo não encontrado: {script_path}")
        return jsonify({"message": f"Arquivo não encontrado: {script_path}"}), 404

    # Grava requirements.txt no mesmo diretório do script quando enviado pelo broker (IA)
    script_dir = os.path.dirname(script_path)
    use_requirements = bool(requirements_txt.strip())
    if use_requirements:
        req_path = os.path.join(script_dir, "requirements.txt")
        try:
            with open(req_path, "w", encoding="utf-8") as f:
                f.write(requirements_txt.strip())
            logger.info(f"📄 requirements.txt gravado em {req_path}")
        except Exception as e:
            logger.warning(f"Falha ao gravar requirements.txt: {e}")
            use_requirements = False

    # Se command_string foi fornecido, usa Docker; senão, usa execução direta
    if command_string:
        # Execução em Docker (com nome de container fixo para monitoramento por cgroups)
        docker_command, container_name = build_docker_command(
            script_path,
            docker_image,
            task_id,
            use_requirements=use_requirements,
            resource_requirements=resource_requirements,
        )
        logger.info(f"🐳 EXECUÇÃO DOCKER INICIADA")
        logger.info(f"📊 Tarefa: {task_id} | Container: {container_name} | Arquivo: {script_path}")
        
        with RESERVATION_LOCK:
            ACTIVE_TASK_RESERVATIONS[task_id] = {
                'cpu_threads': resource_requirements.get('cpu_threads', 0),
                'ram_gb': resource_requirements.get('ram_gb', 0),
                'disk_gb': resource_requirements.get('disk_gb', 0),
                'docker_image': docker_image,
            }

        t = threading.Thread(
            target=run_task_in_background,
            args=(task_id, docker_command, script_path),
            kwargs={"container_name": container_name, "resource_requirements": resource_requirements},
        )
        t.start()
    else:
        # Execução direta (fallback)
        import sys
        command = [sys.executable, script_path]
        logger.info(f"🚀 EXECUÇÃO DIRETA INICIADA")
        logger.info(f"📊 Tarefa: {task_id} | Arquivo: {script_path} | Comando: {' '.join(command)}")
        
        with RESERVATION_LOCK:
            ACTIVE_TASK_RESERVATIONS[task_id] = {
                'cpu_threads': resource_requirements.get('cpu_threads', 0),
                'ram_gb': resource_requirements.get('ram_gb', 0),
                'disk_gb': resource_requirements.get('disk_gb', 0),
            }

        t = threading.Thread(target=run_task_in_background, args=(task_id, command, script_path))
        t.start()

    return jsonify({"message": f"Tarefa {task_id} recebida e iniciada em segundo plano."}), 200


@app.route('/cancel_task', methods=['POST'])
def cancel_task():
    """Recebe pedido do Context Server para interromper uma tarefa em execução."""
    data = request.get_json() or {}
    task_id = data.get('task_id')
    if not task_id:
        return jsonify({"message": "task_id é obrigatório."}), 400
    CANCELLED_TASKS.add(task_id)
    process = RUNNING_TASKS.get(task_id)
    if process:
        try:
            process.terminate()
            logger.info(f"🛑 Tarefa {task_id} interrompida a pedido do Context Server.")
        except Exception as e:
            logger.warning(f"Falha ao terminar processo da tarefa {task_id}: {e}")
    return jsonify({"message": "Cancelamento solicitado.", "task_id": task_id}), 200


def _docker_resource_args(resource_requirements: dict) -> list:
    """Mapeia limites do orquestrador (k8s_job_spec / IA) para flags do docker run."""
    args: list = []
    if not isinstance(resource_requirements, dict):
        return args
    cpu_threads = resource_requirements.get("cpu_threads")
    ram_gb = resource_requirements.get("ram_gb")
    cpu_limit = resource_requirements.get("cpu_limit") or resource_requirements.get("cpu_limit_cores")
    mem_limit = resource_requirements.get("memory_limit") or resource_requirements.get("mem_limit")
    try:
        if cpu_limit is not None:
            args.extend(["--cpus", str(max(0.1, float(cpu_limit)))])
        elif cpu_threads is not None:
            args.extend(["--cpus", str(max(0.1, float(cpu_threads)))])
    except (TypeError, ValueError):
        pass
    try:
        if mem_limit is not None:
            args.extend(["--memory", str(mem_limit)])
        elif ram_gb is not None:
            gb = float(ram_gb)
            if gb >= 1.0:
                args.extend(["--memory", f"{int(gb)}g"])
            else:
                args.extend(["--memory", f"{max(64, int(gb * 1024))}m"])
    except (TypeError, ValueError):
        pass
    return args


# Função para construir comando Docker (container com nome estável para monitoramento via cgroups)
def build_docker_command(script_path, docker_image, task_id, use_requirements=False, resource_requirements=None):
    """
    Constrói o comando Docker para executar o script.
    Sem --rm: o Broker confirma telemetria antes da remoção explícita do container.

    Returns:
        tuple: (lista do comando Docker, nome do container)
    """
    safe_id = re.sub(r"[^a-zA-Z0-9_]", "_", str(task_id))[:64]
    container_name = f"task_{safe_id}"
    script_filename = os.path.basename(script_path)
    if use_requirements:
        inner_cmd = f"cd /app/scripts && pip install -r requirements.txt && python3 {script_filename}"
    else:
        inner_cmd = f"cd /app/scripts && python3 {script_filename}"
    docker_command = [
        "docker", "run",
        "--name", container_name,
        *_docker_resource_args(resource_requirements or {}),
        "-v", f"{os.path.dirname(script_path)}:/app/scripts",
        docker_image,
        "sh", "-c", inner_cmd,
    ]
    return docker_command, container_name


def _parse_docker_stats_line(line):
    """Extrai CPU% e RAM em MB de uma linha do docker stats --no-stream."""
    cpu_percent = None
    ram_mb = None
    parts = line.strip().split()
    for i, p in enumerate(parts):
        if p.endswith("%") and cpu_percent is None:
            try:
                cpu_percent = float(p.replace("%", ""))
            except ValueError:
                pass
        if "MiB" in p or "GiB" in p:
            try:
                num = float(re.sub(r"[^0-9.]", "", p))
                if "GiB" in p:
                    num *= 1024
                if ram_mb is None:
                    ram_mb = num
            except (ValueError, TypeError):
                pass
    return cpu_percent, ram_mb


def _remove_task_container(container_name: str) -> None:
    if not container_name:
        return
    try:
        subprocess.run(
            ["docker", "rm", "-f", container_name],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        logger.info("Container %s removido após confirmação do Broker.", container_name)
    except Exception as e:
        logger.warning("Falha ao remover container %s: %s", container_name, e)


def _post_execution_stats_grace(container_name, stats_holder, stop_event, monitor_thread):
    """Continua amostrando picos após o script terminar, antes do report ao Broker."""
    deadline = time.time() + TELEMETRY_POST_GRACE_SECONDS
    while time.time() < deadline and not stop_event.is_set():
        try:
            out = subprocess.run(
                ["docker", "stats", "--no-stream", "--format", "{{.CPUPerc}}\t{{.MemUsage}}", container_name],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if out.returncode == 0 and out.stdout.strip():
                line = out.stdout.strip().split("\n")[0]
                cpu_p, ram_mb = _parse_docker_stats_line(line)
                if cpu_p is not None:
                    stats_holder["cpu_peak_percent"] = max(stats_holder.get("cpu_peak_percent", 0), cpu_p)
                if ram_mb is not None:
                    stats_holder["ram_peak_mb"] = max(stats_holder.get("ram_peak_mb", 0), ram_mb)
        except Exception:
            pass
        time.sleep(1.0)
    stop_event.set()
    if monitor_thread:
        monitor_thread.join(timeout=5.0)


def _monitor_container_stats(container_name, stats_holder, stop_event):
    """
    Loop que coleta docker stats do container (cgroups) até o container sumir.
    Preenche stats_holder com cpu_peak_percent e ram_peak_mb (máximos observados).
    """
    interval = 1.0
    while not stop_event.is_set():
        try:
            out = subprocess.run(
                ["docker", "stats", "--no-stream", "--format", "{{.CPUPerc}}\t{{.MemUsage}}", container_name],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if out.returncode != 0 or not out.stdout.strip():
                break
            line = out.stdout.strip().split("\n")[0]
            cpu_p, ram_mb = _parse_docker_stats_line(line)
            if cpu_p is not None:
                stats_holder["cpu_peak_percent"] = max(stats_holder.get("cpu_peak_percent", 0), cpu_p)
            if ram_mb is not None:
                stats_holder["ram_peak_mb"] = max(stats_holder.get("ram_peak_mb", 0), ram_mb)
        except (subprocess.TimeoutExpired, FileNotFoundError, Exception) as e:
            logger.debug("Monitor container %s: %s", container_name, e)
            break
        time.sleep(interval)
    logger.debug("Monitor do container %s encerrado. Pico CPU=%.2f%%, RAM=%.2f MB", container_name,
                 stats_holder.get("cpu_peak_percent"), stats_holder.get("ram_peak_mb"))


# Função para enviar artefatos (ex.: matriz de confusão) ao Context Server para inclusão no ZIP de exportação
def upload_task_artifacts_to_context_server(task_id, script_dir):
    """Envia arquivos confusion_matrix_*.png do diretório do script para o broker (exports da tarefa)."""
    if not script_dir or not os.path.isdir(script_dir):
        return
    pattern = os.path.join(script_dir, "confusion_matrix_*.png")
    files = glob.glob(pattern)
    if not files:
        logger.debug(f"Nenhum artefato confusion_matrix_*.png em {script_dir}")
        return
    url = f"{CONTEXT_SERVER_URL}/task_artifacts/{task_id}"
    file_handles = []
    try:
        for path in files:
            fh = open(path, "rb")
            file_handles.append(("files", (os.path.basename(path), fh, "image/png")))
        r = requests.post(url, files=file_handles, timeout=30)
        if r.status_code == 200:
            logger.info(f"Artefatos da tarefa {task_id} enviados ao Context Server: {[os.path.basename(p) for p in files]}")
        else:
            logger.warning(f"Envio de artefatos da tarefa {task_id}: {r.status_code} - {r.text[:200]}")
    except requests.exceptions.RequestException as e:
        logger.warning(f"Falha ao enviar artefatos da tarefa {task_id} ao Context Server: {e}")
    except Exception as e:
        logger.warning(f"Erro ao enviar artefatos da tarefa {task_id}: {e}")
    finally:
        for _, (_, fh, _) in file_handles:
            try:
                fh.close()
            except Exception:
                pass


# Função para executar a tarefa e reportar o resultado
def run_task_in_background(task_id, command, script_path=None, container_name=None, resource_requirements=None):
    resource_requirements = resource_requirements or {}
    logger.info(f"⚙️ Executando tarefa {task_id}...")
    logger.info(f"🔧 Comando: {' '.join(command)}")
    
    start_time = time.time()
    stats_holder = {}
    stop_event = threading.Event()
    monitor_thread = None
    if container_name:
        monitor_thread = threading.Thread(target=_monitor_container_stats, args=(container_name, stats_holder, stop_event), daemon=True)
        monitor_thread.start()
    
    try:
        # Popen: encoding UTF-8 para evitar UnicodeDecodeError quando a tarefa imprime UTF-8 (emoji, relatórios, etc.)
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        RUNNING_TASKS[task_id] = process
        stdout, stderr = process.communicate()
        RUNNING_TASKS.pop(task_id, None)
        if container_name:
            _post_execution_stats_grace(container_name, stats_holder, stop_event, monitor_thread)
        else:
            stop_event.set()
            if monitor_thread:
                monitor_thread.join(timeout=2.0)
        stdout = stdout if stdout is not None else ""
        stderr = stderr if stderr is not None else ""
        return_code = process.returncode
        execution_time = round(time.time() - start_time, 2)
        cpu_peak = stats_holder.get("cpu_peak_percent")
        ram_peak_mb = stats_holder.get("ram_peak_mb")

        def _report(status_final: str) -> bool:
            return report_task_completion(
                task_id,
                status_final,
                execution_time,
                stdout,
                stderr,
                cpu_peak_percent=cpu_peak,
                ram_peak_mb=ram_peak_mb,
                detected_libraries=resource_requirements.get("detected_libraries"),
            )

        if task_id in CANCELLED_TASKS:
            CANCELLED_TASKS.discard(task_id)
            logger.info(f"🛑 Tarefa {task_id} interrompida (cancelada pelo usuário).")
            _report("cancelada")
            if container_name:
                _remove_task_container(container_name)
            return
        if return_code == 0:
            logger.info(f"✅ TAREFA CONCLUÍDA COM SUCESSO")
            logger.info(f"📊 Tarefa: {task_id} | Tempo: {execution_time}s | Código: {return_code} | CPU pico: {cpu_peak}% | RAM pico: {ram_peak_mb} MB")
            
            # Log da saída da tarefa (limitado para não spam)
            if stdout.strip():
                output_lines = stdout.strip().split('\n')
                if len(output_lines) <= 10:  # Se poucas linhas, mostra tudo
                    logger.info(f"📤 Saída da tarefa {task_id}:")
                    for line in output_lines:
                        logger.info(f"   {line}")
                else:  # Se muitas linhas, mostra resumo
                    logger.info(f"📤 Saída da tarefa {task_id} ({len(output_lines)} linhas):")
                    for line in output_lines[:5]:
                        logger.info(f"   {line}")
                    logger.info(f"   ... ({len(output_lines) - 5} linhas omitidas)")
            else:
                logger.info(f"📤 Tarefa {task_id} executada sem saída")
                
            ack = _report("concluida")
            if script_path:
                script_dir = os.path.dirname(script_path)
                upload_task_artifacts_to_context_server(task_id, script_dir)
            if container_name:
                if ack:
                    _remove_task_container(container_name)
                else:
                    logger.warning(
                        "Broker não confirmou telemetria da tarefa %s; container %s mantido para nova tentativa manual.",
                        task_id,
                        container_name,
                    )
        else:
            logger.error(f"❌ TAREFA FALHOU")
            logger.error(f"📊 Tarefa: {task_id} | Tempo: {execution_time}s | Código: {return_code}")
            
            # Log do erro da tarefa
            if stderr.strip():
                error_lines = stderr.strip().split('\n')
                logger.error(f"🚨 Erro da tarefa {task_id}:")
                for line in error_lines[:10]:  # Limita a 10 linhas de erro
                    logger.error(f"   {line}")
                if len(error_lines) > 10:
                    logger.error(f"   ... ({len(error_lines) - 10} linhas de erro omitidas)")
            else:
                logger.error(f"🚨 Tarefa {task_id} falhou sem mensagem de erro")
                
            ack = _report("falha")
            if container_name:
                if ack:
                    _remove_task_container(container_name)
                else:
                    logger.warning("Broker não confirmou telemetria (falha) tarefa %s; container %s preservado.", task_id, container_name)
            
    except Exception as e:
        execution_time = round(time.time() - start_time, 2)
        RUNNING_TASKS.pop(task_id, None)
        stop_event.set()
        cpu_peak = stats_holder.get("cpu_peak_percent")
        ram_peak_mb = stats_holder.get("ram_peak_mb")
        logger.error(f"💥 ERRO DURANTE EXECUÇÃO")
        logger.error(f"📊 Tarefa: {task_id} | Tempo: {execution_time}s | Erro: {e}")
        logger.error(f"🚨 Falha na execução da tarefa {task_id}: {str(e)}")
        
        # Reporta erro do agente para o Context Server
        ack = report_task_completion(task_id, "erro_agente", execution_time, None, str(e), cpu_peak_percent=cpu_peak, ram_peak_mb=ram_peak_mb)
        if container_name and ack:
            _remove_task_container(container_name)

    finally:
        RUNNING_TASKS.pop(task_id, None)
        with RESERVATION_LOCK:
            ACTIVE_TASK_RESERVATIONS.pop(task_id, None)


# Função para reportar conclusão da tarefa para o Context Server (POST /task_completed)
# Payload: task_id, status_final, duration_seconds, stdout_log, stderr_log,
#          cpu_peak_percent, ram_peak_mb, detected_libraries (opcionais)
def report_task_completion(task_id, status_final, duration_seconds, stdout_log=None, stderr_log=None,
                          cpu_peak_percent=None, ram_peak_mb=None, detected_libraries=None) -> bool:
    """
    Envia resultado e telemetria (CPU/RAM pico do container) para o broker.
    Retorna True somente quando o Broker responde HTTP 200 com telemetry_ack.
    """
    try:
        payload = {
            "task_id": task_id,
            "status_final": status_final,
            "duration_seconds": duration_seconds,
            "stdout_log": stdout_log,
            "stderr_log": stderr_log
        }
        if cpu_peak_percent is not None:
            payload["cpu_peak_percent"] = round(cpu_peak_percent, 2)
        if ram_peak_mb is not None:
            payload["ram_peak_mb"] = round(ram_peak_mb, 2)
        if detected_libraries is not None:
            payload["detected_libraries"] = detected_libraries
        
        # URL do endpoint de finalização no Context Server
        completion_url = f"{CONTEXT_SERVER_URL}/task_completed"
        
        logger.info(f"📤 Reportando resultado da tarefa {task_id} para o Context Server...")
        logger.debug(f"📊 Status: {status_final} | Duração: {duration_seconds}s")
        
        for attempt in range(1, TELEMETRY_REPORT_MAX_ATTEMPTS + 1):
            response = requests.post(completion_url, json=payload, timeout=15)
            if response.status_code == 200:
                try:
                    body = response.json()
                except ValueError:
                    body = {}
                if body.get("telemetry_ack"):
                    logger.info(
                        "✅ Tarefa %s: Broker confirmou telemetria (cpu=%s ram_mb=%s)",
                        task_id,
                        cpu_peak_percent,
                        ram_peak_mb,
                    )
                    return True
                logger.warning("Broker 200 sem telemetry_ack para %s (tentativa %s)", task_id, attempt)
            else:
                logger.error(
                    "❌ Falha ao reportar tarefa %s: HTTP %s — %s",
                    task_id,
                    response.status_code,
                    (response.text or "")[:300],
                )
            if attempt < TELEMETRY_REPORT_MAX_ATTEMPTS:
                time.sleep(TELEMETRY_REPORT_RETRY_SECONDS)
        return False

    except requests.exceptions.RequestException as e:
        logger.error(f"🚨 Erro de conexão ao reportar resultado da tarefa {task_id}: {e}")
        return False
    except Exception as e:
        logger.error(f"💥 Erro inesperado ao reportar resultado da tarefa {task_id}: {e}")
        return False


# --- Ciclo de Vida do Agente ---
def agent_lifecycle_job():
    """Job periódico para garantir que o nó esteja registrado e ativo no Context Server."""
    register_with_context_server() # Envia heartbeat de registro

# --- Inicialização ---
if __name__ == '__main__':
    logger.info(f"🖥️ Agente do Nó {NODE_ID} iniciado")
    logger.info(f"🌐 Configurado para se conectar ao Context Server em: {CONTEXT_SERVER_URL}")
    logger.info(f"📡 IP anunciado ao Broker (callback): {NODE_IP}:{NODE_PORT}")
    logger.info(f"🔗 API do agente escutando em 0.0.0.0:{NODE_PORT} (status: http://{NODE_IP}:{NODE_PORT}/status)")

    # Tenta registrar-se no Context Server uma vez ao iniciar
    register_with_context_server()

    # Inicia um agendador para enviar heartbeats de registro periodicamente
    # from apscheduler.schedulers.background import BackgroundScheduler # Já importado acima
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=agent_lifecycle_job, trigger="interval", seconds=MONITOR_INTERVAL_SECONDS)
    scheduler.start()

    # Inicia o servidor Flask do agente
    # host='0.0.0.0': Permite acesso de qualquer IP na rede local.
    app.run(host='0.0.0.0', port=NODE_PORT, debug=False)