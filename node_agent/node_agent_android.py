from flask import Flask, jsonify
import subprocess
import platform
import socket
import os
import time
import requests
import uuid # Para gerar IDs únicos
from apscheduler.schedulers.background import BackgroundScheduler
import pytz



app = Flask(__name__)

# --- Configurações do Agente ---
# Porta onde o agente do nó vai rodar.
NODE_PORT = 5001 
NODE_IP = "192.168.0.101" # IP_DO_CELULAR
CONTEXT_SERVER_URL = "http://192.168.0.50:5000" 
REGISTRATION_ENDPOINT = f"{CONTEXT_SERVER_URL}/register_node"
MONITOR_INTERVAL_SECONDS = 30 # Intervalo para enviar heartbeats de registro.

# Gera um ID único persistente para este nó Android
NODE_ID_FILE = 'node_id.txt'
NODE_ID = None
if os.path.exists(NODE_ID_FILE):
    with open(NODE_ID_FILE, 'r') as f:
        NODE_ID = f.read().strip()
else:
    NODE_ID = "android-" + str(uuid.uuid4())[:8] # ID mais curto para Android
    with open(NODE_ID_FILE, 'w') as f:
        f.write(NODE_ID)

# --- Funções para Coleta de Hardware (Substituindo psutil) ---
def get_total_hardware_info():
    """
    Coleta informações totais de hardware do nó Android usando comandos nativos.
    As saídas precisam ser analisadas para obter valores numéricos.
    """
    # Exemplo: df -h para disco total. A saída precisa ser processada.
    disk_output = subprocess.getoutput("df -h /data/data/com.termux")
    # Para o protótipo, podemos estimar valores fixos ou extrair com mais lógica.
    # No entanto, a lógica de alocação do servidor dependerá da sua capacidade de analisar essa saída.
    return {
        # O número de núcleos e a RAM total podem ser difíceis de obter de forma confiável
        # e podem ser definidos como valores fixos para o TCC ou a partir de um arquivo.
        "cpu_total_cores": 8, # Exemplo de valor fixo para um celular
        "memory_total_gb": 6.0, # Exemplo de valor fixo para um celular
        "disk_total_gb": subprocess.getoutput("df -h /data/data/com.termux") # Saída bruta para análise
    }

def get_current_hardware_usage():
    """Coleta informações de uso/disponibilidade atual do hardware no Android."""
    try:
        # Comando 'top' para uso da CPU. 'top' no Termux pode ter formato diferente do Linux padrão.
        cpu_output = subprocess.getoutput("top -n 1 | grep '%cpu'")
        
        # Comando 'free' para uso de memória.
        mem_output = subprocess.getoutput("free -m")
        
        # Comando 'df' para uso de disco.
        disk_output = subprocess.getoutput("df -h /data/data/com.termux")

        return {
            "cpu_info_raw": cpu_output,
            "mem_info_raw": mem_output,
            "disk_info_raw": disk_output,
            # Se 'termux-api' estiver instalado e o usuário der permissão
            "battery_info_raw": subprocess.getoutput("termux-battery-status") 
        }
    except Exception as e:
        return {"error": str(e)}

# --- Auto-Registro no Context Server ---
def register_with_context_server():
    """Envia as informações do nó para o Context Server para registro/atualização."""
    print(f"Tentando registrar nó Android com ID: {NODE_ID} no Context Server...")
    total_hw = get_total_hardware_info()
    registration_data = {
        "node_id": NODE_ID,
        "ip_address": NODE_IP,
        "agent_port": NODE_PORT,
        "hostname": "Android-" + socket.gethostname(),
        "cpu_total_cores": total_hw["cpu_total_cores"],
        "memory_total_gb": total_hw["memory_total_gb"],
        "disk_total_gb": total_hw["disk_total_gb"] # Enviando a saída bruta para o Context Server analisar ou para logs
    }
    
    try:
        response = requests.post(REGISTRATION_ENDPOINT, json=registration_data, timeout=5)
        if response.status_code == 200:
            print(f"Registro/Atualização no Context Server bem-sucedido: {response.json()}")
            return True
        else:
            print(f"Erro no registro/atualização (Status: {response.status_code}): {response.text}")
            return False
    except requests.exceptions.ConnectionError:
        print(f"Erro de conexão com o Context Server em {CONTEXT_SERVER_URL}. Verifique o IP/porta.")
        return False
    except requests.exceptions.Timeout:
        print(f"Timeout ao tentar registrar no Context Server em {CONTEXT_SERVER_URL}.")
        return False
    except Exception as e:
        print(f"Erro inesperado ao registrar no Context Server: {e}")
        return False

# --- API do Agente do Nó ---
@app.route('/status', methods=['GET'])
def get_agent_status():
    """
    Endpoint para o Context Server (ou outro consumidor) consultar o status e 
    os recursos LIVRES atuais deste nó.
    """
    try:
        hardware_usage = get_current_hardware_usage()
        
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
            "hardware_usage": hardware_usage
        }), 200
    except Exception as e:
        return jsonify({"status": "erro", "message": str(e), "node_id": NODE_ID}), 500

# --- Ciclo de Vida do Agente ---
def agent_lifecycle_job():
    """Job periódico para garantir que o nó esteja registrado e ativo no Context Server."""
    register_with_context_server() # Envia heartbeat de registro

# --- Inicialização ---
if __name__ == '__main__':
    print(f"Agente do Nó Android {NODE_ID} iniciado.")
    print(f"Configurado para se conectar ao Context Server em: {CONTEXT_SERVER_URL}")
    print(f"Rodando API do agente em http://{NODE_IP}:{NODE_PORT}/status")

    # Tenta registrar-se no Context Server uma vez ao iniciar
    register_with_context_server()

    # Inicia um agendador para enviar heartbeats de registro periodicamente
    scheduler = BackgroundScheduler(timezone=pytz.timezone('America/Sao_Paulo'))
    scheduler.add_job(func=agent_lifecycle_job, trigger="interval", seconds=MONITOR_INTERVAL_SECONDS)
    scheduler.start()

    # Inicia o servidor Flask do agente
    # host='0.0.0.0': Permite acesso de qualquer IP na rede local.
    app.run(host='0.0.0.0', port=NODE_PORT, debug=False)