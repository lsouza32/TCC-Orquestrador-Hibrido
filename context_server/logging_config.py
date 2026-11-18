import logging
import sys
from datetime import datetime
from typing import Optional

class ColoredFormatter(logging.Formatter):
    """Formatter com cores para diferentes níveis de log"""
    
    # Códigos de cores ANSI
    COLORS = {
        'DEBUG': '\033[36m',     # Ciano
        'INFO': '\033[32m',      # Verde
        'WARNING': '\033[33m',   # Amarelo
        'ERROR': '\033[31m',     # Vermelho
        'CRITICAL': '\033[35m',  # Magenta
        'RESET': '\033[0m'       # Reset
    }
    
    def format(self, record):
        # Adiciona cor baseada no nível
        color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        reset = self.COLORS['RESET']
        
        # Formata a mensagem com cor
        record.levelname = f"{color}{record.levelname}{reset}"
        return super().format(record)

class ContextServerLogger:
    """Logger centralizado para o Context Server"""
    
    def __init__(self, name: str, level: str = "INFO"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, level.upper()))
        
        # Evita duplicação de handlers
        if not self.logger.handlers:
            self._setup_handlers()
    
    def _setup_handlers(self):
        """Configura os handlers de log"""
        
        # Remove handlers existentes para evitar duplicação
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
        
        # Handler para console com cores
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        
        # Formatter para console
        console_formatter = ColoredFormatter(
            '%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s',
            datefmt='%H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        
        # Handler para arquivo (opcional)
        file_handler = logging.FileHandler('context_server.log', encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        
        # Formatter para arquivo (sem cores)
        file_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)-20s | %(funcName)-15s:%(lineno)-4d | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        
        # Adiciona handlers
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)
        
        # Evita propagação para root logger
        self.logger.propagate = False
    
    def get_logger(self):
        return self.logger

class LogFilter:
    """Filtros para reduzir spam de logs"""
    
    def __init__(self, max_frequency: int = 30):
        self.last_log_time = {}
        self.max_frequency = max_frequency  # segundos entre logs similares
    
    def should_log(self, message: str, level: str = "INFO") -> bool:
        """Determina se uma mensagem deve ser logada baseada na frequência"""
        now = datetime.now().timestamp()
        key = f"{level}:{message}"
        
        if key not in self.last_log_time:
            self.last_log_time[key] = now
            return True
        
        if now - self.last_log_time[key] >= self.max_frequency:
            self.last_log_time[key] = now
            return True
        
        return False

# Instâncias globais
log_filter = LogFilter(max_frequency=30)

def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    """Função conveniente para obter um logger configurado"""
    logger_config = ContextServerLogger(name, level)
    return logger_config.get_logger()

def setup_logging():
    """Configuração global de logging para o projeto"""
    
    # Evita configuração múltipla
    if hasattr(setup_logging, '_configured'):
        return
    setup_logging._configured = True
    
    # Remove handlers existentes para evitar duplicação
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Configura níveis específicos por módulo
    logging_configs = {
        'context_server': 'INFO',
        'context_server.discovery_monitor.node_monitor': 'WARNING',  # Reduz spam do monitoramento
        'context_server.api.nodes_routes': 'INFO',
        'context_server.api.tasks_routes': 'INFO',
        'context_server.node_allocator.allocator': 'INFO',
        'context_server.database.crud': 'WARNING',  # Reduz logs de DB
        'werkzeug': 'WARNING',  # Reduz logs do Flask
        'urllib3': 'WARNING',   # Reduz logs de requests
        'requests': 'WARNING',  # Reduz logs de requests
        'apscheduler': 'WARNING',  # Reduz logs do scheduler
    }
    
    for module, level in logging_configs.items():
        logger = logging.getLogger(module)
        logger.setLevel(getattr(logging, level.upper()))
        logger.propagate = False  # Evita propagação para root logger
    
    # Configuração global única
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s',
        datefmt='%H:%M:%S',
        force=True  # Força reconfiguração
    )

# Funções de conveniência para logs específicos
def log_task_submission(logger: logging.Logger, task_id: str, task_type: str, filename: str):
    """Log estruturado para submissão de tarefas"""
    logger.info(f"📋 TAREFA SUBMETIDA | ID: {task_id} | Tipo: {task_type} | Arquivo: {filename}")

def log_node_allocation(logger: logging.Logger, task_id: str, node_id: str, requirements: dict):
    """Log estruturado para alocação de nós"""
    logger.info(f"🎯 NÓ ALOCADO | Tarefa: {task_id} | Nó: {node_id} | Requisitos: {requirements}")

def log_monitoring_summary(logger: logging.Logger, active_nodes: int, total_nodes: int):
    """Log resumido para monitoramento"""
    if log_filter.should_log(f"monitoring_summary_{active_nodes}_{total_nodes}"):
        logger.info(f"📊 MONITORAMENTO | Nós ativos: {active_nodes}/{total_nodes}")

def log_node_status(logger: logging.Logger, node_id: str, status: str, resources: dict = None):
    """Log estruturado para status de nós"""
    if resources:
        logger.info(f"🖥️  NÓ {status.upper()} | ID: {node_id} | CPU: {resources.get('cpu', 'N/A')}% | RAM: {resources.get('ram', 'N/A')}GB")
    else:
        logger.info(f"🖥️  NÓ {status.upper()} | ID: {node_id}")

def log_error_with_context(logger: logging.Logger, error: Exception, context: str = ""):
    """Log de erro com contexto adicional"""
    logger.error(f"❌ ERRO | Contexto: {context} | Erro: {str(error)}")

def log_success(logger: logging.Logger, message: str, details: str = ""):
    """Log de sucesso estruturado"""
    if details:
        logger.info(f"✅ SUCESSO | {message} | {details}")
    else:
        logger.info(f"✅ SUCESSO | {message}")

