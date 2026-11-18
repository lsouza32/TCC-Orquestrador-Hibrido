"""
Ponto de entrada da aplicação Context Server.

Contém toda a lógica de inicialização: scheduler, banco de dados e execução da aplicação.
Separação clara entre criação da app (__init__.py) e execução (run.py).
"""

import os
import logging

from apscheduler.executors.pool import ThreadPoolExecutor as SchedulerThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler

from context_server import create_app, config
from context_server.database import crud
from context_server.discovery_monitor import node_monitor
from context_server.logging_config import get_logger
from context_server.queue_processor import process_queue_job
from context_server.services.k8s_job_executor import configure_kubernetes
from context_server.services.k8s_cleanup import clean_unused_images_job
from context_server.services.k8s_watcher import sync_k8s_state

# Configuração do sistema de logging
logger = get_logger(__name__)

# Configuração de logging simplificada
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s',
    datefmt='%H:%M:%S',
    force=True
)

# Reduz verbosidade do APScheduler
logging.getLogger('apscheduler').setLevel(logging.WARNING)
logging.getLogger('apscheduler.scheduler').setLevel(logging.WARNING)
logging.getLogger('apscheduler.executors').setLevel(logging.WARNING)
logging.getLogger('apscheduler.jobstores').setLevel(logging.WARNING)
logging.getLogger('werkzeug').setLevel(logging.WARNING)


def main():
    """
    Função principal de inicialização e execução da aplicação.
    
    Responsabilidades:
    1. Criar pasta de uploads se não existir
    2. Inicializar banco de dados
    3. Configurar e iniciar scheduler de monitoramento
    4. Executar aplicação Flask
    """
    logger.info("🚀 Iniciando Context Server...")
    
    # Cria a pasta de uploads se não existir
    if not os.path.exists(config.UPLOAD_FOLDER):
        os.makedirs(config.UPLOAD_FOLDER)
        logger.info(f"📁 Pasta de uploads criada: {config.UPLOAD_FOLDER}")
    else:
        logger.debug(f"📁 Pasta de uploads já existe: {config.UPLOAD_FOLDER}")

    # Cria a pasta base de exportação de resultados (relatórios/planilhas por tarefa)
    if hasattr(config, "EXPORTS_BASE"):
        os.makedirs(config.EXPORTS_BASE, exist_ok=True)
        logger.info(f"📂 Pasta de exportações: {config.EXPORTS_BASE}")
    
    # Inicializa banco de dados
    crud.init_db(config.DATABASE_PATH)
    logger.info(f"🗄️ Banco de dados inicializado: {config.DATABASE_PATH}")

    # Kubernetes client (Kind: kubeconfig no host ou variável KUBECONFIG)
    configure_kubernetes()
    
    # Pool maior evita que um job longo (ex.: K8s API lenta) bloqueie todos os demais
    scheduler = BackgroundScheduler(
        executors={"default": SchedulerThreadPoolExecutor(max_workers=16)},
        job_defaults={"coalesce": True, "misfire_grace_time": 120},
    )
    scheduler.add_job(
        func=lambda: node_monitor.monitor_nodes_job(config.DATABASE_PATH),
        trigger="interval",
        seconds=config.MONITOR_INTERVAL_SECONDS,
        id="node_monitor_job",
        max_instances=2,
    )
    scheduler.add_job(
        func=lambda: process_queue_job(config.DATABASE_PATH),
        trigger="interval",
        seconds=max(15, config.MONITOR_INTERVAL_SECONDS),
        id="task_queue_job",
        max_instances=2,
    )
    scheduler.add_job(
        func=sync_k8s_state,
        trigger="interval",
        seconds=10,
        args=[config.DATABASE_PATH],
        id="k8s_watcher_job",
        max_instances=3,
        misfire_grace_time=300,
        coalesce=True,
    )
    scheduler.add_job(
        func=lambda: clean_unused_images_job(config.DATABASE_PATH),
        trigger="cron",
        hour=3,
        minute=0,
        id="docker_image_cleanup_daily",
        max_instances=1,
        replace_existing=True,
    )
    scheduler.start()
    logger.info(f"⏰ Scheduler de monitoramento ativo (intervalo: {config.MONITOR_INTERVAL_SECONDS}s)")
    logger.info("🧹 Job diário de limpeza de imagens Docker agendado (cron 03:00)")
    logger.info("📋 Fila de tarefas (Kubernetes) processada periodicamente e ao receber /register_node")
    
    # Cria aplicação Flask
    app = create_app()
    
    # Logs finais organizados
    logger.info("=" * 60)
    logger.info("🎉 CONTEXT SERVER INICIADO COM SUCESSO!")
    logger.info(f"🌐 Servidor: http://0.0.0.0:{config.PORTA_SERVER}")
    logger.info(f"📊 Monitoramento: {config.MONITOR_INTERVAL_SECONDS}s")
    logger.info(f"💾 Banco: {config.DATABASE_PATH}")
    logger.info(f"📁 Uploads: {config.UPLOAD_FOLDER}")
    if hasattr(config, "EXPORTS_BASE"):
        logger.info(f"📂 Exportações: {config.EXPORTS_BASE}")
    logger.info("=" * 60)
    
    # Executa aplicação Flask (sem logs verbosos)
    app.run(host='0.0.0.0', port=config.PORTA_SERVER, debug=False, use_reloader=False)


if __name__ == '__main__':
    main()
