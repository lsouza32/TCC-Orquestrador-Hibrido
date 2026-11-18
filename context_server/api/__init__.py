"""
Módulo de APIs do Context Server.

Centraliza o registro de todos os blueprints da aplicação.
Facilita a manutenção e organização das rotas da API.
"""

from context_server.api.nodes_routes import nodes_bp
from context_server.api.tasks_routes import tasks_bp
from context_server.api.admin_routes import admin_bp
from context_server.logging_config import get_logger

logger = get_logger(__name__)


def register_blueprints(app):
    """
    Registra todos os blueprints da API na aplicação Flask.
    
    Args:
        app (Flask): Instância da aplicação Flask
    """
    # Registro dos Blueprints
    app.register_blueprint(nodes_bp)
    app.register_blueprint(tasks_bp)
    app.register_blueprint(admin_bp)
    
    logger.debug("🔗 Blueprints registrados: nodes_bp, tasks_bp, admin_bp")
