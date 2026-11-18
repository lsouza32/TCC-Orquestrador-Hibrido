import os
from flask import Flask
from flask_cors import CORS

from context_server import config
from context_server.logging_config import get_logger

# Configuração do sistema de logging
logger = get_logger(__name__)


def create_app():
    """
    Application Factory Pattern para criação da instância Flask.
    
    Centraliza a configuração da aplicação, CORS e registro de blueprints.
    Permite criação de múltiplas instâncias para testes e diferentes ambientes.
    
    Returns:
        Flask: Instância configurada da aplicação Flask
    """
    # Criação da instância Flask
    app = Flask(__name__)
    
    # Configuração do CORS para permitir requisições do frontend
    CORS(app, resources={r"/*": {
        "origins": [
            config.CONTEXT_SERVER_IP,
            f"{config.CONTEXT_SERVER_IP}:80",
            f"{config.CONTEXT_SERVER_IP}:8080",
            "http://localhost",
            "http://localhost:80",
            "http://localhost:8080",
            "http://127.0.0.1",
            "http://127.0.0.1:80",
            "http://127.0.0.1:8080"
        ]
    }})
    
    # Registro dos Blueprints
    from context_server.api import register_blueprints
    register_blueprints(app)
    
    logger.debug("🏭 Application Factory: Aplicação Flask criada e configurada")
    
    return app
