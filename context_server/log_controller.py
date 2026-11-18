#!/usr/bin/env python3
"""
Controlador de Logs para o Context Server
Permite ajustar níveis de log em tempo de execução
"""

import logging
import yaml
import os
from typing import Dict, Any

class LogController:
    """Controlador centralizado para gerenciar logs do sistema"""
    
    def __init__(self, config_file: str = "log_config.yaml"):
        self.config_file = config_file
        self.config = self._load_config()
        self._setup_logging()
    
    def _load_config(self) -> Dict[str, Any]:
        """Carrega configuração do arquivo YAML"""
        if os.path.exists(self.config_file):
            with open(self.config_file, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        else:
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Retorna configuração padrão se arquivo não existir"""
        return {
            'modules': {
                'context_server.main': 'INFO',
                'context_server.discovery_monitor.node_monitor': 'WARNING',
                'context_server.api.nodes_routes': 'INFO',
                'context_server.api.tasks_routes': 'INFO',
                'context_server.node_allocator.allocator': 'INFO',
                'context_server.database.crud': 'WARNING',
                'werkzeug': 'WARNING',
                'urllib3': 'WARNING',
                'requests': 'WARNING',
                'flask': 'WARNING'
            },
            'global': {
                'level': 'INFO',
                'format': '%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s',
                'date_format': '%H:%M:%S',
                'file_logging': True,
                'log_file': 'context_server.log',
                'colored_output': True,
                'frequency_filter': 30
            }
        }
    
    def _setup_logging(self):
        """Configura o sistema de logging baseado na configuração"""
        # Configura nível global
        global_level = getattr(logging, self.config['global']['level'].upper())
        logging.basicConfig(level=global_level)
        
        # Configura níveis por módulo
        for module, level in self.config['modules'].items():
            logger = logging.getLogger(module)
            logger.setLevel(getattr(logging, level.upper()))
    
    def set_module_level(self, module: str, level: str):
        """Define o nível de log para um módulo específico"""
        if level.upper() in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
            logger = logging.getLogger(module)
            logger.setLevel(getattr(logging, level.upper()))
            self.config['modules'][module] = level.upper()
            print(f"✅ Nível de log do módulo '{module}' alterado para {level.upper()}")
        else:
            print(f"❌ Nível inválido: {level}. Use: DEBUG, INFO, WARNING, ERROR, CRITICAL")
    
    def set_global_level(self, level: str):
        """Define o nível global de log"""
        if level.upper() in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
            logging.getLogger().setLevel(getattr(logging, level.upper()))
            self.config['global']['level'] = level.upper()
            print(f"✅ Nível global de log alterado para {level.upper()}")
        else:
            print(f"❌ Nível inválido: {level}. Use: DEBUG, INFO, WARNING, ERROR, CRITICAL")
    
    def show_status(self):
        """Mostra o status atual dos níveis de log"""
        print("\n📊 Status dos Logs:")
        print("=" * 50)
        
        print(f"🌐 Nível Global: {self.config['global']['level']}")
        print("\n📋 Níveis por Módulo:")
        
        for module, level in self.config['modules'].items():
            current_level = logging.getLogger(module).level
            level_name = logging.getLevelName(current_level)
            print(f"  {module:<40} | {level_name}")
    
    def enable_verbose_mode(self):
        """Ativa modo verboso (DEBUG para todos os módulos)"""
        for module in self.config['modules']:
            self.set_module_level(module, 'DEBUG')
        print("🔍 Modo verboso ativado - todos os módulos em DEBUG")
    
    def enable_quiet_mode(self):
        """Ativa modo silencioso (WARNING para módulos principais)"""
        quiet_modules = [
            'context_server.discovery_monitor.node_monitor',
            'context_server.database.crud',
            'werkzeug',
            'urllib3',
            'requests',
            'flask'
        ]
        
        for module in quiet_modules:
            if module in self.config['modules']:
                self.set_module_level(module, 'WARNING')
        
        print("🔇 Modo silencioso ativado - logs reduzidos")
    
    def reset_to_default(self):
        """Reseta para configuração padrão"""
        self.config = self._get_default_config()
        self._setup_logging()
        print("🔄 Configuração de logs resetada para padrão")

def main():
    """Função principal para interação via linha de comando"""
    controller = LogController()
    
    print("🎛️  Controlador de Logs do Context Server")
    print("=" * 50)
    
    while True:
        print("\nOpções disponíveis:")
        print("1. Mostrar status atual")
        print("2. Alterar nível de módulo específico")
        print("3. Alterar nível global")
        print("4. Ativar modo verboso")
        print("5. Ativar modo silencioso")
        print("6. Resetar para padrão")
        print("0. Sair")
        
        choice = input("\nEscolha uma opção: ").strip()
        
        if choice == '0':
            print("👋 Até logo!")
            break
        elif choice == '1':
            controller.show_status()
        elif choice == '2':
            module = input("Nome do módulo: ").strip()
            level = input("Novo nível (DEBUG/INFO/WARNING/ERROR/CRITICAL): ").strip()
            controller.set_module_level(module, level)
        elif choice == '3':
            level = input("Novo nível global (DEBUG/INFO/WARNING/ERROR/CRITICAL): ").strip()
            controller.set_global_level(level)
        elif choice == '4':
            controller.enable_verbose_mode()
        elif choice == '5':
            controller.enable_quiet_mode()
        elif choice == '6':
            controller.reset_to_default()
        else:
            print("❌ Opção inválida!")

if __name__ == "__main__":
    main()

