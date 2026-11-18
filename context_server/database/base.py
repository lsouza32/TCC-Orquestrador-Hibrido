import sqlite3 # Importa a biblioteca padrão do Python para interagir com bancos de dados SQLite.
import time    # Importa o módulo time para trabalhar com timestamps (usado para registrar a hora).
import os      # Importa o módulo os para interagir com o sistema operacional (não diretamente usado aqui, mas útil para caminhos).
import json

# --- Funções de Conexão e Inicialização do Banco de Dados ---

def get_db_connection(db_path):
    """
    Estabelece e retorna uma conexão com o banco de dados SQLite.
    """
    conn = sqlite3.connect(db_path)
    # Define o row_factory para que as linhas retornadas pelas consultas SQL
    # sejam acessíveis como dicionários (ex: row['node_id']), em vez de tuplas.
    conn.row_factory = sqlite3.Row 
    return conn

def init_db(db_path):
    """
    Inicializa o banco de dados, criando a tabela 'nodes' e 'tasks' se elas ainda não existirem.
    """
    conn = get_db_connection(db_path)
    try:
        # Executa um comando SQL para criar a tabela 'nodes'.
        # IF NOT EXISTS garante que a tabela só será criada se não existir, 
        # evitando erros em execuções subsequentes.
        conn.execute('''
            CREATE TABLE IF NOT EXISTS nodes (
                node_id TEXT PRIMARY KEY,        -- Identificador único do nó (chave primária)
                ip_address TEXT NOT NULL,      -- Endereço IP do nó (não pode ser nulo)
                agent_port INTEGER NOT NULL,   -- Porta onde o agente do nó está escutando (não pode ser nulo)
                hostname TEXT,                 -- Nome do host do nó (opcional)
                cpu_total_threads INTEGER,       -- Número total de threads da CPU do nó
                memory_total_gb REAL,          -- Memória RAM total do nó em GB
                disk_total_gb REAL,            -- Espaço em disco total do nó em GB
                
                cpu_available_percent REAL,    -- Porcentagem de CPU disponível (ex: 100 - uso)
                memory_available_gb REAL,      -- Memória RAM disponível em GB
                disk_available_gb REAL,        -- Espaço em disco disponível em GB
                last_resource_check REAL,      -- Timestamp da última checagem de recursos disponíveis
                
                cpu_reserved_threads REAL DEFAULT 0, -- Threads de CPU reservadas pelo orquestrador
                memory_reserved_gb REAL DEFAULT 0,    -- Memória reservada (GB)
                disk_reserved_gb REAL DEFAULT 0,      -- Espaço em disco reservado (GB)
                
                last_registered_at REAL,       -- Timestamp da última vez que o nó se registrou/enviou um heartbeat de registro
                is_active BOOLEAN,             -- Booleano indicando se o nó está ativo/online (atualizado pelo ping)
                last_checked_at REAL,          -- Timestamp da última verificação de monitoramento (ping)
                current_status_message TEXT    -- Mensagem detalhada sobre o status atual do nó (diagnóstico)
            )
        ''')
        
        # Cria a tabela 'tasks' para gerenciar tarefas atribuídas aos nós.
        conn.execute('''
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY,
                task_type TEXT NOT NULL,
                filename TEXT,
                file_path TEXT,
                status TEXT NOT NULL,
                submitted_at REAL NOT NULL,
                allocated_node_id TEXT,
                user_id TEXT NOT NULL,
                required_cpu_threads REAL,
                required_memory_gb REAL,
                required_disk_gb REAL,
                FOREIGN KEY (allocated_node_id) REFERENCES nodes(node_id)
            )
        ''')
        
        # Garante que bases antigas recebam as novas colunas de forma idempotente
        _ensure_column(conn, 'nodes', 'cpu_reserved_threads', "ALTER TABLE nodes ADD COLUMN cpu_reserved_threads REAL DEFAULT 0")
        _ensure_column(conn, 'nodes', 'memory_reserved_gb', "ALTER TABLE nodes ADD COLUMN memory_reserved_gb REAL DEFAULT 0")
        _ensure_column(conn, 'nodes', 'disk_reserved_gb', "ALTER TABLE nodes ADD COLUMN disk_reserved_gb REAL DEFAULT 0")
        _ensure_column(conn, 'nodes', 'provider_type', "ALTER TABLE nodes ADD COLUMN provider_type TEXT DEFAULT 'node_agent'")
        _ensure_column(conn, 'nodes', 'compute_layer', "ALTER TABLE nodes ADD COLUMN compute_layer TEXT")
        _ensure_column(conn, 'nodes', 'cpu_used_percent', "ALTER TABLE nodes ADD COLUMN cpu_used_percent REAL")
        _ensure_column(conn, 'nodes', 'memory_used_percent', "ALTER TABLE nodes ADD COLUMN memory_used_percent REAL")
        _ensure_column(conn, 'nodes', 'disk_used_percent', "ALTER TABLE nodes ADD COLUMN disk_used_percent REAL")
        _ensure_column(conn, 'tasks', 'required_cpu_threads', "ALTER TABLE tasks ADD COLUMN required_cpu_threads REAL")
        _ensure_column(conn, 'tasks', 'required_memory_gb', "ALTER TABLE tasks ADD COLUMN required_memory_gb REAL")
        _ensure_column(conn, 'tasks', 'required_disk_gb', "ALTER TABLE tasks ADD COLUMN required_disk_gb REAL")

        # Cria a tabela 'task_results' para armazenar resultados e logs de execução das tarefas.
        conn.execute('''
            CREATE TABLE IF NOT EXISTS task_results (
                task_id TEXT PRIMARY KEY,
                status_final TEXT NOT NULL,
                duration_seconds REAL NOT NULL,
                stdout_log TEXT,
                stderr_log TEXT,
                completed_at REAL NOT NULL,
                FOREIGN KEY (task_id) REFERENCES tasks(task_id)
            )
        ''')
        # Telemetria de execução (uso real para calibrar futuras alocações)
        _ensure_column(conn, 'task_results', 'cpu_peak_percent', "ALTER TABLE task_results ADD COLUMN cpu_peak_percent REAL")
        _ensure_column(conn, 'task_results', 'ram_peak_mb', "ALTER TABLE task_results ADD COLUMN ram_peak_mb REAL")
        _ensure_column(conn, 'task_results', 'detected_libraries', "ALTER TABLE task_results ADD COLUMN detected_libraries TEXT")
        _ensure_column(conn, 'task_results', 'prediction_delta_ram', "ALTER TABLE task_results ADD COLUMN prediction_delta_ram REAL")

        _ensure_column(conn, 'tasks', 'generated_requirements_txt', "ALTER TABLE tasks ADD COLUMN generated_requirements_txt TEXT")
        _ensure_column(conn, 'tasks', 'docker_image', "ALTER TABLE tasks ADD COLUMN docker_image TEXT")
        _ensure_column(conn, 'tasks', 'ai_detected_libraries', "ALTER TABLE tasks ADD COLUMN ai_detected_libraries TEXT")
        _ensure_column(conn, 'tasks', 'ai_category', "ALTER TABLE tasks ADD COLUMN ai_category TEXT")
        _ensure_column(conn, 'tasks', 'ai_raw_response', "ALTER TABLE tasks ADD COLUMN ai_raw_response TEXT")
        _ensure_column(conn, 'tasks', 'ai_confidence_score', "ALTER TABLE tasks ADD COLUMN ai_confidence_score REAL")
        _ensure_column(conn, 'tasks', 'ai_image_source', "ALTER TABLE tasks ADD COLUMN ai_image_source TEXT")
        _ensure_column(conn, 'tasks', 'ai_catalog_match_score', "ALTER TABLE tasks ADD COLUMN ai_catalog_match_score REAL")
        _ensure_column(conn, 'tasks', 'ai_telemetry_samples', "ALTER TABLE tasks ADD COLUMN ai_telemetry_samples INTEGER")

        # Métricas de avaliação (TCC II): latências e camadas queryáveis no SQLite
        _ensure_column(conn, 'tasks', 'planning_completed_at', "ALTER TABLE tasks ADD COLUMN planning_completed_at REAL")
        _ensure_column(conn, 'tasks', 'dispatched_at', "ALTER TABLE tasks ADD COLUMN dispatched_at REAL")
        _ensure_column(conn, 'tasks', 'preferred_compute_layer', "ALTER TABLE tasks ADD COLUMN preferred_compute_layer TEXT")
        _ensure_column(conn, 'tasks', 'reallocation_attempts', "ALTER TABLE tasks ADD COLUMN reallocation_attempts INTEGER DEFAULT 0")

        # Catálogo de imagens Docker (fonte determinística para seleção de runtime)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS docker_images_catalog (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                image_name TEXT NOT NULL UNIQUE,
                libraries TEXT NOT NULL,
                min_ram_gb REAL NOT NULL,
                min_cpu REAL NOT NULL,
                min_disk_gb REAL DEFAULT 1.0,
                description TEXT
            )
        ''')
        _ensure_column(conn, 'docker_images_catalog', 'min_disk_gb', "ALTER TABLE docker_images_catalog ADD COLUMN min_disk_gb REAL DEFAULT 1.0")
        _seed_docker_images_catalog(conn)

        conn.execute(
            "UPDATE nodes SET provider_type = 'node_agent' "
            "WHERE provider_type IS NULL OR TRIM(COALESCE(provider_type, '')) = ''"
        )

        conn.commit()
        print(f"Banco de dados '{db_path}' inicializado.")
    except sqlite3.Error as e:
        print(f"Erro ao inicializar o banco de dados: {e}")
    finally:
        conn.close()

def _ensure_column(conn, table_name, column_name, alter_statement):
    """Adiciona a coluna informada se ela ainda não existir."""
    cursor = conn.execute(f"PRAGMA table_info({table_name})")
    columns = {row[1] for row in cursor.fetchall()}
    if column_name not in columns:
        conn.execute(alter_statement)
        conn.commit()


def _seed_docker_images_catalog(conn):
    """
    Seed idempotente do catálogo de imagens.
    """
    conn.execute("DELETE FROM docker_images_catalog WHERE image_name = 'tcc-tf-ml:latest'")

    catalog_seed = [
        {
            "image_name": "tensorflow/tensorflow:latest",
            "libraries": ["tensorflow", "keras", "numpy", "h5py", "scikit-learn", "matplotlib", "seaborn", "opencv-python-headless", "kagglehub"],
            "min_ram_gb": 4.0,
            "min_cpu": 4.0,
            "min_disk_gb": 12.0,
            "description": "Deep Learning / ML Pesado: Treinamento de redes neurais convolucionais (CNNs), visão computacional e datasets pesados.",
        },
        {
            "image_name": "pytorch/pytorch:latest",
            "libraries": ["torch", "torchvision", "torchaudio", "numpy", "scikit-learn", "tqdm", "pillow", "scipy"],
            "min_ram_gb": 4.0,
            "min_cpu": 4.0,
            "min_disk_gb": 12.0,
            "description": "Deep Learning PyTorch: Treinamento e inferência de redes neurais profundas PyTorch, visão computacional e tensores.",
        },
        {
            "image_name": "huggingface/transformers-cpu:latest",
            "libraries": ["transformers", "datasets", "accelerate", "tokenizers", "torch", "sentencepiece", "huggingface-hub"],
            "min_ram_gb": 8.0,
            "min_cpu": 4.0,
            "min_disk_gb": 15.0,
            "description": "NLP / Modelos de Linguagem (LLMs): Processamento de linguagem natural, fine-tuning, embeddings e geração de texto via Transformers.",
        },
        {
            "image_name": "python:3.11-bullseye",
            "libraries": ["cv2", "opencv-python", "opencv-python-headless", "pillow", "scikit-image", "numpy", "matplotlib", "imageio"],
            "min_ram_gb": 4.0,
            "min_cpu": 2.0,
            "min_disk_gb": 6.0,
            "description": "Visão Computacional Nativa: Processamento de imagens/vídeos com dependências nativas C++ (OpenCV/Debian).",
        },
        {
            "image_name": "jupyter/datascience-notebook",
            "libraries": ["pandas", "polars", "numpy", "scipy", "scikit-learn", "seaborn", "matplotlib", "statsmodels", "openpyxl"],
            "min_ram_gb": 3.0,
            "min_cpu": 2.0,
            "min_disk_gb": 5.0,
            "description": "Data Science & ETL Tabular: Análise de dados, engenharia de dados clássica, estatística e machine learning tradicional.",
        },
        {
            "image_name": "bitnami/spark:latest",
            "libraries": ["pyspark", "pyarrow", "duckdb", "pandas", "numpy", "fastparquet"],
            "min_ram_gb": 4.0,
            "min_cpu": 4.0,
            "min_disk_gb": 8.0,
            "description": "Big Data & Processamento Distribuído: Manipulação de grandes volumes de dados (PySpark, Parquet, DuckDB).",
        },
        {
            "image_name": "joyzoursky/python-chromedriver:latest",
            "libraries": ["selenium", "playwright", "beautifulsoup4", "bs4", "scrapy", "requests", "urllib3", "lxml"],
            "min_ram_gb": 2.0,
            "min_cpu": 2.0,
            "min_disk_gb": 4.0,
            "description": "Web Scraping & Browser Automation: Automação de navegadores headless (Chromium/Selenium), crawlers e scraping DOM.",
        },
        {
            "image_name": "python:3.11-slim",
            "libraries": ["requests", "httpx", "flask", "fastapi", "uvicorn", "pydantic", "json", "datetime", "schedule"],
            "min_ram_gb": 0.5,
            "min_cpu": 1.0,
            "min_disk_gb": 2.0,
            "description": "Runtime Python enxuto para automações e scripts gerais.",
        },
        ]

    for row in catalog_seed:
        conn.execute(
            '''
            INSERT INTO docker_images_catalog (image_name, libraries, min_ram_gb, min_cpu, min_disk_gb, description)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(image_name) DO UPDATE SET
                libraries=excluded.libraries,
                min_ram_gb=excluded.min_ram_gb,
                min_cpu=excluded.min_cpu,
                min_disk_gb=excluded.min_disk_gb,
                description=excluded.description
            ''',
            (
                row["image_name"],
                json.dumps(row["libraries"], ensure_ascii=False),
                row["min_ram_gb"],
                row["min_cpu"],
                row["min_disk_gb"],
                row["description"],
            ),
        )
    conn.commit()
