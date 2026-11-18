# Usa uma imagem base Python leve e otimizada para containers
FROM python:3.11-slim

# Instala o pacote iputils-ping que contém o comando 'ping'
RUN apt-get update && apt-get install -y iputils-ping && rm -rf /var/lib/apt/lists/*

# Define o diretório de trabalho dentro do container como /app
WORKDIR /app

# Copia o arquivo de requisitos para o container
COPY requirements.txt .

# Instala as dependências Python
RUN pip install --no-cache-dir -r requirements.txt

# Copia toda a pasta 'context_server' para o diretório de trabalho do container
COPY . ./context_server/

# Expõe a porta que o Flask vai usar
EXPOSE 5000

# Comando para rodar a aplicação quando o container iniciar
# Usa o novo ponto de entrada refatorado (Application Factory Pattern)
# Adiciona o diretório pai ao PYTHONPATH para resolver imports
ENV PYTHONPATH=/app
CMD ["python", "-m", "context_server.run"]