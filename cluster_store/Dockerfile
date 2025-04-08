FROM python:3.10-slim

WORKDIR /app

# Instalar dependências
COPY cluster_store/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fonte
COPY cluster_store/ /app/cluster_store/
COPY common/ /app/common/

# Criar diretório de logs e dados
RUN mkdir -p /app/logs /app/data

# Configuração da porta
EXPOSE 8300

# Comandos de execução
CMD ["python", "-m", "cluster_store.main"]