# Use a imagem base Python 3.10
FROM python:3.10-slim

# Diretório de trabalho
WORKDIR /app

# Instala dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código-fonte
COPY . /app/cluster_store
COPY ../common /app/common

# Cria diretórios para dados persistentes e logs
RUN mkdir -p /data/resources /data/logs
VOLUME /data

# Configuração do ambiente
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV NODE_ID=1
ENV PORT=8080
ENV DEBUG=false
ENV DEBUG_LEVEL=basic
ENV DATA_DIR=/data/resources
ENV LOG_DIR=/data/logs

# Expõe a porta do servidor
EXPOSE 8080

# Define o comando de entrada
CMD ["python", "-m", "cluster_store.main"]