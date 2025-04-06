# Use a imagem base Python 3.10
FROM python:3.10-slim

# Diretório de trabalho
WORKDIR /app

# Instala dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia todos os arquivos do cliente
COPY . .

# Cria diretórios para dados persistentes e logs
RUN mkdir -p /data /data/logs
VOLUME /data

# Configuração do ambiente
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV CLIENT_ID=client-1
ENV NODE_ID=1
ENV API_PORT=8080
ENV WEB_PORT=8081
ENV DEBUG=false
ENV DEBUG_LEVEL=basic
ENV PROPOSER=proposer-1:8080
ENV LOG_DIR=/data/logs
ENV NUM_OPERATIONS=0
ENV AUTO_START=false

# Expõe portas para API e servidor web
EXPOSE 8080 8081

# Comando para iniciar a aplicação
CMD ["python", "main.py"]