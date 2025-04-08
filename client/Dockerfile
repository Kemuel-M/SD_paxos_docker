FROM python:3.10-slim

WORKDIR /app

# Instalar dependências
COPY client/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fonte
COPY client/ /app/client/
COPY common/ /app/common/

# Criar diretório de logs
RUN mkdir -p /app/logs

# Configuração das portas
EXPOSE 8400 8500

# Comandos de execução
CMD ["python", "-m", "client.main"]