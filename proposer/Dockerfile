FROM python:3.10-slim

WORKDIR /app

# Instalar dependências
COPY proposer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fonte
COPY proposer/ /app/proposer/
COPY common/ /app/common/

# Criar diretório de logs
RUN mkdir -p /app/logs

# Configuração da porta
EXPOSE 8000

# Comandos de execução
CMD ["python", "-m", "proposer.main"]