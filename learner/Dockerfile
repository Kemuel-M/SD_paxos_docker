FROM python:3.10-slim

WORKDIR /app

# Instalar dependências
COPY learner/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fonte
COPY learner/ /app/learner/
COPY common/ /app/common/

# Criar diretório de logs
RUN mkdir -p /app/logs

# Configuração da porta
EXPOSE 8200

# Comandos de execução
CMD ["python", "-m", "learner.main"]