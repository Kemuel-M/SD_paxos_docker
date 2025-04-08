FROM python:3.10-slim

WORKDIR /app

# Instalar dependências
COPY acceptor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fonte
COPY acceptor/ /app/acceptor/
COPY common/ /app/common/

# Criar diretório de logs
RUN mkdir -p /app/logs

# Configuração da porta
EXPOSE 8100

# Comandos de execução
CMD ["python", "-m", "acceptor.main"]