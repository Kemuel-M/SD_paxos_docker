FROM python:3.10-slim

WORKDIR /app

# Copiar os arquivos de requisitos
COPY client/requirements.txt .

# Instalar dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código-fonte
COPY client/ .

# Comando para iniciar o cliente
CMD ["python", "client.py"]