FROM python:3.10-slim

WORKDIR /app

# Instala dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia código fonte
COPY . .

# Cria diretório para dados persistentes
RUN mkdir -p /data
VOLUME /data

# Configuração do ambiente
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PORT=8080
ENV NODE_ID=1
ENV DEBUG=false

# Expõe a porta
EXPOSE 8080

# Comando para iniciar a aplicação
CMD ["python", "main.py"]