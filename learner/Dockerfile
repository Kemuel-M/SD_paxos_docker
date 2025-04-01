FROM python:3.10-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Diretórios para dados persistentes e sistema de arquivos compartilhado
RUN mkdir -p /app/data /app/shared
VOLUME /app/data
VOLUME /app/shared

EXPOSE 8080

CMD ["uvicorn", "learner.main:app", "--host", "0.0.0.0", "--port", "8080"]
