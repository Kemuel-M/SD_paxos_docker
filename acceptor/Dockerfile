FROM python:3.10-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Diretório para dados persistentes
RUN mkdir -p /app/data
VOLUME /app/data

EXPOSE 8080

CMD ["uvicorn", "acceptor.main:app", "--host", "0.0.0.0", "--port", "8080"]
