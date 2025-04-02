FROM python:3.10-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Diretório para dados persistentes
RUN mkdir -p /app/data /app/resource
VOLUME /app/data
VOLUME /app/resource

EXPOSE 8080

CMD ["uvicorn", "store.main:app", "--host", "0.0.0.0", "--port", "8080"]