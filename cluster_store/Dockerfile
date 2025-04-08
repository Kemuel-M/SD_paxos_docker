FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY cluster_store/requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy code
COPY cluster_store/ /app/cluster_store/
COPY common/ /app/common/

# Create persistent data and logs directories
RUN mkdir -p /data/resources /data/logs
VOLUME /data

# Environment configuration
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV NODE_ID=1
ENV PORT=8080
ENV DEBUG=false
ENV DEBUG_LEVEL=basic
ENV DATA_DIR=/data/resources
ENV LOG_DIR=/data/logs

# Adicionar diretório ao PYTHONPATH
ENV PYTHONPATH="/app:${PYTHONPATH}"

# Expose port
EXPOSE 8080

# Define entry command
CMD ["python", "-m", "cluster_store.main"]