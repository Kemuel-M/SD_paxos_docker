# File: acceptor/Dockerfile
# Container configuration for the Acceptor component

FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY acceptor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy common modules
COPY common /app/common

# Copy acceptor code
COPY acceptor /app/acceptor

# Create directories for persistent data and logs
RUN mkdir -p /data /data/logs
VOLUME /data

# Environment configuration
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PORT=8080
ENV NODE_ID=1
ENV DEBUG=false
ENV DEBUG_LEVEL=basic
ENV LOG_DIR=/data/logs
ENV DATA_DIR=/data

# Expose port
EXPOSE 8080

# Working directory for execution
WORKDIR /app/acceptor

# Command to start the application
CMD ["python", "main.py"]