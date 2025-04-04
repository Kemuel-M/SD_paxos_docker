# File: acceptor/Dockerfile
# Container configuration for the Acceptor component

FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Create directory for persistent data
RUN mkdir -p /data
VOLUME /data

# Environment configuration
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PORT=8080
ENV NODE_ID=1
ENV DEBUG=false

# Expose port
EXPOSE 8080

# Command to start the application
CMD ["python", "main.py"]