FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY client/requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy code
COPY client/ /app/
COPY common/ /app/common/

# Create persistent data and logs directories
RUN mkdir -p /data /data/logs
VOLUME /data

# Environment configuration
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV CLIENT_ID=client-1
ENV NODE_ID=1
ENV API_PORT=8080
ENV WEB_PORT=8081
ENV DEBUG=false
ENV DEBUG_LEVEL=basic
ENV PROPOSER=proposer-1:8080
ENV LOG_DIR=/data/logs
ENV NUM_OPERATIONS=0
ENV AUTO_START=false

# Expose ports for API and web server
EXPOSE 8080 8081

# Command to start the application
CMD ["python", "main.py"]