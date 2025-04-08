FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY learner/requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy code
COPY learner/ /app/learner/
COPY common/ /app/common/

# Create persistent data and logs directories
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
ENV USE_CLUSTER_STORE=false

# Expose port
EXPOSE 8080

# Set working directory
WORKDIR /app/learner

# Command to start the application
CMD ["python", "main.py"]