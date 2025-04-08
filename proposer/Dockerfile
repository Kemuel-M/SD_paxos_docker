FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY proposer/requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy code
COPY proposer/ /app/proposer/
COPY common/ /app/common/

# Create persistent data directory
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

# Set working directory
WORKDIR /app/proposer

# Command to start the application
CMD ["python", "main.py"]