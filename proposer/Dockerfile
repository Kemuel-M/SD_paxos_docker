FROM python:3.10-slim

WORKDIR /app

# Install utilities
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY proposer/requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create proper structure
RUN mkdir -p /app/proposer

# Copy code - make sure files go to right locations
COPY proposer/__init__.py /app/proposer/
COPY proposer/config.py /app/proposer/
COPY proposer/persistence.py /app/proposer/
COPY proposer/proposer.py /app/proposer/
COPY proposer/leader.py /app/proposer/
COPY proposer/main.py /app/
COPY common/ /app/common/

# Create data directory
RUN mkdir -p /app/data
VOLUME /app/data

# Make startup script executable
RUN chmod +x startup.sh

# Expose port
EXPOSE 8080

# Start command
CMD ["./startup.sh"]