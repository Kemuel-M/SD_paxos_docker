#!/bin/bash

# Script para criar a estrutura de diretórios e arquivos para o sistema Paxos
# Executa no diretório atual do projeto
echo "Criando estrutura de diretórios e arquivos para o sistema Paxos no diretório atual..."

# Criar docker-compose.yml
cat > docker-compose.yml << 'EOL'
version: '3.8'

services:
  # Proposers
  proposer-1:
    build:
      context: ./proposer
      dockerfile: Dockerfile
    environment:
      - NODE_ID=1
      - NODE_ROLE=proposer
      - QUORUM_SIZE=3
    volumes:
      - proposer_1_data:/app/data
    ports:
      - "8081:8080"
    networks:
      - paxos_network
  
  proposer-2:
    build:
      context: ./proposer
      dockerfile: Dockerfile
    environment:
      - NODE_ID=2
      - NODE_ROLE=proposer
      - QUORUM_SIZE=3
    volumes:
      - proposer_2_data:/app/data
    ports:
      - "8082:8080"
    networks:
      - paxos_network

  proposer-3:
    build:
      context: ./proposer
      dockerfile: Dockerfile
    environment:
      - NODE_ID=3
      - NODE_ROLE=proposer
      - QUORUM_SIZE=3
    volumes:
      - proposer_3_data:/app/data
    ports:
      - "8083:8080"
    networks:
      - paxos_network
      
  proposer-4:
    build:
      context: ./proposer
      dockerfile: Dockerfile
    environment:
      - NODE_ID=4
      - NODE_ROLE=proposer
      - QUORUM_SIZE=3
    volumes:
      - proposer_4_data:/app/data
    ports:
      - "8084:8080"
    networks:
      - paxos_network
      
  proposer-5:
    build:
      context: ./proposer
      dockerfile: Dockerfile
    environment:
      - NODE_ID=5
      - NODE_ROLE=proposer
      - QUORUM_SIZE=3
    volumes:
      - proposer_5_data:/app/data
    ports:
      - "8085:8080"
    networks:
      - paxos_network
  
  # Acceptors
  acceptor-1:
    build:
      context: ./acceptor
      dockerfile: Dockerfile
    environment:
      - NODE_ID=1
      - NODE_ROLE=acceptor
    volumes:
      - acceptor_1_data:/app/data
    networks:
      - paxos_network
  
  acceptor-2:
    build:
      context: ./acceptor
      dockerfile: Dockerfile
    environment:
      - NODE_ID=2
      - NODE_ROLE=acceptor
    volumes:
      - acceptor_2_data:/app/data
    networks:
      - paxos_network
  
  acceptor-3:
    build:
      context: ./acceptor
      dockerfile: Dockerfile
    environment:
      - NODE_ID=3
      - NODE_ROLE=acceptor
    volumes:
      - acceptor_3_data:/app/data
    networks:
      - paxos_network
      
  acceptor-4:
    build:
      context: ./acceptor
      dockerfile: Dockerfile
    environment:
      - NODE_ID=4
      - NODE_ROLE=acceptor
    volumes:
      - acceptor_4_data:/app/data
    networks:
      - paxos_network
      
  acceptor-5:
    build:
      context: ./acceptor
      dockerfile: Dockerfile
    environment:
      - NODE_ID=5
      - NODE_ROLE=acceptor
    volumes:
      - acceptor_5_data:/app/data
    networks:
      - paxos_network
  
  # Learners
  learner-1:
    build:
      context: ./learner
      dockerfile: Dockerfile
    environment:
      - NODE_ID=1
      - NODE_ROLE=learner
    volumes:
      - learner_1_data:/app/data
      - shared_fs:/app/shared
    ports:
      - "8091:8080"
    networks:
      - paxos_network
  
  learner-2:
    build:
      context: ./learner
      dockerfile: Dockerfile
    environment:
      - NODE_ID=2
      - NODE_ROLE=learner
    volumes:
      - learner_2_data:/app/data
      - shared_fs:/app/shared
    ports:
      - "8092:8080"
    networks:
      - paxos_network
      
  learner-3:
    build:
      context: ./learner
      dockerfile: Dockerfile
    environment:
      - NODE_ID=3
      - NODE_ROLE=learner
    volumes:
      - learner_3_data:/app/data
      - shared_fs:/app/shared
    ports:
      - "8093:8080"
    networks:
      - paxos_network
  
  # Web UI (para os 3 clientes iniciais)
  web-ui:
    build:
      context: ./web-ui
      dockerfile: Dockerfile
    ports:
      - "80:80"
    networks:
      - paxos_network
    depends_on:
      - proposer-1
      - proposer-2
      - proposer-3
      - proposer-4
      - proposer-5
      - learner-1
      - learner-2
      - learner-3
  
  # Monitoramento Básico
  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus_data:/prometheus
    ports:
      - "9090:9090"
    networks:
      - paxos_network
  
  grafana:
    image: grafana/grafana:latest
    volumes:
      - ./grafana/provisioning:/etc/grafana/provisioning
      - grafana_data:/var/lib/grafana
    ports:
      - "3000:3000"
    networks:
      - paxos_network
    depends_on:
      - prometheus

volumes:
  proposer_1_data:
  proposer_2_data:
  proposer_3_data:
  proposer_4_data:
  proposer_5_data:
  acceptor_1_data:
  acceptor_2_data:
  acceptor_3_data:
  acceptor_4_data:
  acceptor_5_data:
  learner_1_data:
  learner_2_data:
  learner_3_data:
  shared_fs:
  prometheus_data:
  grafana_data:

networks:
  paxos_network:
    driver: bridge
EOL
echo "Criado: docker-compose.yml"

# === PROPOSERS ===
mkdir -p proposer/config proposer/proposer
cat > proposer/Dockerfile << 'EOL'
FROM python:3.10-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Diretório para dados persistentes
RUN mkdir -p /app/data
VOLUME /app/data

EXPOSE 8080

CMD ["uvicorn", "proposer.main:app", "--host", "0.0.0.0", "--port", "8080"]
EOL
echo "Criado: proposer/Dockerfile"

cat > proposer/requirements.txt << 'EOL'
fastapi==0.95.0
uvicorn==0.21.1
pydantic==1.10.7
aiohttp==3.8.4
structlog==23.1.0
prometheus-client==0.16.0
pyyaml==6.0
tinydb==4.7.1
aiofiles==23.1.0
EOL
echo "Criado: proposer/requirements.txt"

cat > proposer/config/config.yaml << 'EOL'
node:
  id: ${NODE_ID}
  role: proposer
  
paxos:
  quorumSize: ${QUORUM_SIZE}
  batchSize: 10
  batchDelayMs: 50
  proposalTimeout: 2000
  
networking:
  port: 8080
  acceptors:
    - acceptor-1:8080
    - acceptor-2:8080
    - acceptor-3:8080
    - acceptor-4:8080
    - acceptor-5:8080
  learners:
    - learner-1:8080
    - learner-2:8080
    - learner-3:8080
EOL
echo "Criado: proposer/config/config.yaml"

touch proposer/proposer/main.py
touch proposer/proposer/__init__.py
echo "Criada estrutura para proposer"

# === ACCEPTORS ===
mkdir -p acceptor/config acceptor/acceptor
cat > acceptor/Dockerfile << 'EOL'
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
EOL
echo "Criado: acceptor/Dockerfile"

cat > acceptor/requirements.txt << 'EOL'
fastapi==0.95.0
uvicorn==0.21.1
pydantic==1.10.7
aiohttp==3.8.4
structlog==23.1.0
prometheus-client==0.16.0
pyyaml==6.0
tinydb==4.7.1
aiofiles==23.1.0
EOL
echo "Criado: acceptor/requirements.txt"

cat > acceptor/config/config.yaml << 'EOL'
node:
  id: ${NODE_ID}
  role: acceptor
  
storage:
  path: /app/data/acceptor.json
  syncOnAccept: true
  
networking:
  port: 8080
  learners:
    - learner-1:8080
    - learner-2:8080
    - learner-3:8080
EOL
echo "Criado: acceptor/config/config.yaml"

touch acceptor/acceptor/main.py
touch acceptor/acceptor/__init__.py
echo "Criada estrutura para acceptor"

# === LEARNERS ===
mkdir -p learner/config learner/learner
cat > learner/Dockerfile << 'EOL'
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
EOL
echo "Criado: learner/Dockerfile"

cat > learner/requirements.txt << 'EOL'
fastapi==0.95.0
uvicorn==0.21.1
pydantic==1.10.7
aiohttp==3.8.4
structlog==23.1.0
prometheus-client==0.16.0
pyyaml==6.0
tinydb==4.7.1
aiofiles==23.1.0
websockets==11.0.2
EOL
echo "Criado: learner/requirements.txt"

cat > learner/config/config.yaml << 'EOL'
node:
  id: ${NODE_ID}
  role: learner
  
filesystem:
  sharedPath: /app/shared
  metadataPath: /app/data/metadata.json
  
networking:
  port: 8080
  acceptors:
    - acceptor-1:8080
    - acceptor-2:8080
    - acceptor-3:8080
    - acceptor-4:8080
    - acceptor-5:8080
  learners:
    - learner-1:8080
    - learner-2:8080
    - learner-3:8080
EOL
echo "Criado: learner/config/config.yaml"

touch learner/learner/main.py
touch learner/learner/__init__.py
echo "Criada estrutura para learner"

# === WEB UI (Frontend + Backend) ===
mkdir -p web-ui/backend web-ui/frontend/src/components web-ui/frontend/public
cat > web-ui/Dockerfile << 'EOL'
# Frontend Build Stage
FROM node:16-alpine as frontend-builder

WORKDIR /app
COPY frontend/package*.json ./
RUN npm install
COPY frontend/ .
RUN npm run build

# Backend Stage
FROM python:3.10-slim

WORKDIR /app
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY backend/ .
COPY --from=frontend-builder /app/build /app/static

EXPOSE 80

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]
EOL
echo "Criado: web-ui/Dockerfile"

cat > web-ui/backend/requirements.txt << 'EOL'
fastapi==0.95.0
uvicorn==0.21.1
aiohttp==3.8.4
python-multipart==0.0.6
jinja2==3.1.2
EOL
echo "Criado: web-ui/backend/requirements.txt"

touch web-ui/backend/main.py
touch web-ui/frontend/package.json
touch web-ui/frontend/src/App.js
touch web-ui/frontend/src/App.css
touch web-ui/frontend/src/index.js
touch web-ui/frontend/public/index.html
touch web-ui/frontend/src/components/FileExplorer.js
touch web-ui/frontend/src/components/FileEditor.js
touch web-ui/frontend/src/components/SystemStatus.js
touch web-ui/frontend/src/components/Navbar.js
echo "Criada estrutura para web-ui"

# === MONITORAMENTO ===
mkdir -p prometheus grafana/provisioning/dashboards grafana/provisioning/datasources
cat > prometheus/prometheus.yml << 'EOL'
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']

  - job_name: 'proposers'
    static_configs:
      - targets: ['proposer-1:8080', 'proposer-2:8080', 'proposer-3:8080', 'proposer-4:8080', 'proposer-5:8080']
        labels:
          group: 'proposers'

  - job_name: 'acceptors'
    static_configs:
      - targets: ['acceptor-1:8080', 'acceptor-2:8080', 'acceptor-3:8080', 'acceptor-4:8080', 'acceptor-5:8080']
        labels:
          group: 'acceptors'

  - job_name: 'learners'
    static_configs:
      - targets: ['learner-1:8080', 'learner-2:8080', 'learner-3:8080']
        labels:
          group: 'learners'
EOL
echo "Criado: prometheus/prometheus.yml"

touch grafana/provisioning/dashboards/dashboard.yml
touch grafana/provisioning/dashboards/paxos_dashboard.json
touch grafana/provisioning/datasources/prometheus.yml
echo "Criada estrutura para monitoramento"

echo "Estrutura de diretórios e arquivos criada com sucesso!"
echo "Para iniciar o projeto:"
echo "1. Preencha os arquivos de código Python"
echo "2. Execute: docker-compose build"
echo "3. Execute: docker-compose up -d"