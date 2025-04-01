#!/bin/bash

# Script para criar a estrutura de diretórios e arquivos vazios 
# para o sistema de consenso distribuído Paxos em Docker

echo "Criando estrutura de diretórios e arquivos para o sistema Paxos..."

# Criar diretório raiz (atual)
mkdir -p paxos-docker-edu
cd paxos-docker-edu

# Criar docker-compose.yml
touch docker-compose.yml
echo "Criado: docker-compose.yml"

# Criar README.md
touch README.md
echo "Criado: README.md"

# === PROPOSERS ===
for i in {1..5}; do
  # Criar estrutura para cada proposer
  mkdir -p proposer/config proposer/proposer
  touch proposer/Dockerfile
  touch proposer/requirements.txt
  touch proposer/proposer/main.py
  touch proposer/proposer/__init__.py
  touch proposer/config/config.yaml
  echo "Criada estrutura para proposer"
done

# === ACCEPTORS ===
for i in {1..5}; do
  # Criar estrutura para cada acceptor
  mkdir -p acceptor/config acceptor/acceptor
  touch acceptor/Dockerfile
  touch acceptor/requirements.txt
  touch acceptor/acceptor/main.py
  touch acceptor/acceptor/__init__.py
  touch acceptor/config/config.yaml
  echo "Criada estrutura para acceptor"
done

# === LEARNERS ===
for i in {1..3}; do
  # Criar estrutura para cada learner
  mkdir -p learner/config learner/learner
  touch learner/Dockerfile
  touch learner/requirements.txt
  touch learner/learner/main.py
  touch learner/learner/__init__.py
  touch learner/config/config.yaml
  echo "Criada estrutura para learner"
done

# === WEB UI (Frontend + Backend) ===
# Backend
mkdir -p web-ui/backend
touch web-ui/backend/requirements.txt
touch web-ui/backend/main.py

# Frontend
mkdir -p web-ui/frontend/src/components web-ui/frontend/public
touch web-ui/frontend/package.json
touch web-ui/frontend/src/App.js
touch web-ui/frontend/src/App.css
touch web-ui/frontend/src/index.js
touch web-ui/frontend/public/index.html

# Componentes do frontend
touch web-ui/frontend/src/components/FileExplorer.js
touch web-ui/frontend/src/components/FileEditor.js
touch web-ui/frontend/src/components/SystemStatus.js
touch web-ui/frontend/src/components/Navbar.js

# Dockerfile para web-ui
touch web-ui/Dockerfile
echo "Criada estrutura para web-ui"

# === MONITORAMENTO ===
# Prometheus
mkdir -p prometheus
touch prometheus/prometheus.yml

# Grafana
mkdir -p grafana/provisioning/dashboards grafana/provisioning/datasources
touch grafana/provisioning/dashboards/dashboard.yml
touch grafana/provisioning/dashboards/paxos_dashboard.json
touch grafana/provisioning/datasources/prometheus.yml
echo "Criada estrutura para monitoramento"

# Criar diretórios para dados que serão usados como volumes
mkdir -p data/proposer_data data/acceptor_data data/learner_data data/shared_fs
echo "Criados diretórios para dados e volumes"

echo "Estrutura de diretórios e arquivos criada com sucesso!"
echo "Para iniciar o projeto:"
echo "1. Preencha os arquivos de configuração e código"
echo "2. Execute: docker-compose build"
echo "3. Execute: docker-compose up -d"
