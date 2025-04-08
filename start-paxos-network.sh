#!/bin/bash
#
# Script para inicializar a rede Paxos completa
# Autor: Claude
# Data: 2025-04-07

# Cores para output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== Inicializando Sistema de Consenso Paxos ===${NC}"

# Cria diretório para dados persistentes
echo -e "${YELLOW}Criando diretórios de dados...${NC}"
mkdir -p ./data/{proposer,acceptor,learner,cluster-store,client}-{1,2,3,4,5}

# Verifica se o Docker está rodando
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}Erro: Docker não está rodando. Por favor inicie o Docker e tente novamente.${NC}"
    exit 1
fi

# Verifica se o Docker Compose está instalado
if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}Erro: Docker Compose não encontrado. Por favor instale o Docker Compose e tente novamente.${NC}"
    exit 1
fi

# Opção para modo detached
DETACHED=""
if [ "$1" == "-d" ]; then
    DETACHED="-d"
    echo -e "${YELLOW}Iniciando em modo detached...${NC}"
fi

# Inicia a rede
echo -e "${YELLOW}Iniciando containers...${NC}"
docker-compose up $DETACHED --build

# Se estiver em modo detached, mostra instruções de acesso
if [ "$1" == "-d" ]; then
    echo -e "${GREEN}Sistema inicializado com sucesso!${NC}"
    echo -e "${BLUE}=== Acessando o Sistema ===${NC}"
    echo -e "Interfaces Web (Clientes):"
    echo -e " - Cliente 1: http://localhost:8501"
    echo -e " - Cliente 2: http://localhost:8502"
    echo -e " - Cliente 3: http://localhost:8503"
    echo -e " - Cliente 4: http://localhost:8504"
    echo -e " - Cliente 5: http://localhost:8505"
    echo -e ""
    echo -e "APIs REST:"
    echo -e " - Proposers: http://localhost:800[1-5]"
    echo -e " - Acceptors: http://localhost:810[1-5]"
    echo -e " - Learners: http://localhost:820[1-2]"
    echo -e " - Cluster Stores: http://localhost:830[1-3]"
    echo -e " - Clientes API: http://localhost:840[1-5]"
    echo -e ""
    echo -e "${YELLOW}Para visualizar os logs: ${NC}docker-compose logs -f"
    echo -e "${YELLOW}Para parar o sistema: ${NC}docker-compose down"
fi
