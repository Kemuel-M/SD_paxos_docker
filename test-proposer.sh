#!/bin/bash

# Define diretórios e arquivos
PROPOSER_DIR="./proposer"
UNIT_TESTS_DIR="$PROPOSER_DIR/tests/unit"
INTEGRATION_TESTS_DIR="$PROPOSER_DIR/tests/integration"

# Cores para output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}====== Iniciando Testes do Proposer ======${NC}"

# Verifica se Python e pytest estão instalados
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Erro: Python 3 não está instalado.${NC}"
    exit 1
fi

if ! command -v pytest &> /dev/null; then
    echo -e "${YELLOW}Instalando pytest e dependências...${NC}"
    pip install pytest pytest-asyncio httpx fastapi
fi

# Cria diretório para logs temporários, se não existir
mkdir -p /tmp/paxos-test-logs

# Configuração do ambiente de teste
echo -e "${YELLOW}Configurando ambiente de teste...${NC}"
export DEBUG=true
export NODE_ID=1
export LOG_DIR="/tmp/paxos-test-logs"
export ACCEPTORS="localhost:8081,localhost:8082,localhost:8083,localhost:8084,localhost:8085"
export PROPOSERS="localhost:8071,localhost:8072,localhost:8073,localhost:8074,localhost:8075"
export LEARNERS="localhost:8091,localhost:8092"
export STORES="localhost:8061,localhost:8062,localhost:8063"

# Garante que módulos comuns estejam no PYTHONPATH
export PYTHONPATH=$PYTHONPATH:$(pwd):$(pwd)/proposer:$(pwd)/common

# Executa testes unitários
echo -e "${YELLOW}Executando testes unitários...${NC}"
cd proposer
if PYTHONPATH=. pytest -xvs tests/unit; then
    echo -e "${GREEN}Testes unitários completados com sucesso!${NC}"
else
    echo -e "${RED}Alguns testes unitários falharam.${NC}"
    TEST_FAILED=1
fi

# Executa testes de integração
echo -e "${YELLOW}Executando testes de integração...${NC}"
if PYTHONPATH=. pytest -xvs tests/integration; then
    echo -e "${GREEN}Testes de integração completados com sucesso!${NC}"
else
    echo -e "${RED}Alguns testes de integração falharam.${NC}"
    TEST_FAILED=1
fi

# Retorna ao diretório original
cd ..

# Limpa arquivos temporários
echo -e "${YELLOW}Limpando arquivos temporários...${NC}"
rm -rf /tmp/paxos-test-logs

# Verifica resultado final
if [ "$TEST_FAILED" = "1" ]; then
    echo -e "${RED}====== Alguns testes do Proposer falharam! ======${NC}"
    exit 1
else
    echo -e "${GREEN}====== Todos os testes do Proposer passaram! ======${NC}"
    exit 0
fi