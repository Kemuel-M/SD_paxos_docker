#!/bin/bash
set -e

# Script para executar os testes automatizados para o sistema Paxos

# Cores para saída
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}    TESTES AUTOMATIZADOS - SISTEMA PAXOS    ${NC}"
echo -e "${BLUE}============================================${NC}"

# Verificar se Python e pip estão instalados
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Python 3 não encontrado. Por favor, instale Python 3.${NC}"
    exit 1
fi

if ! command -v pip3 &> /dev/null; then
    echo -e "${RED}pip3 não encontrado. Por favor, instale pip3.${NC}"
    exit 1
fi

# Instalar dependências
echo -e "${YELLOW}Instalando dependências...${NC}"
# Instalar explicitamente aiofiles, que é uma dependência comum que estava faltando
pip3 install -q aiofiles

# Instalar dependências de todos os componentes
echo -e "${YELLOW}Instalando dependências de teste...${NC}"
pip3 install -q -r tests/requirements.txt

echo -e "${YELLOW}Instalando dependências do Acceptor...${NC}"
pip3 install -q -r acceptor/requirements.txt

echo -e "${YELLOW}Instalando dependências do Proposer...${NC}"
pip3 install -q -r proposer/requirements.txt

echo -e "${YELLOW}Instalando dependências do Learner...${NC}"
pip3 install -q -r learner/requirements.txt

echo -e "${YELLOW}Instalando dependências do Store...${NC}"
pip3 install -q -r store/requirements.txt

echo -e "${YELLOW}Instalando dependências da Web UI...${NC}"
pip3 install -q -r web-ui/backend/requirements.txt

# Preparar ambiente de teste
echo -e "${YELLOW}Preparando ambiente de teste...${NC}"
export PYTHONPATH=$PYTHONPATH:$(pwd)
mkdir -p test-reports

# Executar testes unitários
echo -e "${YELLOW}Executando testes unitários...${NC}"
python3 -m pytest tests/unit/ -v --cov=. --cov-report=xml:test-reports/coverage.xml

# Verificar se deve executar testes de integração
if [ "$1" == "--integration" ]; then
    echo -e "${YELLOW}Executando testes de integração...${NC}"
    
    # Verificar se docker-compose está disponível
    if ! command -v docker-compose &> /dev/null; then
        echo -e "${RED}docker-compose não encontrado. Instale-o para executar testes de integração.${NC}"
        exit 1
    fi
    
    # Iniciar containers de teste
    echo -e "${YELLOW}Iniciando containers de teste...${NC}"
    cd tests/integration
    docker-compose -f docker-compose.test.yml up -d
    
    # Aguardar containers iniciarem
    echo -e "${YELLOW}Aguardando containers iniciarem...${NC}"
    sleep 15
    
    # Executar testes de integração
    cd ../..
    python3 -m pytest tests/integration/ -v --runintegration
    
    # Parar containers de teste
    echo -e "${YELLOW}Parando containers de teste...${NC}"
    cd tests/integration
    docker-compose -f docker-compose.test.yml down
    cd ../..
else
    echo -e "${BLUE}Testes de integração ignorados. Use --integration para executá-los.${NC}"
fi

# Verificar resultado dos testes
if [ $? -eq 0 ]; then
    echo -e "${GREEN}Todos os testes passaram com sucesso!${NC}"
else
    echo -e "${RED}Falha em alguns testes. Verifique os logs acima.${NC}"
    exit 1
fi

echo -e "${BLUE}============================================${NC}"
echo -e "${GREEN}Testes concluídos!${NC}"