#!/bin/bash
# File: scripts/run-tests.sh

# Script para executar todos os testes do Proposer com relatório de cobertura

# Define cores para output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Diretórios
PROPOSER_DIR="./proposer"
UNIT_TESTS_DIR="$PROPOSER_DIR/tests/unit"
INTEGRATION_TESTS_DIR="$PROPOSER_DIR/tests/integration"
COVERAGE_DIR="./coverage"

# Função para exibir ajuda
show_help() {
    echo -e "${BLUE}Uso: $0 [opções]${NC}"
    echo ""
    echo -e "Script para executar testes do Proposer com relatório de cobertura."
    echo ""
    echo -e "Opções:"
    echo -e "  ${GREEN}-h, --help${NC}       Exibe esta ajuda"
    echo -e "  ${GREEN}-u, --unit${NC}       Executa apenas testes unitários"
    echo -e "  ${GREEN}-i, --integration${NC} Executa apenas testes de integração"
    echo -e "  ${GREEN}-a, --all${NC}        Executa todos os testes (padrão)"
    echo -e "  ${GREEN}-c, --coverage${NC}   Gera relatório de cobertura"
    echo -e "  ${GREEN}-v, --verbose${NC}    Modo detalhado"
    echo -e "  ${GREEN}-d, --debug${NC}      Nível de debug para os testes (basic, advanced, trace)"
    echo ""
    echo -e "Exemplos:"
    echo -e "  $0 -a -c -v             Executa todos os testes com cobertura e modo detalhado"
    echo -e "  $0 -u -d advanced       Executa testes unitários com debug avançado"
    exit 0
}

# Valores padrão
RUN_UNIT=true
RUN_INTEGRATION=true
GENERATE_COVERAGE=false
VERBOSE=false
DEBUG_LEVEL="basic"

# Processa argumentos
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            ;;
        -u|--unit)
            RUN_UNIT=true
            RUN_INTEGRATION=false
            shift
            ;;
        -i|--integration)
            RUN_UNIT=false
            RUN_INTEGRATION=true
            shift
            ;;
        -a|--all)
            RUN_UNIT=true
            RUN_INTEGRATION=true
            shift
            ;;
        -c|--coverage)
            GENERATE_COVERAGE=true
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -d|--debug)
            DEBUG_LEVEL="$2"
            shift 2
            ;;
        *)
            echo -e "${RED}Opção desconhecida: $1${NC}"
            show_help
            ;;
    esac
done

# Verifica se Python e dependências estão instalados
check_dependencies() {
    echo -e "${YELLOW}Verificando dependências...${NC}"
    
    # Verifica Python
    if ! command -v python3 &> /dev/null; then
        echo -e "${RED}Erro: Python 3 não está instalado.${NC}"
        exit 1
    fi
    
    # Verifica pytest
    if ! python3 -c "import pytest" &> /dev/null; then
        echo -e "${YELLOW}Instalando pytest e dependências...${NC}"
        pip install pytest pytest-asyncio httpx fastapi
    fi
    
    # Verifica pytest-cov se cobertura for solicitada
    if [ "$GENERATE_COVERAGE" = true ] && ! python3 -c "import pytest_cov" &> /dev/null; then
        echo -e "${YELLOW}Instalando pytest-cov para relatório de cobertura...${NC}"
        pip install pytest-cov
    fi
    
    echo -e "${GREEN}Todas dependências verificadas.${NC}"
}

# Configura ambiente de teste
setup_environment() {
    echo -e "${YELLOW}Configurando ambiente de teste...${NC}"
    
    # Cria diretório para logs temporários
    mkdir -p /tmp/paxos-test-logs
    
    # Configura variáveis de ambiente para teste
    export DEBUG=true
    export DEBUG_LEVEL="$DEBUG_LEVEL"
    export NODE_ID=1
    export LOG_DIR="/tmp/paxos-test-logs"
    export ACCEPTORS="localhost:8081,localhost:8082,localhost:8083,localhost:8084,localhost:8085"
    export PROPOSERS="localhost:8071,localhost:8072,localhost:8073,localhost:8074,localhost:8075"
    export LEARNERS="localhost:8091,localhost:8092"
    export STORES="localhost:8061,localhost:8062,localhost:8063"
    
    # Garante que PYTHONPATH inclua módulos necessários
    export PYTHONPATH=$PYTHONPATH:$(pwd):$(pwd)/proposer:$(pwd)/common
    
    echo -e "${GREEN}Ambiente configurado com DEBUG_LEVEL=$DEBUG_LEVEL${NC}"
}

# Executa testes unitários
run_unit_tests() {
    echo -e "${YELLOW}====== Executando testes unitários ======${NC}"
    
    cd $PROPOSER_DIR
    
    if [ "$GENERATE_COVERAGE" = true ]; then
        # Executa com cobertura
        if [ "$VERBOSE" = true ]; then
            PYTHONPATH=. pytest -xvs tests/unit --cov=. --cov-report=term --cov-report=html:../$COVERAGE_DIR/unit
        else
            PYTHONPATH=. pytest -xvs tests/unit --cov=. --cov-report=html:../$COVERAGE_DIR/unit
        fi
        UNIT_RESULT=$?
    else
        # Executa sem cobertura
        if [ "$VERBOSE" = true ]; then
            PYTHONPATH=. pytest -xvs tests/unit
        else
            PYTHONPATH=. pytest -xs tests/unit
        fi
        UNIT_RESULT=$?
    fi
    
    cd ..
    
    if [ $UNIT_RESULT -eq 0 ]; then
        echo -e "${GREEN}====== Testes unitários concluídos com sucesso! ======${NC}"
    else
        echo -e "${RED}====== Alguns testes unitários falharam! ======${NC}"
        TESTS_FAILED=true
    fi
    
    return $UNIT_RESULT
}

# Executa testes de integração
run_integration_tests() {
    echo -e "${YELLOW}====== Executando testes de integração ======${NC}"
    
    cd $PROPOSER_DIR
    
    if [ "$GENERATE_COVERAGE" = true ]; then
        # Executa com cobertura
        if [ "$VERBOSE" = true ]; then
            PYTHONPATH=. pytest -xvs tests/integration --cov=. --cov-report=term --cov-report=html:../$COVERAGE_DIR/integration
        else
            PYTHONPATH=. pytest -xvs tests/integration --cov=. --cov-report=html:../$COVERAGE_DIR/integration
        fi
        INTEGRATION_RESULT=$?
    else
        # Executa sem cobertura
        if [ "$VERBOSE" = true ]; then
            PYTHONPATH=. pytest -xvs tests/integration
        else
            PYTHONPATH=. pytest -xs tests/integration
        fi
        INTEGRATION_RESULT=$?
    fi
    
    cd ..
    
    if [ $INTEGRATION_RESULT -eq 0 ]; then
        echo -e "${GREEN}====== Testes de integração concluídos com sucesso! ======${NC}"
    else
        echo -e "${RED}====== Alguns testes de integração falharam! ======${NC}"
        TESTS_FAILED=true
    fi
    
    return $INTEGRATION_RESULT
}

# Gera relatório de cobertura combinado
generate_combined_coverage() {
    if [ "$GENERATE_COVERAGE" = true ]; then
        echo -e "${YELLOW}====== Gerando relatório de cobertura combinado ======${NC}"
        
        # Cria diretório para relatório
        mkdir -p $COVERAGE_DIR/combined
        
        cd $PROPOSER_DIR
        
        # Gera relatório combinado
        PYTHONPATH=. pytest -xvs --cov=. --cov-report=html:../$COVERAGE_DIR/combined
        
        cd ..
        
        echo -e "${GREEN}Relatório de cobertura gerado em ${COVERAGE_DIR}/combined${NC}"
        echo -e "${BLUE}Para visualizar: abra ${COVERAGE_DIR}/combined/index.html em um navegador${NC}"
    fi
}

# Limpa arquivos temporários
cleanup() {
    echo -e "${YELLOW}Limpando arquivos temporários...${NC}"
    rm -rf /tmp/paxos-test-logs
    echo -e "${GREEN}Limpeza concluída.${NC}"
}

# ===== EXECUÇÃO PRINCIPAL =====

echo -e "${BLUE}====== Iniciando Testes do Proposer ======${NC}"

# Verifica dependências
check_dependencies

# Configura ambiente
setup_environment

# Marca para rastrear falhas
TESTS_FAILED=false

# Executa testes unitários se solicitado
if [ "$RUN_UNIT" = true ]; then
    run_unit_tests
    UNIT_RESULT=$?
else
    UNIT_RESULT=0
fi

# Executa testes de integração se solicitado
if [ "$RUN_INTEGRATION" = true ]; then
    run_integration_tests
    INTEGRATION_RESULT=$?
else
    INTEGRATION_RESULT=0
fi

# Gera relatório combinado de cobertura
if [ "$GENERATE_COVERAGE" = true ] && [ "$RUN_UNIT" = true ] && [ "$RUN_INTEGRATION" = true ]; then
    generate_combined_coverage
fi

# Limpa arquivos temporários
cleanup

# Verifica resultado final
if [ "$TESTS_FAILED" = true ]; then
    echo -e "${RED}====== Alguns testes do Proposer falharam! ======${NC}"
    exit 1
else
    echo -e "${GREEN}====== Todos os testes do Proposer passaram! ======${NC}"
    exit 0
fi