#!/bin/bash
# File: scripts/run-tests-client.sh

# Script para executar todos os testes do Cliente com relatório de cobertura

# Define cores para output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Diretórios
CLIENT_DIR="./client"
UNIT_TESTS_DIR="$CLIENT_DIR/tests/unit"
INTEGRATION_TESTS_DIR="$CLIENT_DIR/tests/integration"
COVERAGE_DIR="./coverage"
TMP_TEST_DIR="./tmp-test-data"

# Função para exibir ajuda
show_help() {
    echo -e "${BLUE}Uso: $0 [opções]${NC}"
    echo ""
    echo -e "Script para executar testes do Cliente com relatório de cobertura."
    echo ""
    echo -e "Opções:"
    echo -e "  ${GREEN}-h, --help${NC}       Exibe esta ajuda"
    echo -e "  ${GREEN}-u, --unit${NC}       Executa apenas testes unitários"
    echo -e "  ${GREEN}-i, --integration${NC} Executa apenas testes de integração"
    echo -e "  ${GREEN}-a, --all${NC}        Executa todos os testes (padrão)"
    echo -e "  ${GREEN}-c, --coverage${NC}   Gera relatório de cobertura"
    echo -e "  ${GREEN}-v, --verbose${NC}    Modo detalhado"
    echo -e "  ${GREEN}-d, --debug${NC}      Nível de debug para os testes (basic, advanced, trace) [padrão: trace]"
    echo -e "  ${GREEN}--no-verbose-logs${NC} Desativa logs verbosos externos (httpx, asyncio, etc.)"
    echo ""
    echo -e "Exemplos:"
    echo -e "  $0 -a -c -v             Executa todos os testes com cobertura e modo detalhado"
    echo -e "  $0 -u -d advanced       Executa testes unitários com debug avançado"
    echo -e "  $0 -i                   Executa testes de integração com verbose-logs e debug trace (padrão)"
    exit 0
}

# Valores padrão
RUN_UNIT=true
RUN_INTEGRATION=true
GENERATE_COVERAGE=false
VERBOSE=true
DEBUG_LEVEL="trace"
VERBOSE_LOGS=true

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
        --no-verbose-logs)
            VERBOSE_LOGS=false
            shift
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
    
    # Verifica pytest e pytest-asyncio
    if ! python3 -c "import pytest" &> /dev/null; then
        echo -e "${YELLOW}Instalando pytest e dependências...${NC}"
        pip install pytest pytest-asyncio httpx fastapi
    elif ! python3 -c "import pytest_asyncio" &> /dev/null; then
        echo -e "${YELLOW}Instalando pytest-asyncio...${NC}"
        pip install pytest-asyncio
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
    
    # Cria diretórios para testes
    mkdir -p ${TMP_TEST_DIR}/logs
    mkdir -p ${TMP_TEST_DIR}/data
    
    # Configura variáveis de ambiente para teste
    export DEBUG=true
    export DEBUG_LEVEL="$DEBUG_LEVEL"
    export NODE_ID=1
    export CLIENT_ID="client-1"
    export LOG_DIR="${TMP_TEST_DIR}/logs"
    export DATA_DIR="${TMP_TEST_DIR}/data"
    export PROPOSER="localhost:8080"
    export STORES="localhost:8081,localhost:8082,localhost:8083"
    
    # Garante que PYTHONPATH inclua módulos necessários
    export PYTHONPATH=$PYTHONPATH:$(pwd):$(pwd)/client:$(pwd)/common
    
    echo -e "${GREEN}Ambiente configurado com DEBUG_LEVEL=$DEBUG_LEVEL${NC}"
    echo -e "${GREEN}Dados temporários em: $TMP_TEST_DIR${NC}"
}

# Executa testes unitários
run_unit_tests() {
    echo -e "${YELLOW}====== Executando testes unitários ======${NC}"
    
    cd $CLIENT_DIR
    
    # Opções de debug para testes unitários 
    # (só usamos --debug-level, verbose-logs é mais relevante para integração)
    DEBUG_ARGS="--debug-level=$DEBUG_LEVEL"
    
    if [ "$GENERATE_COVERAGE" = true ]; then
        # Executa com cobertura
        if [ "$VERBOSE" = true ]; then
            echo -e "${BLUE}Executando testes unitários com cobertura e modo verbose${NC}"
            PYTHONPATH=.:.. pytest -xvs tests/unit $DEBUG_ARGS --cov=. --cov-report=term --cov-report=html:../$COVERAGE_DIR/unit
        else
            echo -e "${BLUE}Executando testes unitários com cobertura${NC}"
            PYTHONPATH=.:.. pytest -xvs tests/unit $DEBUG_ARGS --cov=. --cov-report=html:../$COVERAGE_DIR/unit
        fi
        UNIT_RESULT=$?
    else
        # Executa sem cobertura
        if [ "$VERBOSE" = true ]; then
            echo -e "${BLUE}Executando testes unitários em modo verbose${NC}"
            PYTHONPATH=.:.. pytest -xvs tests/unit $DEBUG_ARGS
        else
            echo -e "${BLUE}Executando testes unitários${NC}"
            PYTHONPATH=.:.. pytest -xs tests/unit $DEBUG_ARGS
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
    
    cd $CLIENT_DIR
    
    # Configura opções para testes de integração
    # Sempre incluímos --debug-level
    DEBUG_ARGS="--debug-level=$DEBUG_LEVEL"
    
    # Adiciona --verbose-logs conforme configuração
    if [ "$VERBOSE_LOGS" = true ]; then
        DEBUG_ARGS="$DEBUG_ARGS --verbose-logs"
        echo -e "${BLUE}Logs verbosos ativados para módulos externos${NC}"
    fi
    
    if [ "$GENERATE_COVERAGE" = true ]; then
        # Executa com cobertura
        if [ "$VERBOSE" = true ]; then
            echo -e "${BLUE}Executando testes de integração com cobertura e modo verbose${NC}"
            echo -e "${BLUE}Usando: $DEBUG_ARGS${NC}"
            PYTHONPATH=.:.. pytest -xvs tests/integration $DEBUG_ARGS --cov=. --cov-report=term --cov-report=html:../$COVERAGE_DIR/integration
        else
            echo -e "${BLUE}Executando testes de integração com cobertura${NC}"
            echo -e "${BLUE}Usando: $DEBUG_ARGS${NC}"
            PYTHONPATH=.:.. pytest -xvs tests/integration $DEBUG_ARGS --cov=. --cov-report=html:../$COVERAGE_DIR/integration
        fi
        INTEGRATION_RESULT=$?
    else
        # Executa sem cobertura
        if [ "$VERBOSE" = true ]; then
            echo -e "${BLUE}Executando testes de integração em modo verbose${NC}"
            echo -e "${BLUE}Usando: $DEBUG_ARGS${NC}"
            PYTHONPATH=.:.. pytest -xvs tests/integration $DEBUG_ARGS
        else
            echo -e "${BLUE}Executando testes de integração${NC}"
            echo -e "${BLUE}Usando: $DEBUG_ARGS${NC}"
            PYTHONPATH=.:.. pytest -xs tests/integration $DEBUG_ARGS
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
        
        cd $CLIENT_DIR
        
        # Gera relatório combinado
        PYTHONPATH=.:.. pytest -xvs --cov=. --cov-report=html:../$COVERAGE_DIR/combined
        
        cd ..
        
        echo -e "${GREEN}Relatório de cobertura gerado em ${COVERAGE_DIR}/combined${NC}"
        echo -e "${BLUE}Para visualizar: abra ${COVERAGE_DIR}/combined/index.html em um navegador${NC}"
    fi
}

# Limpa arquivos temporários
cleanup() {
    echo -e "${YELLOW}Limpando arquivos temporários...${NC}"
    rm -rf "${TMP_TEST_DIR}"
    echo -e "${GREEN}Limpeza concluída.${NC}"
}

# ===== EXECUÇÃO PRINCIPAL =====

echo -e "${BLUE}====== Iniciando Testes do Cliente ======${NC}"
echo -e "${BLUE}Configurações: DEBUG_LEVEL=${DEBUG_LEVEL}, VERBOSE_LOGS=${VERBOSE_LOGS}${NC}"

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
    echo -e "${RED}====== Alguns testes do Cliente falharam! ======${NC}"
    exit 1
else
    echo -e "${GREEN}====== Todos os testes do Cliente passaram! ======${NC}"
    exit 0
fi