#!/bin/bash
#
# Script para acessar e editar um recurso no sistema Paxos via Cliente 1
# Autor: Claude
# Data: 2025-04-08

# Cores para saída
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Configurações
CLIENT_API_PORT=8401
CLIENT_WEB_PORT=8501
CLIENT_HOST="localhost"
RESOURCE_ID="R"

# Arquivo de log
LOG_FILE="paxos-client-access-$(date +%Y%m%d-%H%M%S).log"

# Função para log
log() {
    local level=$1
    local message=$2
    local timestamp=$(date "+%Y-%m-%d %H:%M:%S")
    
    # Log para o terminal
    case "$level" in
        "INFO")
            echo -e "${BLUE}[INFO]${NC} $message"
            ;;
        "SUCCESS")
            echo -e "${GREEN}[SUCCESS]${NC} $message"
            ;;
        "WARNING")
            echo -e "${YELLOW}[WARNING]${NC} $message"
            ;;
        "ERROR")
            echo -e "${RED}[ERROR]${NC} $message"
            ;;
        "REQUEST")
            echo -e "${CYAN}[REQUEST]${NC} $message"
            ;;
        "RESPONSE")
            echo -e "${CYAN}[RESPONSE]${NC} $message"
            ;;
        *)
            echo -e "$message"
            ;;
    esac
    
    # Log para o arquivo
    echo "[$timestamp] [$level] $message" >> "$LOG_FILE"
}

# Função para verificar requisitos
check_requirements() {
    log "INFO" "Verificando requisitos..."
    
    # Verifica se curl está instalado
    if ! command -v curl &> /dev/null; then
        log "ERROR" "curl não está instalado. Por favor, instale-o e tente novamente."
        exit 1
    fi
    
    # Verifica se jq está instalado
    if ! command -v jq &> /dev/null; then
        log "ERROR" "jq não está instalado. Por favor, instale-o e tente novamente."
        exit 1
    fi
    
    log "SUCCESS" "Todos os requisitos estão atendidos."
}

# Função para verificar se o cliente está acessível
check_client() {
    log "INFO" "Verificando se o Cliente 1 está acessível..."
    
    # Tenta acessar a API de health do cliente
    local response
    response=$(curl -s -o /dev/null -w "%{http_code}" "http://${CLIENT_HOST}:${CLIENT_API_PORT}/health")
    
    if [ "$response" == "200" ]; then
        log "SUCCESS" "Cliente 1 está acessível."
        return 0
    else
        log "ERROR" "Cliente 1 não está acessível. Código de resposta: $response"
        log "INFO" "Certifique-se de que o sistema Paxos está em execução e tente novamente."
        exit 1
    fi
}

# Função para obter o status do cliente
get_client_status() {
    log "INFO" "Obtendo status do Cliente 1..."
    log "REQUEST" "GET http://${CLIENT_HOST}:${CLIENT_API_PORT}/status"
    
    local response
    response=$(curl -s "http://${CLIENT_HOST}:${CLIENT_API_PORT}/status")
    
    log "RESPONSE" "$response"
    
    # Extrai informações usando jq
    local client_id=$(echo "$response" | jq -r '.client_id')
    local proposer_url=$(echo "$response" | jq -r '.proposer_url')
    local running=$(echo "$response" | jq -r '.running')
    
    log "INFO" "Cliente ID: $client_id"
    log "INFO" "Proposer URL: $proposer_url"
    log "INFO" "Em execução: $running"
    
    # Verifica se o cliente está em execução
    if [ "$running" != "true" ]; then
        log "WARNING" "Cliente não está em execução. Tentando iniciar..."
        start_client
    fi
}

# Função para iniciar o cliente
start_client() {
    log "INFO" "Iniciando o Cliente 1..."
    log "REQUEST" "POST http://${CLIENT_HOST}:${CLIENT_API_PORT}/api/start"
    
    local response
    response=$(curl -s -X POST "http://${CLIENT_HOST}:${CLIENT_API_PORT}/api/start")
    
    log "RESPONSE" "$response"
    
    # Verifica resposta
    local status=$(echo "$response" | jq -r '.status')
    
    if [ "$status" == "started" ]; then
        log "SUCCESS" "Cliente iniciado com sucesso."
    else
        log "ERROR" "Falha ao iniciar o cliente."
        exit 1
    fi
    
    # Pequena pausa para garantir que o cliente esteja pronto
    sleep 1
}

# Função para obter informações do recurso
get_resource_info() {
    log "INFO" "Obtendo informações do recurso '$RESOURCE_ID'..."
    log "REQUEST" "GET http://${CLIENT_HOST}:${CLIENT_WEB_PORT}/resource/$RESOURCE_ID"
    
    # Usamos curl para obter a página HTML do recurso
    local resource_page
    resource_page=$(curl -s "http://${CLIENT_HOST}:${CLIENT_WEB_PORT}/resource/$RESOURCE_ID")
    
    # Verificamos se a página foi carregada
    if [ -z "$resource_page" ]; then
        log "ERROR" "Falha ao obter informações do recurso."
        exit 1
    fi
    
    log "SUCCESS" "Página do recurso carregada com sucesso."
    
    # Como não podemos analisar facilmente o HTML com ferramentas de shell básicas,
    # vamos fazer uma requisição para obter os dados do recurso diretamente do Cluster Store
    log "INFO" "Obtendo dados do recurso via API..."
    
    # A API do cliente não tem um endpoint direto para obter o recurso,
    # então tentamos acessar o Cluster Store diretamente
    local cluster_store_port=8301  # Cluster Store 1
    log "REQUEST" "GET http://${CLIENT_HOST}:${cluster_store_port}/resource/$RESOURCE_ID"
    
    local resource_data
    resource_data=$(curl -s "http://${CLIENT_HOST}:${cluster_store_port}/resource/$RESOURCE_ID")
    
    # Verificamos se obtivemos dados
    if [ -z "$resource_data" ]; then
        log "WARNING" "Não foi possível obter dados diretamente do Cluster Store."
        return 1
    fi
    
    log "RESPONSE" "$resource_data"
    
    # Extrair dados do recurso
    local data=$(echo "$resource_data" | jq -r '.data')
    local version=$(echo "$resource_data" | jq -r '.version')
    local node_id=$(echo "$resource_data" | jq -r '.node_id')
    
    log "INFO" "Conteúdo atual do recurso: $data"
    log "INFO" "Versão: $version"
    log "INFO" "Último nó modificador: $node_id"
    
    echo "$data" > resource_current_content.txt
    log "INFO" "Conteúdo atual salvo em 'resource_current_content.txt'"
}

# Função para editar o recurso
edit_resource() {
    log "INFO" "Preparando para editar o recurso '$RESOURCE_ID'..."
    
    # Gera um conteúdo novo com timestamp
    local timestamp=$(date "+%Y-%m-%d %H:%M:%S")
    local new_content="Recurso $RESOURCE_ID atualizado via script em $timestamp"
    
    log "INFO" "Novo conteúdo: $new_content"
    
    # Envia operação para o cliente
    log "INFO" "Enviando operação de edição..."
    log "REQUEST" "POST http://${CLIENT_HOST}:${CLIENT_API_PORT}/api/operation com dados: $new_content"
    
    local response
    response=$(curl -s -X POST "http://${CLIENT_HOST}:${CLIENT_API_PORT}/api/operation" \
               -H "Content-Type: application/json" \
               -d "\"$new_content\"")
    
    log "RESPONSE" "$response"
    
    # Extrai informações da resposta
    local status=$(echo "$response" | jq -r '.status')
    local operation_id=$(echo "$response" | jq -r '.operation_id')
    
    if [ "$status" == "initiated" ]; then
        log "SUCCESS" "Operação de edição iniciada com ID: $operation_id"
    else
        log "ERROR" "Falha ao iniciar operação de edição."
        return 1
    fi
    
    # Aguarda alguns segundos para o consenso acontecer
    log "INFO" "Aguardando processamento da operação..."
    
    # Loop para verificar o status da operação
    local attempts=0
    local max_attempts=10
    local operation_completed=false
    
    while [ $attempts -lt $max_attempts ] && [ "$operation_completed" == "false" ]; do
        sleep 3
        attempts=$((attempts+1))
        
        # Obtém histórico de operações
        log "REQUEST" "GET http://${CLIENT_HOST}:${CLIENT_API_PORT}/api/history"
        local history
        history=$(curl -s "http://${CLIENT_HOST}:${CLIENT_API_PORT}/api/history?limit=5")
        
        # Extrai status da operação
        local operation
        operation=$(echo "$history" | jq -r --arg operation_id "$operation_id" '.operations[] | select(.id == ($operation_id | tonumber))')
        
        if [ -n "$operation" ]; then
            local op_status=$(echo "$operation" | jq -r '.status')
            log "INFO" "Tentativa $attempts/$max_attempts - Status da operação: $op_status"
            
            if [ "$op_status" == "COMMITTED" ]; then
                operation_completed=true
                log "SUCCESS" "Operação completada com sucesso!"
                break
            elif [ "$op_status" == "NOT_COMMITTED" ] || [ "$op_status" == "failed" ] || [ "$op_status" == "timeout" ]; then
                log "ERROR" "Operação falhou com status: $op_status"
                return 1
            fi
        else
            log "INFO" "Tentativa $attempts/$max_attempts - Operação ainda não encontrada no histórico"
        fi
    done
    
    if [ "$operation_completed" == "false" ]; then
        log "WARNING" "Tempo limite excedido aguardando conclusão da operação"
        return 1
    fi
    
    # Verifica o recurso atualizado
    sleep 2  # Espera um pouco mais para garantir que as mudanças foram propagadas
    log "INFO" "Verificando o recurso atualizado..."
    get_resource_info
}

# Principal
main() {
    echo -e "${BOLD}=== Script de Acesso ao Cliente Paxos ===${NC}"
    echo -e "Logs detalhados serão salvos em: ${YELLOW}$LOG_FILE${NC}"
    echo ""
    
    log "INFO" "Iniciando script de acesso ao Cliente Paxos..."
    
    # Verifica requisitos
    check_requirements
    
    # Verifica se o cliente está acessível
    check_client
    
    # Obtém status do cliente
    get_client_status
    
    # Obtém informações do recurso
    get_resource_info
    
    # Edita o recurso
    echo ""
    echo -e "${BOLD}=== Editando Recurso ===${NC}"
    edit_resource
    
    # Conclusão
    echo ""
    echo -e "${BOLD}=== Resumo da Operação ===${NC}"
    log "INFO" "Operação concluída. Verifique o log para detalhes: $LOG_FILE"
    
    # Exibe resumo
    echo ""
    echo -e "${BOLD}Para visualizar o cliente no navegador:${NC}"
    echo -e "${YELLOW}http://$CLIENT_HOST:$CLIENT_WEB_PORT${NC}"
    echo ""
}

# Executa a função principal
main
