#!/bin/bash

# Cores para saída
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

# Função para imprimir com timestamp
log() {
  echo -e "[$(date '+%Y-%m-%d %H:%M:%S')] ${GREEN}$1${NC}"
}

# Função para imprimir erro
log_error() {
  echo -e "[$(date '+%Y-%m-%d %H:%M:%S')] ${RED}$1${NC}"
}

# Função para imprimir aviso
log_warning() {
  echo -e "[$(date '+%Y-%m-%d %H:%M:%S')] ${YELLOW}$1${NC}"
}

# Função para iniciar o sistema
start_system() {
  log "Construindo e iniciando o sistema..."
  docker-compose build
  docker-compose up -d
  
  log "Aguardando inicialização dos serviços..."
  sleep 15
  
  check_services
}

# Função para verificar o status dos serviços
check_services() {
  log "Verificando status dos serviços..."
  
  # Proposers
  for i in {1..5}; do
    if curl -s http://localhost:808$i/health > /dev/null; then
      log "Proposer $i está ativo"
    else
      log_error "Proposer $i não está respondendo"
    fi
  done
  
  # Acceptors
  for i in {1..5}; do
    if curl -s http://localhost:809$i/health > /dev/null; then
      log "Acceptor $i está ativo"
    else
      log_error "Acceptor $i não está respondendo"
    fi
  done
  
  # Learners
  for i in {1..2}; do
    if curl -s http://localhost:810$i/health > /dev/null; then
      log "Learner $i está ativo"
    else
      log_error "Learner $i não está respondendo"
    fi
  done
  
  # Stores
  for i in {1..3}; do
    if curl -s http://localhost:811$i/health > /dev/null; then
      log "Store $i está ativo"
    else
      log_error "Store $i não está respondendo"
    fi
  done
  
  # Prometheus e Grafana
  if curl -s http://localhost:9090/-/healthy > /dev/null; then
    log "Prometheus está ativo"
  else
    log_error "Prometheus não está respondendo"
  fi
  
  if curl -s http://localhost:3000/api/health > /dev/null; then
    log "Grafana está ativo"
  else
    log_error "Grafana não está respondendo"
  fi
}

# Função para parar o sistema
stop_system() {
  log "Parando o sistema..."
  docker-compose down
}

# Função para mostrar logs
show_logs() {
  component=$1
  if [ -z "$component" ]; then
    log "Mostrando logs de todos os componentes..."
    docker-compose logs --tail=100 -f
  else
    log "Mostrando logs do componente $component..."
    docker-compose logs --tail=100 -f $component
  fi
}

# Função para simular falha em um nó do Cluster Store sem pedido pendente
simulate_failure_store_without_request() {
  store_id=$1
  
  if [ -z "$store_id" ]; then
    store_id=1
  fi
  
  log_warning "Simulando falha no Store $store_id sem pedido pendente..."
  docker-compose stop store-$store_id
  
  log "Aguardando 7 segundos para detecção da falha (3 heartbeats + tempo de processamento)..."
  sleep 7
  
  log "Verificando comportamento do sistema após falha..."
  check_services
  
  log_warning "Pressione Enter para restaurar o nó falho..."
  read
  
  log "Restaurando Store $store_id..."
  docker-compose start store-$store_id
  
  log "Aguardando recuperação do nó..."
  sleep 10
  
  check_services
  
  log "Verificando sincronização..."
  curl -s http://localhost:811$store_id/status | grep -E "version|timestamp"
}

# Função para simular falha em um nó do Cluster Store durante o processamento de uma requisição
simulate_failure_store_with_request() {
  store_id=$1
  
  if [ -z "$store_id" ]; then
    store_id=2
  fi
  
  log_warning "Simulando falha no Store $store_id durante processamento de requisição..."
  log "Primeiro, aguarde até ver uma operação iniciando nos logs do learner..."
  
  log "Monitorando logs do learner-1..."
  docker-compose logs -f learner-1 &
  log_pid=$!
  
  log_warning "Quando observar uma operação iniciando, pressione Enter para parar o Store $store_id..."
  read
  
  # Matar o processo de log
  kill $log_pid
  
  log "Parando Store $store_id..."
  docker-compose stop store-$store_id
  
  log "Aguardando timeout da operação (500ms)..."
  sleep 2
  
  log "Verificando comportamento do sistema após falha..."
  check_services
  
  log_warning "Pressione Enter para restaurar o nó falho..."
  read
  
  log "Restaurando Store $store_id..."
  docker-compose start store-$store_id
  
  log "Aguardando recuperação do nó..."
  sleep 10
  
  check_services
}

# Função para simular falha em um nó do Cluster Store após "ready" mas antes de completar a escrita
simulate_failure_store_after_ready() {
  store_id=$1
  
  if [ -z "$store_id" ]; then
    store_id=3
  fi
  
  log_warning "Simulando falha no Store $store_id após 'ready' mas antes de completar a escrita..."
  log "Este cenário requer modificação temporária do código para inserir um delay após a fase prepare."
  log "Como isso exigiria alteração do código, vamos simular de forma aproximada."
  
  log "Monitorando logs do store-$store_id para identificar operações prepare..."
  docker-compose logs -f store-$store_id | grep -i "prepare" &
  log_pid=$!
  
  log_warning "Quando observar uma requisição prepare sendo recebida, pressione Enter para parar o Store $store_id..."
  read
  
  # Matar o processo de log
  kill $log_pid
  
  log "Parando Store $store_id..."
  docker-compose stop store-$store_id
  
  log "Aguardando timeout da operação..."
  sleep 2
  
  log "Verificando comportamento do sistema após falha..."
  check_services
  
  log_warning "Pressione Enter para restaurar o nó falho..."
  read
  
  log "Restaurando Store $store_id..."
  docker-compose start store-$store_id
  
  log "Aguardando recuperação do nó..."
  sleep 10
  
  check_services
}

# Função para mostrar menu de ajuda
show_help() {
  echo -e "${GREEN}Sistema de Consenso Distribuído Paxos em Docker${NC}"
  echo
  echo "Uso: $0 [comando]"
  echo
  echo "Comandos:"
  echo "  start             - Inicia o sistema"
  echo "  stop              - Para o sistema"
  echo "  status            - Verifica o status dos serviços"
  echo "  logs [componente] - Mostra logs (ex: logs proposer-1)"
  echo "  fail1 [store_id]  - Simula falha no Store sem pedido (Caso 1)"
  echo "  fail2 [store_id]  - Simula falha no Store com pedido (Caso 2)"
  echo "  fail3 [store_id]  - Simula falha no Store após 'ready' (Caso 3)"
  echo "  help              - Mostra esta ajuda"
  echo
  echo "Acesso às interfaces web:"
  echo "  - Prometheus: http://localhost:9090"
  echo "  - Grafana: http://localhost:3000 (admin/admin)"
  echo
}

# Menu principal
case "$1" in
  start)
    start_system
    ;;
  stop)
    stop_system
    ;;
  status)
    check_services
    ;;
  logs)
    show_logs $2
    ;;
  fail1)
    simulate_failure_store_without_request $2
    ;;
  fail2)
    simulate_failure_store_with_request $2
    ;;
  fail3)
    simulate_failure_store_after_ready $2
    ;;
  help|*)
    show_help
    ;;
esac