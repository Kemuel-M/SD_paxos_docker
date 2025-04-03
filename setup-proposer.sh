#!/bin/bash
# Script de configuração inicial para o componente Proposer (Cluster Sync)

# Cria diretórios necessários
mkdir -p /data/logs
mkdir -p /data/checkpoints

# Verifica variáveis de ambiente necessárias
if [ -z "$NODE_ID" ]; then
  echo "ERRO: Variável NODE_ID não definida. Definindo NODE_ID=1 como padrão."
  export NODE_ID=1
fi

# Configura variáveis baseadas no NODE_ID para ambiente Docker Compose
export PORT=8080

# Se as variáveis ACCEPTORS, PROPOSERS, LEARNERS e STORES não estiverem definidas,
# configura valores padrão para ambiente Docker Compose
if [ -z "$ACCEPTORS" ]; then
  export ACCEPTORS="acceptor-1:8080,acceptor-2:8080,acceptor-3:8080,acceptor-4:8080,acceptor-5:8080"
fi

if [ -z "$PROPOSERS" ]; then
  export PROPOSERS="proposer-1:8080,proposer-2:8080,proposer-3:8080,proposer-4:8080,proposer-5:8080"
fi

if [ -z "$LEARNERS" ]; then
  export LEARNERS="learner-1:8080,learner-2:8080"
fi

if [ -z "$STORES" ]; then
  export STORES="cluster-store-1:8080,cluster-store-2:8080,cluster-store-3:8080"
fi

# Define DEBUG como true se não estiver definido
if [ -z "$DEBUG" ]; then
  export DEBUG=false
fi

# Cria arquivo .env para registro das variáveis configuradas
cat > .env << EOF
NODE_ID=$NODE_ID
PORT=$PORT
DEBUG=$DEBUG
ACCEPTORS=$ACCEPTORS
PROPOSERS=$PROPOSERS
LEARNERS=$LEARNERS
STORES=$STORES
USE_CLUSTER_STORE=${USE_CLUSTER_STORE:-false}
EOF

echo "Configuração concluída. Variáveis de ambiente definidas:"
echo "NODE_ID=$NODE_ID"
echo "PORT=$PORT"
echo "DEBUG=$DEBUG"

# Instalação de dependências se necessário
if [ "$INSTALL_DEPS" = "true" ]; then
  echo "Instalando dependências..."
  pip install -r requirements.txt
fi

echo "Componente Proposer (NODE_ID=$NODE_ID) pronto para iniciar."
echo "Execute 'python main.py' para iniciar o serviço."
