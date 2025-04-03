#!/bin/bash

# Criar diretório para common se não existir
mkdir -p /app/common

# Verificar se common existe no diretório atual
if [ -d "/app/common" ]; then
    echo "Diretório common encontrado"
else
    echo "Copiando módulo common..."
    cp -r /app/../common /app/common || true
fi

# Verificar se o código Python está presente
if [ -f "/app/main.py" ]; then
    echo "Código Python encontrado"
else
    echo "Código Python não encontrado"
    exit 1
fi

# Iniciar a aplicação
echo "Iniciando a aplicação..."
exec uvicorn main:app --host 0.0.0.0 --port 8080