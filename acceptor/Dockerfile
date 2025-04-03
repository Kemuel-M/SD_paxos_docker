FROM python:3.10-slim

WORKDIR /app

# Instalar utilitários
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Copiar os arquivos de requisitos
COPY acceptor/requirements.txt .

# Instalar dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código-fonte
COPY acceptor/ .
COPY common/ /app/common/

# Diretório para dados persistentes
RUN mkdir -p /app/data
VOLUME /app/data

# Tornar o script de inicialização executável
RUN chmod +x startup.sh

# Expor a porta
EXPOSE 8080

# Comando para iniciar a aplicação
CMD ["./startup.sh"]