# Proposer (Cluster Sync)

Este componente implementa o Proposer para o algoritmo de consenso Paxos no sistema distribuído. O Proposer é responsável por iniciar o processo de consenso, coordenar com Acceptors para chegar a um acordo sobre as operações dos clientes e garantir a ordem global das operações.

## Características

- Implementação completa do algoritmo Paxos
- Suporte a Multi-Paxos com eleição de líder para melhor desempenho
- Detecção e recuperação de falhas automática
- Persistência de estado para sobreviver a reinicializações
- Interface REST API para comunicação com outros componentes
- Sistema de logging detalhado para depuração

## Arquitetura

O Proposer é composto pelos seguintes módulos:

- **main.py**: Ponto de entrada da aplicação
- **api.py**: Endpoints da API REST
- **proposer.py**: Implementação do algoritmo Paxos
- **leader.py**: Lógica de eleição de líder para Multi-Paxos
- **persistence.py**: Armazenamento persistente de estado
- **common/**: Módulos compartilhados entre componentes
  - **communication.py**: Utilitários de comunicação HTTP com retry e circuit breaker
  - **logging.py**: Sistema de logging unificado
  - **heartbeat.py**: Sistema de heartbeat para detecção de falhas
  - **utils.py**: Funções de utilidade geral

## Requisitos

- Python 3.8+
- Docker
- Bibliotecas Python listadas em `requirements.txt`

## Configuração

O componente é configurado através de variáveis de ambiente:

- `NODE_ID`: ID único deste Proposer (1-5)
- `PORT`: Porta HTTP para escutar (padrão: 8080)
- `DEBUG`: Habilita logs detalhados (true/false)
- `ACCEPTORS`: Lista de endereços dos Acceptors (formato: host:port,host:port,...)
- `PROPOSERS`: Lista de endereços dos Proposers (formato: host:port,host:port,...)
- `LEARNERS`: Lista de endereços dos Learners (formato: host:port,host:port,...)
- `STORES`: Lista de endereços dos Cluster Stores (formato: host:port,host:port,...)
- `USE_CLUSTER_STORE`: Habilita integração com Cluster Store na Parte 2 (true/false)

## Instalação

```bash
# Clone o repositório
git clone <repo-url>
cd paxos-consensus-system/proposer

# Instale dependências
pip install -r requirements.txt

# Execute o componente
python main.py
```

## Executando com Docker

```bash
# Construa a imagem
docker build -t paxos-proposer .

# Execute o container
docker run -p 8080:8080 \
  -e NODE_ID=1 \
  -e DEBUG=true \
  -e ACCEPTORS=acceptor-1:8080,acceptor-2:8080,acceptor-3:8080,acceptor-4:8080,acceptor-5:8080 \
  -e PROPOSERS=proposer-1:8080,proposer-2:8080,proposer-3:8080,proposer-4:8080,proposer-5:8080 \
  -e LEARNERS=learner-1:8080,learner-2:8080 \
  -e STORES=cluster-store-1:8080,cluster-store-2:8080,cluster-store-3:8080 \
  -v $(pwd)/data:/data \
  paxos-proposer
```

## API REST

O Proposer expõe os seguintes endpoints:

- `POST /propose`: Recebe requisições de clientes
  - Corpo: Objeto JSON com os detalhes da requisição
  - Resposta: Status 202 (Accepted) se aceita para processamento

- `GET /status`: Retorna o estado atual do Proposer
  - Resposta: Objeto JSON com informações sobre o estado

- `GET /health`: Endpoint para verificação de saúde (heartbeat)
  - Resposta: Status 200 com `{"status": "healthy"}` se funcionando corretamente

- `GET /logs`: Retorna logs do componente (requer DEBUG=true)
  - Resposta: Lista de entradas de log

- `GET /logs/important`: Retorna apenas logs importantes
  - Resposta: Lista de entradas de log importantes

- `GET /leader-status`: Retorna informações do líder (apenas se for líder)
  - Resposta: Objeto JSON com informações sobre o líder

- `POST /leader-heartbeat`: Recebe heartbeat do líder
  - Corpo: Objeto JSON com informações do líder
  - Resposta: Status 200 se processado com sucesso

## Testes

Para executar os testes:

```bash
# Instale as dependências de teste
pip install pytest pytest-asyncio httpx

# Execute os testes unitários
pytest tests/unit

# Execute os testes de integração
pytest tests/integration
```

## Algoritmo Paxos

Este componente implementa o algoritmo Paxos para consenso distribuído, com as seguintes fases:

1. **Fase Prepare**:
   - Proposer seleciona número de proposta e envia para Acceptors
   - Acceptors respondem com promise se o número for maior que qualquer anterior

2. **Fase Accept**:
   - Se recebeu maioria de promises, Proposer envia accept com valor
   - Acceptors aceitam se não prometeram para número maior

3. **Otimização Multi-Paxos**:
   - Líder eleito pode pular fase Prepare para instâncias subsequentes
   - Eleição de líder é realizada usando o próprio Paxos (instância 0)

## Logs e Monitoramento

O componente gera logs detalhados quando `DEBUG=true` e logs importantes sempre. Os logs são armazenados em:

- `/data/logs/proposer-X_all.log`: Todos os logs
- `/data/logs/proposer-X_important.log`: Logs importantes

Onde X é o NODE_ID.

## Persistência

O estado do Proposer é persistido em `/data/proposer_X_state.json`, permitindo recuperação após reinicialização. Checkpoints periódicos são criados em `/data/checkpoints/`.

## Tratamento de Falhas

O componente implementa detecção de falhas baseada em heartbeats. Se o líder atual falhar, uma nova eleição é iniciada automaticamente. Circuit breakers protegem contra falhas em cascata.