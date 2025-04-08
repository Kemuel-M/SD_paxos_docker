# Guia do Sistema de Consenso Distribuído Paxos

Este documento descreve como iniciar, monitorar e interagir com o sistema de consenso distribuído baseado no algoritmo Paxos.

## Visão Geral da Arquitetura

O sistema é composto pelos seguintes componentes:

- **Proposers (5 nós)**: Iniciam o processo de consenso, recebem requisições dos clientes.
- **Acceptors (5 nós)**: Recebem propostas e decidem se as aceitam ou não.
- **Learners (2 nós)**: Aprendem sobre as decisões tomadas e as aplicam.
- **Cluster Store (3 nós)**: Armazenam dados de forma replicada e consistente.
- **Clients (5 nós)**: Fornecem interface para usuários interagirem com o sistema.

Todos os componentes se comunicam via HTTP REST APIs.

## Requisitos

- Docker
- Docker Compose

## Inicializando o Sistema

### Método 1: Usando o script

1. Torne o script executável:
   ```bash
   chmod +x start-paxos-network.sh
   ```

2. Execute o script:
   ```bash
   ./start-paxos-network.sh
   ```

   Para iniciar em modo detached (background):
   ```bash
   ./start-paxos-network.sh -d
   ```

### Método 2: Usando Docker Compose diretamente

1. Iniciar todos os serviços:
   ```bash
   docker-compose up --build
   ```

   Para iniciar em modo detached:
   ```bash
   docker-compose up -d --build
   ```

## Acessando o Sistema

### Interfaces Web (Clientes)

- Cliente 1: http://localhost:8501
- Cliente 2: http://localhost:8502
- Cliente 3: http://localhost:8503
- Cliente 4: http://localhost:8504
- Cliente 5: http://localhost:8505

Cada interface web de cliente permite:
- Ver o dashboard com estatísticas
- Ver os recursos disponíveis
- Editar recursos
- Ver histórico de operações
- Ver logs de operações

### APIs REST

Os seguintes endpoints estão disponíveis:

- **Proposers**: http://localhost:8001 até http://localhost:8005
- **Acceptors**: http://localhost:8101 até http://localhost:8105
- **Learners**: http://localhost:8201 até http://localhost:8202
- **Cluster Stores**: http://localhost:8301 até http://localhost:8303
- **Clients API**: http://localhost:8401 até http://localhost:8405

## Monitoramento e Operação

### Visualizando Logs

Para ver os logs de todos os serviços:
```bash
docker-compose logs -f
```

Para ver logs de um serviço específico:
```bash
docker-compose logs -f [serviço]
```

Exemplos:
```bash
docker-compose logs -f proposer-1
docker-compose logs -f acceptor-2
docker-compose logs -f learner-1
```

### Debugging

Cada componente tem um endpoint `/debug/config` que permite alterar o nível de debug em tempo de execução:

```bash
curl -X POST http://localhost:8001/debug/config -H "Content-Type: application/json" -d '{"enabled": true, "level": "advanced"}'
```

Os níveis de debug disponíveis são:
- `basic`: Logs básicos
- `advanced`: Logs detalhados
- `trace`: Logs extremamente detalhados para depuração

### Simulando Falhas

O script `scripts/simulate_failures.sh` (não implementado) pode ser usado para testar a resiliência do sistema simulando falhas em diferentes componentes.

## Parando o Sistema

Para parar todos os serviços:
```bash
docker-compose down
```

Para parar e remover volumes (CUIDADO: isso apagará todos os dados persistentes):
```bash
docker-compose down -v
```

## Estrutura de Dados Persistentes

Os dados persistentes são armazenados em diretórios mapeados na pasta `./data`:

```
./data/
├── proposer-1/
├── proposer-2/
...
├── acceptor-1/
├── acceptor-2/
...
├── learner-1/
├── learner-2/
├── cluster-store-1/
├── cluster-store-2/
├── cluster-store-3/
├── client-1/
├── client-2/
...
```

## Arquitetura Detalhada

### Protocolo Paxos

O sistema implementa o algoritmo Multi-Paxos com as seguintes otimizações:
- Eleição de líder entre os Proposers para reduzir colisões
- Pulo da fase Prepare para instâncias subsequentes quando há um líder estável
- Mecanismo de heartbeat para detecção de falhas
- Circuit breaker para evitar falhas em cascata

### Replicação de Dados

Os dados são replicados usando o protocolo Read-One-Write-All (ROWA) com coordenação via Two-Phase Commit (2PC), garantindo que todos os nós do Cluster Store tenham versões consistentes dos recursos.

## Solução de Problemas

### Comportamento de Reeleição de Líder

Se o líder atual falhar, um novo líder será eleito automaticamente entre os Proposers restantes. Durante este período, pode haver uma breve indisponibilidade do sistema.

### Falhas de Consenso

Se não for possível alcançar consenso (por exemplo, se a maioria dos Acceptors estiver indisponível), as operações falharão com mensagens de erro nos logs.

### Sincronização do Cluster Store

Se houver problemas de sincronização no Cluster Store, verifique os logs com `docker-compose logs -f cluster-store-1` para diagnóstico.
