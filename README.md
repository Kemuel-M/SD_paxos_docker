# Sistema de Consenso Distribuído Paxos em Docker

Este projeto implementa um sistema de consenso distribuído baseado no algoritmo Paxos, dividido em duas partes:

1. **Parte 1**: Implementação do protocolo de sincronização distribuída (Paxos)
2. **Parte 2**: Implementação da estratégia de replicação (ROWA) e tolerância a falhas

## Visão Geral da Arquitetura

O sistema é composto pelos seguintes componentes:

- **Proposers (Cluster Sync)** - 5 instâncias: Recebem requisições dos clientes e iniciam o protocolo Paxos.
- **Acceptors** - 5 instâncias: Votam em propostas e mantêm o estado persistente do protocolo.
- **Learners** - 2 instâncias: Observam o processo de aceitação e determinam quando um consenso foi alcançado.
- **Cluster Store** - 3 instâncias: Armazenam e gerenciam o recurso compartilhado R.
- **Clientes** - 5 instâncias: Enviam requisições de acesso ao recurso.

Além disso, o sistema inclui ferramentas de monitoramento:

- **Prometheus**: Coleta métricas de todos os componentes.
- **Grafana**: Fornece dashboards para visualização das métricas.

## Algoritmo Paxos

O Paxos é executado em duas fases principais:

1. **Fase Prepare**:
   - Proposer envia mensagem "prepare(n)" para os acceptors.
   - Acceptors respondem com "promise" ou "not promise".

2. **Fase Accept**:
   - Se o proposer receber promises da maioria, envia mensagem "accept(n, v)".
   - Acceptors respondem com "accepted" ou "not accepted".

3. **Fase de Aprendizado**:
   - Learners determinam quando um valor foi decidido pela maioria.

## Protocolo de Replicação ROWA

Na Parte 2, implementamos a estratégia Read-One, Write-All (ROWA):

- **Leitura (Nr=1)**: Lê de qualquer réplica única.
- **Escrita (Nw=3)**: Escreve em todas as réplicas disponíveis.

## Requisitos do Sistema

- Docker
- Docker Compose

## Como Executar

1. Clone o repositório:
   ```bash
   git clone <repositório>
   cd paxos-sistema
   ```

2. Torne o script de execução executável:
   ```bash
   chmod +x run.sh
   ```

3. Inicie o sistema:
   ```bash
   ./run.sh start
   ```

4. Verifique o status dos serviços:
   ```bash
   ./run.sh status
   ```

5. Acesse os dashboards de monitoramento:
   - Prometheus: http://localhost:9090
   - Grafana: http://localhost:3000 (usuário: admin, senha: admin)

6. Para visualizar logs:
   ```bash
   ./run.sh logs [componente]
   ```
   Por exemplo: `./run.sh logs proposer-1`

7. Para parar o sistema:
   ```bash
   ./run.sh stop
   ```

## Simulação de Falhas

O sistema implementa três cenários específicos de falha:

1. **Falha de nó do Cluster Store sem pedido pendente**:
   ```bash
   ./run.sh fail1 [store_id]
   ```

2. **Falha de nó do Cluster Store com pedido pendente**:
   ```bash
   ./run.sh fail2 [store_id]
   ```

3. **Falha de nó do Cluster Store após confirmação na fase prepare**:
   ```bash
   ./run.sh fail3 [store_id]
   ```

## Estrutura do Projeto

```
paxos-sistema/
│
├── proposer/          # Código para o Proposer (Cluster Sync)
├── acceptor/          # Código para o Acceptor
├── learner/           # Código para o Learner
├── store/             # Código para o Cluster Store
├── client/            # Código para o Cliente
├── common/            # Código comum a todos os componentes
├── prometheus/        # Configuração do Prometheus
├── grafana/           # Configuração do Grafana
├── run.sh             # Script para execução e teste
└── docker-compose.yml # Configuração Docker Compose
```

## Monitoramento

O sistema inclui uma infraestrutura completa de monitoramento:

- **Métricas coletadas**: Número de propostas, durações de fases, consensos alcançados, operações de leitura/escrita, etc.
- **Dashboards**: Visão geral do sistema, protocolo Paxos, replicação e falhas.
- **Logs estruturados**: Todos os componentes geram logs em formato JSON para facilitar a análise.

## Limitações e Considerações

- Este projeto é uma implementação educacional e não deve ser usado em produção sem revisões adicionais.
- A implementação utiliza FastAPI para a comunicação HTTP entre componentes.
- O simulador de falhas é uma aproximação e pode não capturar todos os cenários reais de falha.