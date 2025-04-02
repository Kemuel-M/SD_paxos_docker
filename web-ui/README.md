# Paxos Web UI

Interface web para interagir com o sistema de consenso distribuído Paxos. Esta interface permite:

1. Explorar e manipular arquivos no sistema distribuído
2. Editar conteúdo de arquivos
3. Visualizar o status de todos os componentes do sistema

## Arquitetura

O Web UI é composto de duas partes principais:

### Backend (FastAPI)

- Serve os arquivos estáticos do frontend React
- Funciona como proxy para os componentes do sistema (proposers, acceptors, learners)
- Implementa lógica de balanceamento simples entre os nós
- Suporta WebSockets para atualizações em tempo real

### Frontend (React)

- Interface moderna e responsiva
- Três componentes principais:
  - **FileExplorer**: Para navegar e manipular arquivos/diretórios
  - **FileEditor**: Para editar conteúdo de arquivos
  - **SystemStatus**: Para visualizar o status de todos os componentes

## Como Usar

### Explorando Arquivos

1. Navegue pela interface de diretórios clicando nas pastas
2. Crie novos arquivos ou diretórios com os botões "New File" e "New Directory"
3. Edite arquivos clicando neles ou no botão "Edit"
4. Exclua arquivos ou diretórios usando o botão "Delete"

### Editando Arquivos

1. Abra um arquivo para edição clicando nele na interface do explorador
2. Modifique o conteúdo do arquivo no editor
3. Salve as alterações clicando em "Save"
4. O asterisco (*) indica alterações não salvas

### Monitorando o Sistema

1. Acesse a página "System Status" para visualizar o estado de todos os componentes
2. Veja detalhes sobre:
   - Quais nós estão ativos/inativos
   - Qual proposer é o líder atual
   - Contadores de propostas, promessas e aceitações
   - Estatísticas de arquivos e diretórios
3. Configure a atualização automática com o botão "Auto Refresh"

## Detalhes Técnicos

- O frontend se comunica com o backend através de APIs REST
- As operações de arquivo são propostas ao sistema Paxos através dos proposers
- As leituras de arquivos são feitas diretamente dos learners
- Atualizações em tempo real são entregues via WebSockets

## Fluxo de Consenso

Quando você cria, modifica ou exclui um arquivo:

1. A operação é enviada para um proposer
2. O proposer inicia o protocolo Paxos com os acceptors
3. Quando o consenso é alcançado, os learners aplicam a operação ao sistema de arquivos
4. A interface é atualizada com o novo estado

Esta arquitetura garante que todas as operações sejam consistentes em todo o sistema, mesmo em caso de falhas.