"""
Implementação do armazenamento persistente para o Proposer.
Responsável por salvar e carregar o estado entre reinicializações.
"""
import os
import json
import logging
import asyncio
from typing import Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger("proposer")

class ProposerPersistence:
    """
    Gerencia a persistência de estado do Proposer.
    
    Responsável por:
    1. Salvar o estado do proposer em arquivo
    2. Carregar o estado ao inicializar
    3. Criar checkpoints periódicos
    """
    
    def __init__(self, node_id: int, data_dir: str = "/data"):
        """
        Inicializa o gerenciador de persistência.
        
        Args:
            node_id: ID do Proposer
            data_dir: Diretório para armazenar dados persistentes
        """
        self.node_id = node_id
        self.data_dir = Path(data_dir)
        
        # Cria diretório de dados se não existir
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Caminho para o arquivo de estado
        self.state_file = self.data_dir / f"proposer_{node_id}_state.json"
        
        # Caminho para arquivo temporário (para escrita atômica)
        self.temp_file = self.data_dir / f"proposer_{node_id}_state.tmp.json"
        
        # Caminho para arquivo de checkpoint
        self.checkpoint_dir = self.data_dir / "checkpoints"
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        
        # Lock para escrita
        self.write_lock = asyncio.Lock()
        
        # Contador de operações
        self.op_counter = 0
        
        logger.info(f"Persistência inicializada. Arquivo de estado: {self.state_file}")
    
    def load_state(self) -> Dict[str, Any]:
        """
        Carrega o estado do Proposer a partir do arquivo.
        
        Returns:
            Dict[str, Any]: Estado carregado ou estado padrão se não existir
        """
        try:
            if self.state_file.exists():
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                    logger.info(f"Estado carregado com sucesso: proposal_counter={state.get('proposal_counter', 0)}, "
                                f"last_instance_id={state.get('last_instance_id', 0)}")
                    return state
        except Exception as e:
            logger.error(f"Erro ao carregar estado: {e}", exc_info=True)
            
            # Tenta restaurar de checkpoint se a leitura falhar
            return self._restore_from_checkpoint()
        
        # Retorna estado padrão se não existir arquivo ou falhar
        logger.info("Arquivo de estado não encontrado, iniciando com estado padrão")
        return self._get_default_state()
    
    async def save_state(self, state: Optional[Dict[str, Any]] = None) -> bool:
        """
        Salva o estado do Proposer em arquivo.
        
        Args:
            state: Estado a ser salvo, ou None para usar estado padrão
        
        Returns:
            bool: True se salvou com sucesso, False caso contrário
        """
        if state is None:
            state = self._get_default_state()
        
        async with self.write_lock:
            try:
                # Escreve em arquivo temporário
                with open(self.temp_file, 'w') as f:
                    json.dump(state, f, indent=2)
                
                # Renomeia atomicamente para arquivo final
                self.temp_file.replace(self.state_file)
                
                # Incrementa contador de operações
                self.op_counter += 1
                
                # Cria checkpoint a cada 100 operações
                if self.op_counter % 100 == 0:
                    await self._create_checkpoint(state)
                
                return True
                
            except Exception as e:
                logger.error(f"Erro ao salvar estado: {e}", exc_info=True)
                return False
    
    async def _create_checkpoint(self, state: Dict[str, Any]) -> bool:
        """
        Cria arquivo de checkpoint do estado atual.
        
        Args:
            state: Estado a ser salvo no checkpoint
        
        Returns:
            bool: True se criou checkpoint com sucesso, False caso contrário
        """
        try:
            # Cria nome do arquivo com timestamp e contador de proposta
            checkpoint_file = self.checkpoint_dir / f"proposer_{self.node_id}_cp_{state.get('proposal_counter', 0)}_{int(asyncio.get_event_loop().time())}.json"
            
            # Escreve checkpoint
            with open(checkpoint_file, 'w') as f:
                json.dump(state, f, indent=2)
            
            logger.info(f"Checkpoint criado: {checkpoint_file}")
            
            # Limpa checkpoints antigos (mantém apenas os 5 mais recentes)
            await self._cleanup_old_checkpoints()
            
            return True
            
        except Exception as e:
            logger.error(f"Erro ao criar checkpoint: {e}", exc_info=True)
            return False
    
    async def _cleanup_old_checkpoints(self, keep: int = 5):
        """
        Remove checkpoints antigos, mantendo apenas os mais recentes.
        
        Args:
            keep: Número de checkpoints a manter
        """
        try:
            # Lista checkpoints ordenados por data de modificação
            checkpoints = sorted(
                [f for f in self.checkpoint_dir.glob(f"proposer_{self.node_id}_cp_*.json")],
                key=lambda f: f.stat().st_mtime,
                reverse=True
            )
            
            # Remove os mais antigos, mantendo 'keep' arquivos
            for old_cp in checkpoints[keep:]:
                old_cp.unlink()
                logger.info(f"Checkpoint antigo removido: {old_cp}")
            
        except Exception as e:
            logger.error(f"Erro ao limpar checkpoints antigos: {e}", exc_info=True)
    
    def _restore_from_checkpoint(self) -> Dict[str, Any]:
        """
        Tenta restaurar o estado a partir do checkpoint mais recente.
        
        Returns:
            Dict[str, Any]: Estado restaurado do checkpoint ou estado padrão
        """
        try:
            # Busca o checkpoint mais recente
            checkpoints = sorted(
                [f for f in self.checkpoint_dir.glob(f"proposer_{self.node_id}_cp_*.json")],
                key=lambda f: f.stat().st_mtime,
                reverse=True
            )
            
            if not checkpoints:
                logger.warning("Nenhum checkpoint encontrado para restauração")
                return self._get_default_state()
            
            # Carrega o checkpoint mais recente
            with open(checkpoints[0], 'r') as f:
                state = json.load(f)
                
            logger.info(f"Estado restaurado do checkpoint: {checkpoints[0]}")
            return state
            
        except Exception as e:
            logger.error(f"Erro ao restaurar de checkpoint: {e}", exc_info=True)
            return self._get_default_state()
    
    def _get_default_state(self) -> Dict[str, Any]:
        """
        Retorna o estado padrão para inicialização.
        
        Returns:
            Dict[str, Any]: Estado padrão
        """
        return {
            "proposal_counter": 0,
            "last_instance_id": 0,
            "current_leader": None,
            "current_term": 0
        }