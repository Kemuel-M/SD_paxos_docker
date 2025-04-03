"""
Testes unitários para o módulo de persistência.
"""
import os
import json
import time
import pytest
import asyncio
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, AsyncMock

from persistence import ProposerPersistence

@pytest.fixture
def temp_data_dir():
    """Fixture que cria um diretório temporário para dados de teste."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)

@pytest.fixture
def persistence(temp_data_dir):
    """Fixture que cria uma instância de ProposerPersistence para testes."""
    return ProposerPersistence(node_id=1, data_dir=temp_data_dir)

def test_initialization(persistence, temp_data_dir):
    """Testa se a persistência é inicializada corretamente."""
    # Verifica se os diretórios foram criados
    assert os.path.exists(temp_data_dir)
    assert os.path.exists(os.path.join(temp_data_dir, "checkpoints"))
    
    # Verifica os caminhos dos arquivos
    assert persistence.state_file == Path(temp_data_dir) / "proposer_1_state.json"
    assert persistence.temp_file == Path(temp_data_dir) / "proposer_1_state.tmp.json"
    assert persistence.checkpoint_dir == Path(temp_data_dir) / "checkpoints"

def test_load_state_new(persistence):
    """Testa carregamento de estado quando não existe arquivo."""
    # Carrega estado (não existe arquivo)
    state = persistence.load_state()
    
    # Verifica se retornou estado padrão
    assert state["proposal_counter"] == 0
    assert state["last_instance_id"] == 0
    assert state["current_leader"] is None
    assert state["current_term"] == 0

def test_load_state_existing(persistence):
    """Testa carregamento de estado de arquivo existente."""
    # Cria arquivo de estado
    test_state = {
        "proposal_counter": 42,
        "last_instance_id": 100,
        "current_leader": 3,
        "current_term": 5
    }
    
    with open(persistence.state_file, 'w') as f:
        json.dump(test_state, f)
    
    # Carrega estado
    state = persistence.load_state()
    
    # Verifica se carregou corretamente
    assert state["proposal_counter"] == 42
    assert state["last_instance_id"] == 100
    assert state["current_leader"] == 3
    assert state["current_term"] == 5

def test_load_state_corrupted(persistence):
    """Testa carregamento quando o arquivo está corrompido."""
    # Cria arquivo corrompido
    with open(persistence.state_file, 'w') as f:
        f.write("This is not valid JSON")
    
    # Tenta carregar estado
    state = persistence.load_state()
    
    # Verifica se retornou estado padrão
    assert state["proposal_counter"] == 0
    assert state["last_instance_id"] == 0
    assert state["current_leader"] is None
    assert state["current_term"] == 0

@pytest.mark.asyncio
async def test_save_state(persistence):
    """Testa salvamento de estado."""
    # Estado para salvar
    test_state = {
        "proposal_counter": 42,
        "last_instance_id": 100,
        "current_leader": 3,
        "current_term": 5
    }
    
    # Salva estado
    result = await persistence.save_state(test_state)
    
    # Verifica resultado
    assert result == True
    
    # Verifica se arquivo foi criado
    assert os.path.exists(persistence.state_file)
    
    # Verifica conteúdo
    with open(persistence.state_file, 'r') as f:
        saved_state = json.load(f)
        assert saved_state["proposal_counter"] == 42
        assert saved_state["last_instance_id"] == 100
        assert saved_state["current_leader"] == 3
        assert saved_state["current_term"] == 5

@pytest.mark.asyncio
async def test_save_state_atomic(persistence):
    """Testa atomicidade do salvamento de estado."""
    # Estado para salvar
    test_state = {
        "proposal_counter": 42,
        "last_instance_id": 100,
        "current_leader": 3,
        "current_term": 5
    }
    
    # Simula falha na renomeação do arquivo temporário
    with patch('pathlib.Path.replace', side_effect=Exception("Simulated failure")):
        result = await persistence.save_state(test_state)
        
        # Verifica que falhou
        assert result == False
        
        # Verifica que não existe arquivo final
        assert not os.path.exists(persistence.state_file)
        
        # Mas pode existir arquivo temporário
        # (não verificamos isso pois a implementação pode limpá-lo)

@pytest.mark.asyncio
async def test_checkpoint_creation(persistence):
    """Testa criação de checkpoint."""
    # Define contador de operações para forçar checkpoint
    persistence.op_counter = 99
    
    # Estado para salvar
    test_state = {
        "proposal_counter": 42,
        "last_instance_id": 100,
        "current_leader": 3,
        "current_term": 5
    }
    
    # Salva estado
    result = await persistence.save_state(test_state)
    
    # Verifica resultado
    assert result == True
    
    # Verifica se contador foi incrementado
    assert persistence.op_counter == 100
    
    # Verifica se checkpoint foi criado (pelo menos um arquivo no diretório)
    checkpoints = list(persistence.checkpoint_dir.glob(f"proposer_{persistence.node_id}_cp_*.json"))
    assert len(checkpoints) > 0
    
    # Verifica conteúdo do checkpoint
    with open(checkpoints[0], 'r') as f:
        cp_state = json.load(f)
        assert cp_state["proposal_counter"] == 42
        assert cp_state["last_instance_id"] == 100
        assert cp_state["current_leader"] == 3
        assert cp_state["current_term"] == 5

@pytest.mark.asyncio
async def test_cleanup_old_checkpoints(persistence):
    """Testa limpeza de checkpoints antigos."""
    # Cria vários checkpoints de teste
    for i in range(10):
        cp_file = persistence.checkpoint_dir / f"proposer_{persistence.node_id}_cp_{i}_{int(time.time())}.json"
        with open(cp_file, 'w') as f:
            json.dump({"test": i}, f)
        
        # Pequeno atraso para garantir timestamps diferentes
        await asyncio.sleep(0.01)
    
    # Executa limpeza (mantendo 5)
    await persistence._cleanup_old_checkpoints(keep=5)
    
    # Verifica se apenas 5 checkpoints permanecem
    checkpoints = list(persistence.checkpoint_dir.glob(f"proposer_{persistence.node_id}_cp_*.json"))
    assert len(checkpoints) == 5
    
    # Verifica se são os mais recentes (maiores valores de i)
    for cp in checkpoints:
        with open(cp, 'r') as f:
            state = json.load(f)
            assert state["test"] >= 5

def test_restore_from_checkpoint(persistence):
    """Testa restauração a partir de checkpoint."""
    # Cria alguns checkpoints de teste
    for i in range(3):
        cp_file = persistence.checkpoint_dir / f"proposer_{persistence.node_id}_cp_{i}_{int(time.time())}.json"
        with open(cp_file, 'w') as f:
            json.dump({"proposal_counter": i, "test": i}, f)
        
        # Pequeno atraso para garantir timestamps diferentes
        time.sleep(0.01)
    
    # Executa restauração
    state = persistence._restore_from_checkpoint()
    
    # Verifica se restaurou o checkpoint mais recente
    assert state["proposal_counter"] == 2
    assert state["test"] == 2

def test_restore_no_checkpoints(persistence):
    """Testa restauração quando não há checkpoints."""
    # Executa restauração
    state = persistence._restore_from_checkpoint()
    
    # Verifica se retornou estado padrão
    assert state["proposal_counter"] == 0
    assert state["last_instance_id"] == 0
    assert state["current_leader"] is None
    assert state["current_term"] == 0