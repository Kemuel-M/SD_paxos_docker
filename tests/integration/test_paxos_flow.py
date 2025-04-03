"""
Testes de integração para o fluxo completo do protocolo Paxos.
Verifica a comunicação entre Proposer -> Acceptor -> Learner -> Store.
"""
import pytest
import requests
import time
import json
from datetime import datetime
import logging
from tenacity import retry, stop_after_attempt, wait_fixed

# Configurar logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# URLs dos componentes para testes
PROPOSER_URL = "http://localhost:8091"
LEARNER_URL = "http://localhost:8092"
STORE_1_URL = "http://localhost:8093"
STORE_2_URL = "http://localhost:8094"
STORE_3_URL = "http://localhost:8095"

# Marca para testes que exigem containers docker
pytestmark = pytest.mark.skipif(
    "not config.getoption('--runintegration')",
    reason="Precisa da flag --runintegration para executar"
)

# Verificar se o ambiente Docker está disponível
@pytest.fixture(scope="module")
def docker_services_available(docker_ip, docker_services):
    """Esperar os serviços Docker estarem disponíveis."""
    # Verificar se os serviços estão rodando
    docker_services.wait_until_responsive(
        timeout=30.0, pause=0.1, check=lambda: is_responsive(PROPOSER_URL)
    )
    docker_services.wait_until_responsive(
        timeout=30.0, pause=0.1, check=lambda: is_responsive(LEARNER_URL)
    )
    docker_services.wait_until_responsive(
        timeout=30.0, pause=0.1, check=lambda: is_responsive(STORE_1_URL)
    )
    return True

def is_responsive(url):
    """Verificar se o serviço está respondendo."""
    try:
        response = requests.get(f"{url}/status", timeout=2)
        return response.status_code == 200
    except requests.exceptions.ConnectionError:
        return False

@retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
def wait_for_system_ready():
    """Esperar todos os componentes estarem prontos."""
    responses = []
    urls = [PROPOSER_URL, LEARNER_URL, STORE_1_URL, STORE_2_URL, STORE_3_URL]
    
    for url in urls:
        try:
            response = requests.get(f"{url}/status", timeout=2)
            if response.status_code != 200:
                return False
            responses.append(response.json())
        except requests.exceptions.RequestException:
            return False
    
    # Verificar se proposer é líder
    proposer_data = responses[0]
    if not proposer_data.get("is_leader", False):
        return False
    
    # Verificar se learner está conectado aos stores
    learner_data = responses[1]
    if 'store_status' not in learner_data:
        return False
    
    # Verificar se todos os stores estão ativos
    for i in range(2, 5):
        if i >= len(responses):
            return False
        if responses[i].get("is_recovering", False):
            return False
    
    return True

# Teste de fluxo completo
@pytest.mark.asyncio
async def test_full_paxos_flow(docker_services_available):
    """
    Testar o fluxo completo do sistema Paxos.
    Proposer -> Acceptor -> Learner -> Store.
    """
    # Aguardar sistema inicializar completamente
    wait_for_system_ready()
    
    # Preparar dados de teste 
    test_data = {
        "operation": "CREATE",
        "path": f"/integration_test_{int(time.time())}",
        "content": f"Test content created at {datetime.now().isoformat()}"
    }
    
    # 1. Enviar proposta para o Proposer
    logger.info(f"Enviando proposta para Proposer: {test_data}")
    response = requests.post(f"{PROPOSER_URL}/propose", json=test_data)
    assert response.status_code == 200, f"Falha ao propor: {response.text}"
    
    proposal_data = response.json()
    assert proposal_data["success"] == True, "Proposta não foi aceita pelo Proposer"
    assert "queued" in proposal_data, "Resposta não contém campo 'queued'"
    
    # 2. Aguardar processamento da proposta (devido ao batching)
    time.sleep(3)
    
    # 3. Verificar se o recurso foi criado no Store (ler do Learner)
    logger.info(f"Verificando recurso no Learner: {test_data['path']}")
    
    # Retry logic para dar tempo ao sistema processar a operação
    @retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
    def verify_resource_in_learner():
        learner_response = requests.get(f"{LEARNER_URL}/files{test_data['path']}")
        assert learner_response.status_code == 200, f"Falha ao ler recurso do Learner: {learner_response.text}"
        learner_data = learner_response.json()
        assert learner_data["success"] == True, f"Learner reporta falha: {learner_data}"
        assert learner_data["content"] == test_data["content"], "Conteúdo do recurso não corresponde ao esperado"
        return learner_data
    
    learner_data = verify_resource_in_learner()
    logger.info(f"Recurso verificado no Learner: {learner_data}")
    
    # 4. Verificar se o recurso foi replicado em todos os nós do Store
    @retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
    def verify_resource_in_store(store_url):
        store_response = requests.get(f"{store_url}/resources{test_data['path']}")
        assert store_response.status_code == 200, f"Falha ao ler recurso do Store: {store_response.text}"
        store_data = store_response.json()
        assert store_data["success"] == True, f"Store reporta falha: {store_data}"
        assert store_data["content"] == test_data["content"], "Conteúdo do recurso não corresponde ao esperado"
        return store_data
    
    # Verificar em cada Store
    store_1_data = verify_resource_in_store(STORE_1_URL)
    store_2_data = verify_resource_in_store(STORE_2_URL)
    store_3_data = verify_resource_in_store(STORE_3_URL)
    
    logger.info(f"Recurso verificado em todos os Stores")
    
    # 5. Verificar consistência entre os Stores
    assert store_1_data["version"] == store_2_data["version"] == store_3_data["version"], \
        "Versões inconsistentes entre os Stores"

# Teste de fluxo de rejeição
@pytest.mark.asyncio
async def test_rejection_flow(docker_services_available):
    """
    Testar o fluxo de rejeição, quando o número da proposta é muito baixo.
    """
    # Aguardar sistema inicializar completamente
    wait_for_system_ready()
    
    # Este teste é mais complexo e requer acesso direto às APIs internas
    # Como alternativa, podemos testar um cenário de edição concorrente 
    # que pode levar a rejeições
    
    # Preparar dados para teste concorrente
    path = f"/concurrent_test_{int(time.time())}"
    
    # Primeiro, criar o recurso
    create_data = {
        "operation": "CREATE",
        "path": path,
        "content": "Initial content"
    }
    
    logger.info(f"Criando recurso inicial: {create_data}")
    response = requests.post(f"{PROPOSER_URL}/propose", json=create_data)
    assert response.status_code == 200, f"Falha ao criar recurso: {response.text}"
    
    # Aguardar criação
    time.sleep(3)
    
    # Agora enviar duas modificações quase simultaneamente
    # Ambas devem ser processadas, mas em ordem específica
    modify_data_1 = {
        "operation": "MODIFY",
        "path": path,
        "content": "Modified content 1"
    }
    
    modify_data_2 = {
        "operation": "MODIFY",
        "path": path,
        "content": "Modified content 2"
    }
    
    # Enviar primeira modificação
    logger.info(f"Enviando primeira modificação: {modify_data_1}")
    response1 = requests.post(f"{PROPOSER_URL}/propose", json=modify_data_1)
    assert response1.status_code == 200, f"Falha ao modificar (1): {response1.text}"
    
    # Enviar segunda modificação imediatamente
    logger.info(f"Enviando segunda modificação: {modify_data_2}")
    response2 = requests.post(f"{PROPOSER_URL}/propose", json=modify_data_2)
    assert response2.status_code == 200, f"Falha ao modificar (2): {response2.text}"
    
    # Aguardar processamento
    time.sleep(5)
    
    # Verificar conteúdo final - deve ser o da segunda modificação devido à ordem
    @retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
    def verify_final_content():
        response = requests.get(f"{LEARNER_URL}/files{path}")
        assert response.status_code == 200, f"Falha ao ler recurso: {response.text}"
        data = response.json()
        assert data["success"] == True, f"Learner reporta falha: {data}"
        # A segunda modificação deve prevalecer
        assert data["content"] == "Modified content 2", "Conteúdo final não é o esperado"
        return data
    
    final_data = verify_final_content()
    logger.info(f"Conteúdo final verificado: {final_data}")