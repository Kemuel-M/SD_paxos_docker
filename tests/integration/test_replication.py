"""
Testes de integração para verificar o mecanismo de replicação entre nós Store.
Verifica se a estratégia ROWA (Read One, Write All) está funcionando corretamente.
"""
import pytest
import requests
import time
import json
import logging
import uuid
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
    
    return True

@pytest.mark.asyncio
async def test_write_replication(docker_services_available):
    """
    Testar se escritas são replicadas em todos os nós Store (ROWA: Write All).
    """
    # Aguardar sistema inicializar completamente
    wait_for_system_ready()
    
    # Criar um recurso com conteúdo único para teste
    unique_id = uuid.uuid4().hex
    test_path = f"/replication_test_{unique_id}"
    test_content = f"Replication test content {unique_id}"
    
    test_data = {
        "operation": "CREATE",
        "path": test_path,
        "content": test_content
    }
    
    # Enviar requisição via Proposer
    logger.info(f"Criando recurso para teste de replicação: {test_data}")
    response = requests.post(f"{PROPOSER_URL}/propose", json=test_data)
    assert response.status_code == 200, f"Falha ao propor: {response.text}"
    
    # Aguardar replicação ocorrer
    time.sleep(3)
    
    # Verificar se o recurso existe em cada Store
    @retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
    def verify_resource_in_store(store_url):
        store_response = requests.get(f"{store_url}/resources{test_path}")
        assert store_response.status_code == 200, f"Falha ao ler recurso do Store: {store_response.text}"
        store_data = store_response.json()
        assert store_data["success"] == True, f"Store reporta falha: {store_data}"
        assert store_data["content"] == test_content, "Conteúdo do recurso não corresponde ao esperado"
        return store_data
    
    # Verificar em cada Store
    store_1_data = verify_resource_in_store(STORE_1_URL)
    store_2_data = verify_resource_in_store(STORE_2_URL)
    store_3_data = verify_resource_in_store(STORE_3_URL)
    
    logger.info("Recurso encontrado em todos os nós Store")
    
    # Verificar se as versões são consistentes
    assert store_1_data["version"] == store_2_data["version"] == store_3_data["version"], \
        "Versões inconsistentes entre os Stores"
    
    # Verificar se os timestamps são próximos
    timestamps = [
        store_1_data.get("timestamp", 0),
        store_2_data.get("timestamp", 0),
        store_3_data.get("timestamp", 0)
    ]
    
    max_timestamp_diff = max(timestamps) - min(timestamps)
    assert max_timestamp_diff < 5, f"Diferença de timestamp muito grande: {max_timestamp_diff} segundos"
    
    logger.info(f"Replicação verificada com sucesso. Versão: {store_1_data['version']}")

@pytest.mark.asyncio
async def test_read_from_any_node(docker_services_available):
    """
    Testar se leituras podem ser feitas de qualquer nó Store (ROWA: Read One).
    """
    # Aguardar sistema inicializar completamente
    wait_for_system_ready()
    
    # Criar um recurso com conteúdo único para teste
    unique_id = uuid.uuid4().hex
    test_path = f"/read_test_{unique_id}"
    test_content = f"Read test content {unique_id}"
    
    test_data = {
        "operation": "CREATE",
        "path": test_path,
        "content": test_content
    }
    
    # Enviar requisição via Proposer
    logger.info(f"Criando recurso para teste de leitura: {test_data}")
    response = requests.post(f"{PROPOSER_URL}/propose", json=test_data)
    assert response.status_code == 200, f"Falha ao propor: {response.text}"
    
    # Aguardar replicação ocorrer
    time.sleep(3)
    
    # Ler diretamente do Learner (que escolherá um dos Stores)
    @retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
    def read_from_learner():
        learner_response = requests.get(f"{LEARNER_URL}/files{test_path}")
        assert learner_response.status_code == 200, f"Falha ao ler recurso do Learner: {learner_response.text}"
        learner_data = learner_response.json()
        assert learner_data["success"] == True, f"Learner reporta falha: {learner_data}"
        assert learner_data["content"] == test_content, "Conteúdo do recurso não corresponde ao esperado"
        return learner_data
    
    learner_data = read_from_learner()
    logger.info(f"Leitura via Learner bem-sucedida: {learner_data}")
    
    # Realizar leituras sequenciais para verificar se o conteúdo é consistente
    # independentemente do nó escolhido
    consistent_reads = []
    for i in range(5):  # Realizar 5 leituras
        learner_response = requests.get(f"{LEARNER_URL}/files{test_path}")
        assert learner_response.status_code == 200, f"Falha na leitura {i+1}: {learner_response.text}"
        read_data = learner_response.json()
        consistent_reads.append(read_data)
        logger.info(f"Leitura {i+1}: sucesso={read_data['success']}")
    
    # Verificar se todas as leituras retornaram o mesmo conteúdo
    for i, read in enumerate(consistent_reads):
        assert read["success"] == True, f"Leitura {i+1} falhou"
        assert read["content"] == test_content, f"Conteúdo inconsistente na leitura {i+1}"
    
    logger.info("Todas as leituras foram consistentes")

@pytest.mark.asyncio
async def test_version_consistency(docker_services_available):
    """
    Testar consistência de versões após múltiplas modificações.
    """
    # Aguardar sistema inicializar completamente
    wait_for_system_ready()
    
    # Criar um recurso para teste
    unique_id = uuid.uuid4().hex
    test_path = f"/version_test_{unique_id}"
    initial_content = f"Initial version {unique_id}"
    
    create_data = {
        "operation": "CREATE",
        "path": test_path,
        "content": initial_content
    }
    
    # Criar o recurso
    logger.info(f"Criando recurso para teste de versões: {create_data}")
    response = requests.post(f"{PROPOSER_URL}/propose", json=create_data)
    assert response.status_code == 200, f"Falha ao criar recurso: {response.text}"
    
    # Aguardar replicação
    time.sleep(3)
    
    # Realizar uma série de modificações
    modifications = 3
    expected_final_version = 1 + modifications  # Versão inicial (1) + número de modificações
    
    for i in range(modifications):
        modify_data = {
            "operation": "MODIFY",
            "path": test_path,
            "content": f"Modified content {i+1} - {unique_id}"
        }
        
        logger.info(f"Enviando modificação {i+1}: {modify_data}")
        response = requests.post(f"{PROPOSER_URL}/propose", json=modify_data)
        assert response.status_code == 200, f"Falha ao modificar (iteração {i+1}): {response.text}"
        
        # Pequena pausa entre modificações
        time.sleep(2)
    
    # Aguardar última modificação replicar
    time.sleep(3)
    
    # Verificar se todos os Stores têm a mesma versão final
    @retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
    def verify_store_versions():
        store_versions = []
        
        # Verificar em cada Store
        for store_url in [STORE_1_URL, STORE_2_URL, STORE_3_URL]:
            store_response = requests.get(f"{store_url}/resources{test_path}")
            assert store_response.status_code == 200, f"Falha ao ler recurso: {store_response.text}"
            store_data = store_response.json()
            assert store_data["success"] == True, f"Store reporta falha: {store_data}"
            store_versions.append(store_data["version"])
        
        # Todas as versões devem ser iguais
        assert len(set(store_versions)) == 1, f"Versões inconsistentes: {store_versions}"
        
        # A versão deve corresponder ao número esperado
        assert store_versions[0] == expected_final_version, \
            f"Versão final {store_versions[0]} difere da esperada {expected_final_version}"
        
        return store_versions[0]
    
    final_version = verify_store_versions()
    logger.info(f"Versão final consistente em todos os Stores: {final_version}")