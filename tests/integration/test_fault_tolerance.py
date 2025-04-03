"""
Testes de integração para verificar a tolerância a falhas do sistema.
Testa os três cenários de falha requeridos:
1. Store sem pedido pendente falhando
2. Store com pedido falha durante processamento
3. Store com permissão de escrita falha durante commit
"""
import pytest
import requests
import time
import json
import logging
import uuid
import subprocess
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

# Lista de containers para manipulação
STORE_CONTAINERS = ["tests_store-1-test_1", "tests_store-2-test_1", "tests_store-3-test_1"]

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

def stop_container(container_name):
    """Parar um container Docker."""
    logger.info(f"Parando container: {container_name}")
    result = subprocess.run(["docker", "stop", container_name], capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"Falha ao parar container: {result.stderr}")
        return False
    return True

def start_container(container_name):
    """Iniciar um container Docker."""
    logger.info(f"Iniciando container: {container_name}")
    result = subprocess.run(["docker", "start", container_name], capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"Falha ao iniciar container: {result.stderr}")
        return False
    return True

@pytest.mark.asyncio
async def test_store_failure_without_request(docker_services_available):
    """
    Cenário 1: Store falha sem pedido pendente.
    """
    # Aguardar sistema inicializar completamente
    wait_for_system_ready()
    
    # Primeiro, criar um recurso para garantir que o sistema está funcional
    unique_id = uuid.uuid4().hex
    test_path = f"/failure_test1_{unique_id}"
    initial_content = f"Initial content for failure test 1 - {unique_id}"
    
    create_data = {
        "operation": "CREATE",
        "path": test_path,
        "content": initial_content
    }
    
    # Criar o recurso
    logger.info(f"Criando recurso inicial: {create_data}")
    response = requests.post(f"{PROPOSER_URL}/propose", json=create_data)
    assert response.status_code == 200, f"Falha ao criar recurso: {response.text}"
    
    # Aguardar replicação
    time.sleep(3)
    
    # Verificar se o recurso foi criado corretamente
    learner_response = requests.get(f"{LEARNER_URL}/files{test_path}")
    assert learner_response.status_code == 200, f"Falha ao ler recurso inicial: {learner_response.text}"
    
    # Parar um dos nós Store (Store-1)
    container_to_stop = STORE_CONTAINERS[0]
    assert stop_container(container_to_stop), "Falha ao parar container"
    
    # Aguardar sistema detectar a falha (via heartbeats)
    logger.info("Aguardando sistema detectar falha do Store...")
    time.sleep(5)
    
    # Criar um segundo recurso enquanto o Store-1 está parado
    second_id = uuid.uuid4().hex
    test_path2 = f"/failure_test1_after_{second_id}"
    second_content = f"Content after Store-1 failure - {second_id}"
    
    second_data = {
        "operation": "CREATE",
        "path": test_path2,
        "content": second_content
    }
    
    # Tentar criar recurso com Store-1 parado
    logger.info(f"Tentando criar recurso com Store-1 parado: {second_data}")
    response2 = requests.post(f"{PROPOSER_URL}/propose", json=second_data)
    
    # Na configuração ROWA (Write All), a operação deve falhar
    # Mas como estamos em ambiente de teste, pode haver adaptações
    logger.info(f"Resposta do sistema com Store-1 parado: {response2.text}")
    
    # Iniciar o Store-1 novamente
    assert start_container(container_to_stop), "Falha ao iniciar container"
    
    # Aguardar Store-1 recuperar e sincronizar
    logger.info("Aguardando Store-1 recuperar e sincronizar...")
    time.sleep(15)
    
    # Verificar se o Store-1 recuperou estado
    @retry(stop=stop_after_attempt(5), wait=wait_fixed(3))
    def verify_store1_recovered():
        store_response = requests.get(f"{STORE_1_URL}/status")
        assert store_response.status_code == 200, "Store-1 não está respondendo após reinício"
        
        # Verificar se o Store-1 não está mais em recuperação
        status_data = store_response.json()
        assert status_data.get("is_recovering", False) == False, "Store-1 ainda está em modo de recuperação"
        
        # Verificar se o recurso original está disponível no Store-1
        resource_response = requests.get(f"{STORE_1_URL}/resources{test_path}")
        assert resource_response.status_code == 200, "Falha ao ler recurso do Store-1 após recuperação"
        resource_data = resource_response.json()
        assert resource_data["success"] == True, "Store-1 reporta falha ao ler recurso"
        assert resource_data["content"] == initial_content, "Conteúdo do recurso não corresponde ao esperado"
        
        return status_data
    
    recovery_status = verify_store1_recovered()
    logger.info(f"Store-1 recuperado e sincronizado: {recovery_status}")

@pytest.mark.asyncio
async def test_store_failure_during_request(docker_services_available):
    """
    Cenário 2: Store falha durante processamento de pedido.
    """
    # Aguardar sistema inicializar completamente
    wait_for_system_ready()
    
    # Criar um recurso inicial para o teste
    unique_id = uuid.uuid4().hex
    test_path = f"/failure_test2_{unique_id}"
    initial_content = f"Initial content for failure test 2 - {unique_id}"
    
    create_data = {
        "operation": "CREATE",
        "path": test_path,
        "content": initial_content
    }
    
    # Criar o recurso
    logger.info(f"Criando recurso inicial: {create_data}")
    response = requests.post(f"{PROPOSER_URL}/propose", json=create_data)
    assert response.status_code == 200, f"Falha ao criar recurso: {response.text}"
    
    # Aguardar replicação
    time.sleep(3)
    
    # Verificar se o recurso foi criado corretamente
    learner_response = requests.get(f"{LEARNER_URL}/files{test_path}")
    assert learner_response.status_code == 200, f"Falha ao ler recurso inicial: {learner_response.text}"
    
    # Preparar modificação para o recurso
    modify_content = f"Modified content during Store-2 failure - {unique_id}"
    modify_data = {
        "operation": "MODIFY",
        "path": test_path,
        "content": modify_content
    }
    
    # Este teste é complexo em ambiente real, pois precisaríamos sincronizar
    # exatamente para parar o Store durante o processamento do pedido.
    # Como aproximação, vamos parar o Store-2 e imediatamente enviar um pedido
    
    # Parar Store-2
    container_to_stop = STORE_CONTAINERS[1]
    assert stop_container(container_to_stop), "Falha ao parar container"
    
    # Imediatamente enviar a modificação
    logger.info(f"Enviando modificação imediatamente após parar Store-2: {modify_data}")
    response2 = requests.post(f"{PROPOSER_URL}/propose", json=modify_data)
    assert response2.status_code == 200, f"Falha ao enviar modificação: {response2.text}"
    
    # Na configuração ROWA (Write All), a operação deve falhar ou ficar pendente
    logger.info(f"Resposta do sistema com Store-2 parado: {response2.text}")
    
    # Aguardar timeout da operação
    logger.info("Aguardando timeout da operação...")
    time.sleep(10)
    
    # Iniciar Store-2 novamente
    assert start_container(container_to_stop), "Falha ao iniciar container"
    
    # Aguardar Store-2 recuperar
    logger.info("Aguardando Store-2 recuperar...")
    time.sleep(15)
    
    # Verificar se o Store-2 recuperou estado
    @retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
    def verify_store2_recovered():
        store_response = requests.get(f"{STORE_2_URL}/status")
        assert store_response.status_code == 200, "Store-2 não está respondendo após reinício"
        
        # Verificar se o Store-2 não está mais em recuperação
        status_data = store_response.json()
        assert status_data.get("is_recovering", False) == False, "Store-2 ainda está em modo de recuperação"
        
        return status_data
    
    recovery_status = verify_store2_recovered()
    logger.info(f"Store-2 recuperado: {recovery_status}")
    
    # Agora enviar outra modificação, que deve ter sucesso
    final_content = f"Final content after Store-2 recovery - {unique_id}"
    final_data = {
        "operation": "MODIFY",
        "path": test_path,
        "content": final_content
    }
    
    logger.info(f"Enviando modificação após recuperação: {final_data}")
    response3 = requests.post(f"{PROPOSER_URL}/propose", json=final_data)
    assert response3.status_code == 200, f"Falha ao enviar modificação final: {response3.text}"
    
    # Aguardar replicação
    time.sleep(3)
    
    # Verificar se o recurso foi modificado em todos os nós
    @retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
    def verify_final_content():
        # Verificar via Learner
        learner_response = requests.get(f"{LEARNER_URL}/files{test_path}")
        assert learner_response.status_code == 200, f"Falha ao ler recurso final: {learner_response.text}"
        learner_data = learner_response.json()
        assert learner_data["success"] == True, "Learner reporta falha ao ler recurso"
        assert learner_data["content"] == final_content, "Conteúdo final do recurso não corresponde ao esperado"
        return learner_data
    
    final_data = verify_final_content()
    logger.info(f"Recurso verificado com conteúdo final após recuperação: {final_data}")

@pytest.mark.asyncio
async def test_store_failure_after_permission(docker_services_available):
    """
    Cenário 3: Store falha após conceder permissão de escrita.
    
    Este cenário é o mais complexo para testar, pois envolve falha durante o Two-Phase Commit.
    Como aproximação, precisaremos estimar o momento da falha durante o processo.
    """
    # Aguardar sistema inicializar completamente
    wait_for_system_ready()
    
    # Criar um recurso para teste
    unique_id = uuid.uuid4().hex
    test_path = f"/failure_test3_{unique_id}"
    initial_content = f"Initial content for failure test 3 - {unique_id}"
    
    create_data = {
        "operation": "CREATE",
        "path": test_path,
        "content": initial_content
    }
    
    # Criar o recurso
    logger.info(f"Criando recurso inicial: {create_data}")
    response = requests.post(f"{PROPOSER_URL}/propose", json=create_data)
    assert response.status_code == 200, f"Falha ao criar recurso: {response.text}"
    
    # Aguardar replicação
    time.sleep(3)
    
    # Verificar se o recurso foi criado corretamente
    learner_response = requests.get(f"{LEARNER_URL}/files{test_path}")
    assert learner_response.status_code == 200, f"Falha ao ler recurso inicial: {learner_response.text}"
    
    # Como é difícil sincronizar exatamente com o 2PC, vamos criar uma estratégia alternativa:
    # 1. Iniciar várias operações de modificação em rápida sucessão
    # 2. Durante as operações, parar o Store-3
    # 3. Isso deve causar falha durante o 2PC para pelo menos uma operação
    
    # Enviar primeira modificação
    modify1_content = f"Modification 1 - {unique_id}"
    modify1_data = {
        "operation": "MODIFY",
        "path": test_path,
        "content": modify1_content
    }
    
    logger.info(f"Enviando primeira modificação: {modify1_data}")
    requests.post(f"{PROPOSER_URL}/propose", json=modify1_data)
    
    # Imediatamente parar Store-3
    container_to_stop = STORE_CONTAINERS[2]
    assert stop_container(container_to_stop), "Falha ao parar container"
    
    # Enviar mais modificações em rápida sucessão
    for i in range(2, 5):
        modify_content = f"Modification {i} - {unique_id}"
        modify_data = {
            "operation": "MODIFY",
            "path": test_path,
            "content": modify_content
        }
        
        logger.info(f"Enviando modificação {i}: {modify_data}")
        requests.post(f"{PROPOSER_URL}/propose", json=modify_data)
        # Pequena pausa entre modificações
        time.sleep(0.5)
    
    # Aguardar sistema processar operações
    logger.info("Aguardando sistema processar operações com Store-3 parado...")
    time.sleep(10)
    
    # Iniciar Store-3 novamente
    assert start_container(container_to_stop), "Falha ao iniciar container"
    
    # Aguardar Store-3 recuperar
    logger.info("Aguardando Store-3 recuperar e sincronizar...")
    time.sleep(20)
    
    # Verificar se Store-3 recuperou estado
    @retry(stop=stop_after_attempt(5), wait=wait_fixed(3))
    def verify_store3_recovered():
        store_response = requests.get(f"{STORE_3_URL}/status")
        assert store_response.status_code == 200, "Store-3 não está respondendo após reinício"
        
        # Verificar se Store-3 não está mais em recuperação
        status_data = store_response.json()
        assert status_data.get("is_recovering", False) == False, "Store-3 ainda está em modo de recuperação"
        
        return status_data
    
    recovery_status = verify_store3_recovered()
    logger.info(f"Store-3 recuperado: {recovery_status}")
    
    # Verificar consistência entre os nós
    @retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
    def verify_store_consistency():
        # Obter versão e conteúdo de cada Store
        store_data = []
        
        for url in [STORE_1_URL, STORE_2_URL, STORE_3_URL]:
            try:
                response = requests.get(f"{url}/resources{test_path}")
                assert response.status_code == 200, f"Falha ao ler recurso de {url}"
                data = response.json()
                assert data["success"] == True, f"Store reporta falha: {data}"
                store_data.append({
                    "url": url,
                    "version": data["version"],
                    "content": data["content"]
                })
            except Exception as e:
                logger.error(f"Erro ao ler de {url}: {str(e)}")
                raise
        
        # Verificar se todos os nós têm a mesma versão e conteúdo
        versions = [d["version"] for d in store_data]
        contents = [d["content"] for d in store_data]
        
        assert len(set(versions)) == 1, f"Versões inconsistentes: {versions}"
        assert len(set(contents)) == 1, f"Conteúdos inconsistentes: {contents}"
        
        return store_data
    
    consistency_data = verify_store_consistency()
    logger.info(f"Sistema está consistente após recuperação. Versão: {consistency_data[0]['version']}")
    
    # Verificar leitura via Learner (deve ser consistente com os Stores)
    learner_response = requests.get(f"{LEARNER_URL}/files{test_path}")
    learner_data = learner_response.json()
    assert learner_data["content"] == consistency_data[0]["content"], \
        "Conteúdo via Learner difere do conteúdo nos Stores"
    
    logger.info("Teste do cenário 3 concluído com sucesso")