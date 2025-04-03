"""
Testes de integração para o processo de eleição de líder entre Proposers.
"""
import pytest
import requests
import time
import json
import logging
import subprocess
import uuid
from tenacity import retry, stop_after_attempt, wait_fixed, wait_exponential

# Configurar logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# URLs dos componentes para testes
PROPOSER_URL = "http://localhost:8091"
LEARNER_URL = "http://localhost:8092"

# Lista de containers para manipulação
PROPOSER_CONTAINER = "tests_proposer-test_1"

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
    return True

def is_responsive(url):
    """Verificar se o serviço está respondendo."""
    try:
        response = requests.get(f"{url}/status", timeout=2)
        return response.status_code == 200
    except requests.exceptions.ConnectionError:
        return False

@retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
def wait_for_proposer_ready():
    """Esperar o proposer estar pronto."""
    try:
        response = requests.get(f"{PROPOSER_URL}/status", timeout=2)
        if response.status_code != 200:
            return False
        return True
    except requests.exceptions.RequestException:
        return False

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

def send_heartbeat_to_proposer(leader_id, term, timestamp):
    """Enviar heartbeat simulado para o proposer."""
    heartbeat_data = {
        "leader_id": leader_id,
        "timestamp": timestamp,
        "proposal_number": term
    }
    
    response = requests.post(f"{PROPOSER_URL}/heartbeat", json=heartbeat_data)
    return response.status_code == 200

@pytest.mark.asyncio
async def test_proposer_is_leader(docker_services_available):
    """
    Verificar se o proposer se tornou líder corretamente.
    
    No ambiente de teste, temos apenas um proposer, então ele deve se eleger líder.
    """
    # Aguardar proposer inicializar completamente
    wait_for_proposer_ready()
    
    # Verificar status do proposer
    logger.info("Verificando status do proposer")
    
    @retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
    def verify_leader_status():
        response = requests.get(f"{PROPOSER_URL}/status")
        assert response.status_code == 200, f"Falha ao obter status: {response.text}"
        
        status_data = response.json()
        logger.info(f"Status do proposer: {status_data}")
        
        # Verificar se é líder
        assert status_data.get("is_leader") == True, "Proposer não é líder como esperado"
        assert status_data.get("leader_id") == status_data.get("node_id"), "O ID do líder deve ser o ID do próprio proposer"
        
        return status_data
    
    status = verify_leader_status()
    logger.info(f"Proposer confirmado como líder: {status}")

@pytest.mark.asyncio
async def test_heartbeat_mechanism(docker_services_available):
    """
    Testar o mecanismo de heartbeat do líder.
    """
    # Aguardar proposer inicializar completamente
    wait_for_proposer_ready()
    
    # Primeiro, verificar se o proposer é líder
    response1 = requests.get(f"{PROPOSER_URL}/status")
    assert response1.status_code == 200, f"Falha ao obter status inicial: {response1.text}"
    
    initial_status = response1.json()
    assert initial_status.get("is_leader") == True, "Proposer deve ser líder para este teste"
    
    # Aguardar alguns segundos para os heartbeats ocorrerem
    logger.info("Aguardando heartbeats do líder...")
    time.sleep(5)
    
    # Verificar status novamente
    response2 = requests.get(f"{PROPOSER_URL}/status")
    assert response2.status_code == 200, f"Falha ao obter status final: {response2.text}"
    
    final_status = response2.json()
    logger.info(f"Status final após heartbeats: {final_status}")
    
    # O proposer deve continuar sendo líder
    assert final_status.get("is_leader") == True, "Proposer deve permanecer como líder"
    
    # A instância deve ter avançado (devido a atividade do sistema)
    assert final_status.get("instance_counter", 0) >= initial_status.get("instance_counter", 0), \
        "O contador de instância deve avançar ou permanecer igual"

@pytest.mark.asyncio
async def test_proposal_processing(docker_services_available):
    """
    Testar processamento de propostas pelo líder.
    """
    # Aguardar proposer inicializar completamente
    wait_for_proposer_ready()
    
    # Verificar status inicial
    response_initial = requests.get(f"{PROPOSER_URL}/status")
    assert response_initial.status_code == 200, f"Falha ao obter status inicial: {response_initial.text}"
    
    initial_status = response_initial.json()
    initial_instance = initial_status.get("instance_counter", 0)
    logger.info(f"Estado inicial: contador de instância = {initial_instance}")
    
    # Enviar proposta
    test_data = {
        "operation": "CREATE",
        "path": f"/leader_test_{int(time.time())}",
        "content": "Test leader processing"
    }
    
    logger.info(f"Enviando proposta para líder: {test_data}")
    proposal_response = requests.post(f"{PROPOSER_URL}/propose", json=test_data)
    assert proposal_response.status_code == 200, f"Falha ao propor: {proposal_response.text}"
    
    proposal_data = proposal_response.json()
    logger.info(f"Resposta da proposta: {proposal_data}")
    assert proposal_data.get("success") == True, "A proposta deve ser aceita pelo líder"
    
    # Aguardar processamento
    time.sleep(3)
    
    # Verificar se o contador de instâncias avançou
    @retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
    def verify_instance_counter_advanced():
        response_final = requests.get(f"{PROPOSER_URL}/status")
        assert response_final.status_code == 200, f"Falha ao obter status final: {response_final.text}"
        
        final_status = response_final.json()
        final_instance = final_status.get("instance_counter", 0)
        logger.info(f"Estado final: contador de instância = {final_instance}")
        
        # O contador de instâncias deve ter avançado
        assert final_instance > initial_instance, "O contador de instância deve avançar após processamento da proposta"
        
        return final_status
    
    final_status = verify_instance_counter_advanced()
    logger.info(f"Contador de instância avançou conforme esperado: {final_status.get('instance_counter')}")

@pytest.mark.asyncio
async def test_leader_election_after_failure(docker_services_available):
    """
    Testar o processo de eleição de líder após falha do líder atual.
    Simularemos a falha parando e reiniciando o container do Proposer.
    """
    # Aguardar proposer inicializar completamente
    wait_for_proposer_ready()
    
    # Verificar status inicial e confirmar que é líder
    response_initial = requests.get(f"{PROPOSER_URL}/status")
    assert response_initial.status_code == 200, f"Falha ao obter status inicial: {response_initial.text}"
    
    initial_status = response_initial.json()
    assert initial_status.get("is_leader") == True, "Proposer deve ser líder inicialmente"
    logger.info(f"Status inicial como líder: {initial_status}")
    
    # Parar o container para simular falha
    assert stop_container(PROPOSER_CONTAINER), "Falha ao parar container do proposer"
    
    # Aguardar um momento
    logger.info("Aguardando após parada do proposer...")
    time.sleep(5)
    
    # Reiniciar o container
    assert start_container(PROPOSER_CONTAINER), "Falha ao reiniciar container do proposer"
    
    # Aguardar o proposer se recuperar e iniciar nova eleição
    logger.info("Aguardando proposer se recuperar e realizar nova eleição...")
    time.sleep(15)
    
    # Verificar se o proposer se elegeu líder novamente
    @retry(stop=stop_after_attempt(8), wait=wait_exponential(multiplier=1, min=2, max=10))
    def verify_reelection():
        response = requests.get(f"{PROPOSER_URL}/status")
        assert response.status_code == 200, f"Falha ao obter status após reinício: {response.text}"
        
        status_data = response.json()
        logger.info(f"Status após reinício: {status_data}")
        
        # Verificar se voltou a ser líder
        assert status_data.get("is_leader") == True, "Proposer deve se reeleger líder após reinício"
        
        # Verificar se o proposal counter aumentou (sinal de nova eleição)
        assert status_data.get("proposal_counter", 0) > initial_status.get("proposal_counter", 0), \
            "Contador de propostas deve aumentar após nova eleição"
        
        return status_data
    
    reelection_status = verify_reelection()
    logger.info(f"Proposer reeleito líder após falha: {reelection_status}")
    
    # Enviar uma proposta para confirmar que o sistema está funcional
    test_data = {
        "operation": "CREATE",
        "path": f"/leader_recovery_test_{uuid.uuid4().hex}",
        "content": "Test leader recovery after failure"
    }
    
    logger.info(f"Enviando proposta após recuperação: {test_data}")
    recovery_response = requests.post(f"{PROPOSER_URL}/propose", json=test_data)
    assert recovery_response.status_code == 200, f"Falha ao propor após recuperação: {recovery_response.text}"
    
    recovery_data = recovery_response.json()
    assert recovery_data.get("success") == True, "A proposta deve ser aceita pelo líder recuperado"
    logger.info(f"Proposta aceita pelo líder recuperado: {recovery_data}")

@pytest.mark.asyncio
async def test_leader_recognition(docker_services_available):
    """
    Testar se um proposer reconhece outro líder quando recebe heartbeats.
    Simularemos um líder com ID maior enviando heartbeats.
    """
    # Aguardar proposer inicializar completamente
    wait_for_proposer_ready()
    
    # Verificar status inicial
    response_initial = requests.get(f"{PROPOSER_URL}/status")
    assert response_initial.status_code == 200, f"Falha ao obter status inicial: {response_initial.text}"
    
    initial_status = response_initial.json()
    logger.info(f"Status inicial: {initial_status}")
    
    # Obter ID e term atual
    node_id = initial_status.get("node_id")
    proposal_counter = initial_status.get("proposal_counter", 0)
    
    # Criar um ID de líder fictício maior que o atual
    fake_leader_id = node_id + 1
    fake_term = (proposal_counter + 1000) << 8 | fake_leader_id  # Número de proposta muito maior
    
    # Enviar heartbeat simulando um líder com ID maior
    logger.info(f"Enviando heartbeat de líder fictício com ID {fake_leader_id} e term {fake_term}")
    success = send_heartbeat_to_proposer(fake_leader_id, fake_term, time.time())
    assert success, "Falha ao enviar heartbeat"
    
    # Verificar se o proposer reconheceu o novo líder
    @retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
    def verify_leader_recognition():
        response = requests.get(f"{PROPOSER_URL}/status")
        assert response.status_code == 200, f"Falha ao obter status: {response.text}"
        
        status_data = response.json()
        logger.info(f"Status após heartbeat: {status_data}")
        
        # Deve ter reconhecido o novo líder
        assert status_data.get("is_leader") == False, "Proposer não deve mais se considerar líder"
        assert status_data.get("leader_id") == fake_leader_id, f"Proposer deve reconhecer líder {fake_leader_id}"
        
        # O contador de propostas deve ter sido atualizado
        assert status_data.get("proposal_counter", 0) >= proposal_counter, \
            "Contador de propostas deve ter sido atualizado"
        
        return status_data
    
    recognition_status = verify_leader_recognition()
    logger.info(f"Proposer reconheceu o novo líder: {recognition_status}")
    
    # Aguardar timeout do heartbeat falso para o proposer iniciar nova eleição
    logger.info("Aguardando timeout do heartbeat falso...")
    time.sleep(10)
    
    # Verificar se o proposer iniciou nova eleição e se tornou líder novamente
    @retry(stop=stop_after_attempt(8), wait=wait_exponential(multiplier=1, min=2, max=10))
    def verify_reelection_after_timeout():
        response = requests.get(f"{PROPOSER_URL}/status")
        assert response.status_code == 200, f"Falha ao obter status: {response.text}"
        
        status_data = response.json()
        logger.info(f"Status após timeout: {status_data}")
        
        # Deve ter voltado a ser líder
        assert status_data.get("is_leader") == True, "Proposer deve ter voltado a ser líder após timeout"
        
        return status_data
    
    final_status = verify_reelection_after_timeout()
    logger.info(f"Proposer voltou a ser líder após timeout: {final_status}")

@pytest.mark.asyncio
async def test_multi_paxos_optimization(docker_services_available):
    """
    Testar a otimização de Multi-Paxos onde o líder executa prepare estendido
    para múltiplas instâncias futuras.
    """
    # Aguardar proposer inicializar completamente
    wait_for_proposer_ready()
    
    # Verificar status inicial
    response_initial = requests.get(f"{PROPOSER_URL}/status")
    assert response_initial.status_code == 200, f"Falha ao obter status inicial: {response_initial.text}"
    
    initial_status = response_initial.json()
    assert initial_status.get("is_leader") == True, "Proposer deve ser líder para este teste"
    
    # Coletar estado inicial
    initial_instance = initial_status.get("instance_counter", 0)
    initial_prepared = initial_status.get("prepared_instances", [])
    logger.info(f"Estado inicial: contador={initial_instance}, prepared_instances={initial_prepared}")
    
    # Enviar múltiplas propostas em sequência para testar otimização
    for i in range(5):
        test_data = {
            "operation": "CREATE",
            "path": f"/multi_paxos_test_{i}_{int(time.time())}",
            "content": f"Testing Multi-Paxos optimization - batch {i}"
        }
        
        logger.info(f"Enviando proposta {i+1}: {test_data}")
        response = requests.post(f"{PROPOSER_URL}/propose", json=test_data)
        assert response.status_code == 200, f"Falha ao propor: {response.text}"
        
        # Pequena pausa entre propostas para não sobrecarregar
        time.sleep(0.5)
    
    # Aguardar processamento
    logger.info("Aguardando processamento de propostas...")
    time.sleep(5)
    
    # Verificar estado final
    response_final = requests.get(f"{PROPOSER_URL}/status")
    assert response_final.status_code == 200, f"Falha ao obter status final: {response_final.text}"
    
    final_status = response_final.json()
    final_instance = final_status.get("instance_counter", 0)
    final_prepared = final_status.get("prepared_instances", [])
    
    logger.info(f"Estado final: contador={final_instance}, prepared_instances={final_prepared}")
    
    # Verificações
    assert final_instance > initial_instance, "Contador de instâncias deve avançar"
    assert final_instance >= initial_instance + 5, "Contador deve avançar pelo menos 5 (número de propostas)"
    
    # Verificar se o sistema está funcional através do Learner
    learner_response = requests.get(f"{LEARNER_URL}/status")
    assert learner_response.status_code == 200, f"Falha ao obter status do learner: {learner_response.text}"
    
    learner_status = learner_response.json()
    logger.info(f"Status do Learner: {learner_status}")
    
    # O Learner deve ter aplicado decisões
    assert learner_status.get("last_applied_instance", 0) > 0, "Learner deve ter aplicado decisões"
    
    logger.info("Teste da otimização Multi-Paxos concluído com sucesso")