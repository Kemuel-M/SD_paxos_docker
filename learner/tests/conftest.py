"""
File: learner/tests/conftest.py
Configuration for pytest integration tests.
"""
import os
import pytest

def pytest_addoption(parser):
    """Add command line options to pytest."""
    parser.addoption(
        "--use-cluster-store",
        action="store_true",
        default=False,
        help="Run tests with Cluster Store enabled",
    )
    
    # Adicionar opção para sobrescrever através de opção -o
    parser.addini(
        "use_cluster_store", 
        type="string",
        help="Run tests with Cluster Store enabled (true/false)",
        default="false"
    )

def pytest_configure(config):
    """Configure pytest with command line options and set values to be accessible in tests."""
    # Verificar se a opção --use-cluster-store foi passada na linha de comando
    cmd_line_value = config.getoption("--use-cluster-store")
    
    # Verificar se a opção -o use_cluster_store=true foi passada
    ini_value = config.getini("use_cluster_store")
    
    # Combinar valores, priorizando linha de comando, depois -o, depois variável de ambiente
    use_cluster_store = "false"
    if cmd_line_value:
        use_cluster_store = "true"
    elif ini_value.lower() == "true":
        use_cluster_store = "true"
    elif os.environ.get("USE_CLUSTER_STORE", "").lower() == "true":
        use_cluster_store = "true"
    
    # Configurar para uso nos testes
    pytest.use_cluster_store = use_cluster_store
    
    # Imprimir configuração para referência
    print(f"\nConfiguring tests with USE_CLUSTER_STORE={use_cluster_store}\n")
    
    # Definir a variável de ambiente para consistência
    os.environ["USE_CLUSTER_STORE"] = use_cluster_store