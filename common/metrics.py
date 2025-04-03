"""
Configuração de métricas Prometheus para todos os componentes.
"""
from prometheus_client import Counter, Histogram, Gauge
from functools import wraps
import time
from typing import Callable, Any


# Métricas para o Proposer
proposer_metrics = {
    "proposals_total": Counter(
        "paxos_proposals_total", 
        "Número total de propostas iniciadas",
        ["node_id"]
    ),
    "proposals_success": Counter(
        "paxos_proposals_success", 
        "Número de propostas bem-sucedidas",
        ["node_id"]
    ),
    "proposals_failure": Counter(
        "paxos_proposals_failure", 
        "Número de propostas que falharam",
        ["node_id"]
    ),
    "prepare_phase_duration": Histogram(
        "paxos_prepare_phase_duration_seconds", 
        "Duração da fase prepare",
        ["node_id"]
    ),
    "accept_phase_duration": Histogram(
        "paxos_accept_phase_duration_seconds", 
        "Duração da fase accept",
        ["node_id"]
    ),
    "leadership_changes": Counter(
        "paxos_leadership_changes_total", 
        "Número de mudanças de liderança",
        ["node_id"]
    )
}


# Métricas para o Acceptor
acceptor_metrics = {
    "prepare_received": Counter(
        "paxos_prepare_received_total", 
        "Número total de mensagens prepare recebidas",
        ["node_id"]
    ),
    "accept_received": Counter(
        "paxos_accept_received_total", 
        "Número total de mensagens accept recebidas",
        ["node_id"]
    ),
    "promise_sent": Counter(
        "paxos_promise_sent_total", 
        "Número de respostas promise enviadas",
        ["node_id"]
    ),
    "not_promise_sent": Counter(
        "paxos_not_promise_sent_total", 
        "Número de respostas not promise enviadas",
        ["node_id"]
    ),
    "accepted_sent": Counter(
        "paxos_accepted_sent_total", 
        "Número de respostas accepted enviadas",
        ["node_id"]
    ),
    "not_accepted_sent": Counter(
        "paxos_not_accepted_sent_total", 
        "Número de respostas not accepted enviadas",
        ["node_id"]
    )
}


# Métricas para o Learner
learner_metrics = {
    "learn_messages": Counter(
        "paxos_learn_messages_total", 
        "Número total de mensagens learn recebidas",
        ["node_id"]
    ),
    "consensus_reached": Counter(
        "paxos_consensus_reached_total", 
        "Número de consensos alcançados",
        ["node_id"]
    ),
    "client_notifications": Counter(
        "paxos_client_notifications_total", 
        "Número de notificações enviadas aos clientes",
        ["node_id", "status"]
    ),
    "store_operations": Counter(
        "paxos_store_operations_total", 
        "Número de operações no Cluster Store",
        ["node_id", "operation", "status"]
    )
}


# Métricas para o Cluster Store
store_metrics = {
    "reads": Counter(
        "paxos_store_reads_total", 
        "Número total de operações de leitura",
        ["node_id"]
    ),
    "writes": Counter(
        "paxos_store_writes_total", 
        "Número total de operações de escrita",
        ["node_id"]
    ),
    "prepare": Counter(
        "paxos_store_prepare_total", 
        "Número de mensagens prepare do 2PC recebidas",
        ["node_id", "status"]
    ),
    "commit": Counter(
        "paxos_store_commit_total", 
        "Número de mensagens commit do 2PC recebidas",
        ["node_id", "status"]
    ),
    "abort": Counter(
        "paxos_store_abort_total", 
        "Número de mensagens abort do 2PC recebidas",
        ["node_id"]
    ),
    "sync": Counter(
        "paxos_store_sync_total", 
        "Número de operações de sincronização",
        ["node_id", "result"]
    ),
    "resource_version": Gauge(
        "paxos_store_resource_version", 
        "Versão atual do recurso",
        ["node_id", "resource"]
    )
}


# Métricas para o Cliente
client_metrics = {
    "requests": Counter(
        "paxos_client_requests_total", 
        "Número total de requisições feitas",
        ["client_id"]
    ),
    "committed": Counter(
        "paxos_client_committed_total", 
        "Número de respostas COMMITTED recebidas",
        ["client_id"]
    ),
    "wait_time": Histogram(
        "paxos_client_wait_time_seconds", 
        "Tempo de espera entre requisições",
        ["client_id"]
    )
}


def time_and_count(metric_histogram: Histogram, metric_counter: Counter, labels: dict = None) -> Callable:
    """
    Decorador para medir o tempo de execução de uma função e incrementar um contador.
    
    Args:
        metric_histogram: Métrica do tipo Histogram para registrar tempo.
        metric_counter: Métrica do tipo Counter para incrementar.
        labels: Rótulos para as métricas.
        
    Returns:
        Decorador configurado.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                if labels:
                    metric_counter.labels(**labels).inc()
                else:
                    metric_counter.inc()
                return result
            finally:
                duration = time.time() - start_time
                if labels:
                    metric_histogram.labels(**labels).observe(duration)
                else:
                    metric_histogram.observe(duration)
        return wrapper
    return decorator