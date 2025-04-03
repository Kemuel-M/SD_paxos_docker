"""
Sistema de logging unificado para todos os componentes.
"""
import os
import sys
import json
import time
import logging
import datetime
from typing import Dict, Any, List, Optional
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from collections import deque

# Configuração
DEBUG = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")
LOG_DIR = os.getenv("LOG_DIR", "/data/logs")

# Níveis de log
LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "IMPORTANT": 25,  # Nível customizado entre INFO e WARNING
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL
}

# Registra o nível IMPORTANT
logging.addLevelName(LEVELS["IMPORTANT"], "IMPORTANT")

# Buffer circular para logs em memória
log_buffer = {}  # component -> deque(log entries)
log_buffer_size = 1000  # Tamanho máximo do buffer por componente

# Timestamp de início para cálculo de uptime
start_time = time.time()

def setup_logging(component_name: str, debug: bool = None, log_dir: str = None):
    """
    Configura o sistema de logging para um componente.
    
    Args:
        component_name: Nome do componente
        debug: Se True, habilita logs de DEBUG (sobrescreve variável de ambiente)
        log_dir: Diretório para salvar logs (sobrescreve variável de ambiente)
    """
    # Usa valores de parâmetros ou fallback para variáveis de ambiente
    debug_enabled = debug if debug is not None else DEBUG
    logs_directory = log_dir if log_dir is not None else LOG_DIR
    
    # Cria diretório de logs se não existir
    os.makedirs(logs_directory, exist_ok=True)
    
    # Configura logger raiz
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG if debug_enabled else logging.INFO)
    
    # Remove handlers existentes
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Adiciona handler para console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG if debug_enabled else logging.INFO)
    console_handler.setFormatter(JsonFormatter(component_name))
    root_logger.addHandler(console_handler)
    
    # Adiciona handler para arquivo completo (todos os logs)
    all_log_file = os.path.join(logs_directory, f"{component_name}_all.log")
    file_handler = RotatingFileHandler(
        all_log_file,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5
    )
    file_handler.setLevel(logging.DEBUG if debug_enabled else logging.INFO)
    file_handler.setFormatter(JsonFormatter(component_name))
    root_logger.addHandler(file_handler)
    
    # Adiciona handler para arquivo de logs importantes (IMPORTANT e acima)
    important_log_file = os.path.join(logs_directory, f"{component_name}_important.log")
    important_handler = TimedRotatingFileHandler(
        important_log_file,
        when="midnight",
        interval=1,
        backupCount=7
    )
    important_handler.setLevel(LEVELS["IMPORTANT"])
    important_handler.setFormatter(JsonFormatter(component_name))
    root_logger.addHandler(important_handler)
    
    # Inicializa buffer para este componente
    log_buffer[component_name] = deque(maxlen=log_buffer_size)
    
    # Registra método para logger
    def important(self, message, *args, **kwargs):
        if self.isEnabledFor(LEVELS["IMPORTANT"]):
            self._log(LEVELS["IMPORTANT"], message, args, **kwargs)
    
    logging.Logger.important = important
    
    # Adiciona callback para interceptar todos os logs
    old_factory = logging.getLogRecordFactory()
    
    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        
        # Adiciona ao buffer se for do componente correto
        if getattr(record, "name", "").startswith(component_name):
            add_to_buffer(component_name, record)
            
        return record
    
    logging.setLogRecordFactory(record_factory)
    
    logger = logging.getLogger(component_name)
    logger.info(f"Logging inicializado para {component_name}. Debug: {debug_enabled}")
    
    return logger

def add_to_buffer(component: str, record: logging.LogRecord):
    """
    Adiciona um registro de log ao buffer circular.
    
    Args:
        component: Nome do componente
        record: Registro de log
    """
    # Cria buffer para componente se não existir
    if component not in log_buffer:
        log_buffer[component] = deque(maxlen=log_buffer_size)
    
    # Converte record para dicionário
    log_entry = {
        "timestamp": int(record.created * 1000),  # milissegundos
        "level": record.levelname,
        "component": component,
        "node_id": getattr(record, "node_id", None),
        "message": record.getMessage(),
        "module": record.module,
        "lineno": record.lineno,
        "context": getattr(record, "context", None)
    }
    
    # Adiciona ao buffer
    log_buffer[component].append(log_entry)

def get_log_entries(component: str, limit: int = 100) -> List[Dict[str, Any]]:
    """
    Obtém registros de log do buffer.
    
    Args:
        component: Nome do componente
        limit: Número máximo de registros a retornar
    
    Returns:
        List[Dict[str, Any]]: Lista de registros de log
    """
    if component not in log_buffer:
        return []
    
    # Obtém últimos registros (mais recentes primeiro)
    entries = list(log_buffer[component])
    entries.reverse()
    
    return entries[:limit]

def get_important_log_entries(component: str, limit: int = 100) -> List[Dict[str, Any]]:
    """
    Obtém registros de log importantes do buffer.
    
    Args:
        component: Nome do componente
        limit: Número máximo de registros a retornar
    
    Returns:
        List[Dict[str, Any]]: Lista de registros de log importantes
    """
    if component not in log_buffer:
        return []
    
    # Filtra logs importantes (IMPORTANT e acima)
    important_levels = ["IMPORTANT", "WARNING", "ERROR", "CRITICAL"]
    entries = [e for e in log_buffer[component] if e["level"] in important_levels]
    entries.reverse()
    
    return entries[:limit]

def get_uptime() -> float:
    """
    Retorna o tempo de execução em segundos.
    
    Returns:
        float: Tempo de execução em segundos
    """
    return time.time() - start_time

class JsonFormatter(logging.Formatter):
    """
    Formatador que converte logs para formato JSON.
    """
    
    def __init__(self, component: str):
        """
        Inicializa o formatador.
        
        Args:
            component: Nome do componente
        """
        super().__init__()
        self.component = component
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Formata um registro de log como JSON.
        
        Args:
            record: Registro de log
        
        Returns:
            str: JSON formatado
        """
        # Informações básicas
        log_data = {
            "timestamp": int(record.created * 1000),  # milissegundos
            "datetime": datetime.datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "component": self.component,
            "node_id": getattr(record, "node_id", None),
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "lineno": record.lineno
        }
        
        # Adiciona contexto se disponível
        context = getattr(record, "context", None)
        if context:
            log_data["context"] = context
        
        # Adiciona informações de exceção se disponível
        if record.exc_info:
            log_data["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": self.formatException(record.exc_info)
            }
        
        return json.dumps(log_data)