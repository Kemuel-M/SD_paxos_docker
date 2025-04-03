"""
Configuração de logs estruturados em formato JSON para todos os componentes.
"""
import json
import logging
import time
from typing import Dict, Any, Optional


class JSONLogFormatter(logging.Formatter):
    """
    Formatador de logs em formato JSON conforme especificação do projeto.
    """
    def __init__(self, component: str, node_id: int):
        super().__init__()
        self.component = component
        self.node_id = node_id

    def format(self, record: logging.LogRecord) -> str:
        """
        Formata o registro de log em JSON.
        """
        log_object = {
            "timestamp": int(time.time() * 1000),  # Unix timestamp em milissegundos
            "level": record.levelname,
            "component": self.component,
            "node_id": self.node_id,
            "message": record.getMessage(),
            "context": {}
        }

        # Adiciona atributos extras do LogRecord ao contexto
        for key, value in record.__dict__.items():
            if key not in ["timestamp", "level", "component", "node_id", "message", "context"] and not key.startswith("_"):
                if key == "exc_info" and value:
                    log_object["context"]["exception"] = self.formatException(value)
                elif key == "stack_info" and value:
                    log_object["context"]["stack"] = self.formatStack(value)
                elif isinstance(value, (str, int, float, bool, type(None))):
                    log_object["context"][key] = value

        return json.dumps(log_object)


def configure_logger(component: str, node_id: int, level: int = logging.INFO) -> logging.Logger:
    """
    Configura e retorna um logger com formatação JSON.
    
    Args:
        component: Nome do componente (proposer, acceptor, learner, store).
        node_id: ID do nó.
        level: Nível de log (padrão: INFO).
        
    Returns:
        Logger configurado.
    """
    logger = logging.getLogger(f"{component}-{node_id}")
    logger.setLevel(level)
    
    # Remover handlers existentes
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Configurar handler para saída no console
    handler = logging.StreamHandler()
    formatter = JSONLogFormatter(component, node_id)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger


def log_with_context(logger: logging.Logger, level: int, message: str, context: Optional[Dict[str, Any]] = None):
    """
    Registra uma mensagem de log com contexto adicional.
    
    Args:
        logger: Logger a ser utilizado.
        level: Nível de log (logging.INFO, logging.ERROR, etc).
        message: Mensagem de log.
        context: Dados de contexto adicionais.
    """
    if context is None:
        logger.log(level, message)
    else:
        extra = {"context": context}
        logger.log(level, message, extra=extra)