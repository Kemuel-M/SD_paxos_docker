#!/usr/bin/env python3
"""
File: scripts/debug_control.py
Ferramenta para controlar o modo de debug de componentes do sistema Paxos.
Permite alternar o modo de debug e o nível de detalhamento em tempo de execução.
"""
import os
import sys
import argparse
import json
import requests
from typing import List, Dict, Any, Optional

# Configurações padrão
DEFAULT_PORT = 8080
COMPONENT_TYPES = ["proposer", "acceptor", "learner", "store", "client"]
DEBUG_LEVELS = ["basic", "advanced", "trace"]

def parse_args():
    """Processa os argumentos da linha de comando."""
    parser = argparse.ArgumentParser(
        description="Controla o modo de debug de componentes do sistema Paxos"
    )
    
    # Argumentos principais
    parser.add_argument("--component", "-c", choices=COMPONENT_TYPES, required=True,
                        help="Tipo do componente a ser configurado")
    parser.add_argument("--node-ids", "-n", type=str, default="all",
                        help="IDs dos nós a configurar (separados por vírgula ou 'all' para todos)")
    parser.add_argument("--action", "-a", choices=["enable", "disable", "status"], required=True,
                        help="Ação a ser executada")
    parser.add_argument("--level", "-l", choices=DEBUG_LEVELS, default="basic",
                        help="Nível de debug quando habilitado (basic, advanced, trace)")
    parser.add_argument("--port", "-p", type=int, default=DEFAULT_PORT,
                        help=f"Porta HTTP (padrão: {DEFAULT_PORT})")
    
    return parser.parse_args()

def get_node_list(component: str, node_ids_arg: str) -> List[int]:
    """
    Determina a lista de nós a serem configurados.
    
    Args:
        component: Tipo do componente
        node_ids_arg: String com IDs separados por vírgula ou 'all'
        
    Returns:
        List[int]: Lista de IDs dos nós
    """
    # Define o número máximo de nós por tipo de componente
    max_nodes = {
        "proposer": 5,
        "acceptor": 5,
        "learner": 2,
        "store": 3,
        "client": 5
    }
    
    # Se for 'all', retorna todos os IDs desse tipo de componente
    if node_ids_arg.lower() == "all":
        return list(range(1, max_nodes.get(component, 5) + 1))
    
    # Caso contrário, processa a lista separada por vírgulas
    try:
        ids = [int(x.strip()) for x in node_ids_arg.split(",")]
        # Verifica se os IDs estão dentro do intervalo válido
        for node_id in ids:
            if node_id < 1 or node_id > max_nodes.get(component, 5):
                print(f"Aviso: ID {node_id} está fora do intervalo válido para {component} (1-{max_nodes.get(component, 5)})")
        return ids
    except ValueError:
        print(f"Erro: Formato inválido para node-ids. Use 'all' ou números separados por vírgula.")
        sys.exit(1)

def get_component_hostname(component: str, node_id: int) -> str:
    """
    Determina o hostname para um componente específico.
    
    Args:
        component: Tipo do componente
        node_id: ID do nó
        
    Returns:
        str: Hostname para o componente
    """
    # Mapeia tipo de componente para prefixo do hostname
    prefix_map = {
        "proposer": "proposer",
        "acceptor": "acceptor",
        "learner": "learner",
        "store": "cluster-store",
        "client": "client"
    }
    
    prefix = prefix_map.get(component, component)
    return f"{prefix}-{node_id}"

def configure_debug(component: str, node_id: int, action: str, level: str, port: int) -> Dict[str, Any]:
    """
    Configura o modo de debug para um nó específico.
    
    Args:
        component: Tipo do componente
        node_id: ID do nó
        action: Ação a ser executada (enable, disable, status)
        level: Nível de debug
        port: Porta HTTP
        
    Returns:
        Dict[str, Any]: Resultado da operação
    """
    hostname = get_component_hostname(component, node_id)
    url = f"http://{hostname}:{port}/debug/config"
    
    try:
        if action == "status":
            # Para status, usamos o endpoint de health que sempre deve estar disponível
            response = requests.get(f"http://{hostname}:{port}/health", timeout=2)
        else:
            # Para enable/disable, enviamos configuração para o endpoint de debug
            enabled = (action == "enable")
            payload = {"enabled": enabled, "level": level}
            response = requests.post(url, json=payload, timeout=2)
        
        # Processa a resposta
        if response.status_code == 200:
            return {
                "success": True,
                "node_id": node_id,
                "hostname": hostname,
                "response": response.json()
            }
        else:
            return {
                "success": False,
                "node_id": node_id,
                "hostname": hostname,
                "error": f"Status code: {response.status_code}"
            }
    except requests.RequestException as e:
        return {
            "success": False,
            "node_id": node_id,
            "hostname": hostname,
            "error": str(e)
        }

def main():
    """Função principal."""
    args = parse_args()
    
    # Obtém lista de nós para configurar
    node_ids = get_node_list(args.component, args.node_ids)
    
    print(f"== Configurando {args.action} debug para {args.component} (nós: {node_ids}) ==")
    
    # Aplica configuração para cada nó
    results = []
    for node_id in node_ids:
        result = configure_debug(
            component=args.component,
            node_id=node_id,
            action=args.action,
            level=args.level,
            port=args.port
        )
        results.append(result)
        
        # Mostra resultado imediato
        hostname = result.get("hostname", f"{args.component}-{node_id}")
        if result.get("success", False):
            if args.action == "status":
                debug_info = result.get("response", {}).get("debug_enabled", "?")
                debug_level = result.get("response", {}).get("debug_level", "?")
                print(f"✅ {hostname}: Debug {'Ativado' if debug_info else 'Desativado'}, Nível: {debug_level}")
            else:
                print(f"✅ {hostname}: {args.action} concluído com sucesso")
        else:
            print(f"❌ {hostname}: Falha - {result.get('error', 'Erro desconhecido')}")
    
    # Resumo final
    success_count = sum(1 for r in results if r.get("success", False))
    print(f"\n== Resumo: {success_count}/{len(node_ids)} nós configurados com sucesso ==")

if __name__ == "__main__":
    main()