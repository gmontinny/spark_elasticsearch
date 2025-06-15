"""
Módulo de utilitários Elasticsearch para a aplicação de Indexação de Documentos.
Contém funções para interagir com o Elasticsearch.
"""

import logging
from typing import List, Dict, Any, Tuple

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_elasticsearch_client(host: str, port: int, scheme: str = 'http') -> Elasticsearch:
    """
    Cria e retorna um cliente Elasticsearch.

    Args:
        host: Host do Elasticsearch
        port: Porta do Elasticsearch
        scheme: Esquema de conexão (http ou https)

    Returns:
        Cliente Elasticsearch
    """
    try:
        # Use the URL format for Elasticsearch 8.x
        es_url = f"{scheme}://{host}:{port}"
        es = Elasticsearch(es_url, verify_certs=False)
        logger.info(f"Conectado ao Elasticsearch em {es_url}")
        return es
    except Exception as e:
        logger.error(f"Falha ao conectar ao Elasticsearch: {str(e)}")
        raise

def ensure_index_exists(es: Elasticsearch, index_name: str) -> None:
    """
    Garante que o índice especificado existe no Elasticsearch.

    Args:
        es: Cliente Elasticsearch
        index_name: Nome do índice para verificar/criar
    """
    try:
        # In Elasticsearch 8.x
        # Check if index exists
        if not es.indices.exists(index=index_name):
            # Create index with empty settings
            es.indices.create(index=index_name, ignore=400)
            logger.info(f"Índice Elasticsearch criado: {index_name}")
        else:
            logger.info(f"Índice Elasticsearch já existe: {index_name}")
    except Exception as e:
        logger.error(f"Falha ao criar índice Elasticsearch {index_name}: {str(e)}")
        raise

def index_documents(es: Elasticsearch, index_name: str, documents: List[Dict[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Indexa documentos no Elasticsearch.

    Args:
        es: Cliente Elasticsearch
        index_name: Nome do índice a ser usado
        documents: Lista de documentos para indexar

    Returns:
        Tupla contendo o número de documentos indexados com sucesso e uma lista de documentos que falharam
    """
    if not documents:
        logger.warning("Nenhum documento para indexar")
        return 0, []

    # Garantir que o índice existe
    ensure_index_exists(es, index_name)

    # Preparar ações para indexação em lote
    # Format for Elasticsearch 8.x
    actions = [
        {
            "_index": index_name,
            "_source": doc
        }
        for doc in documents
    ]

    # Rastrear documentos que falharam
    failed_docs = []

    try:
        # Realizar indexação em lote
        success, failed = bulk(es, actions, stats_only=False, raise_on_error=False)

        # Registrar resultados
        if failed:
            for item in failed:
                logger.error(f"Falha ao indexar documento: {item}")
                # In Elasticsearch 8.x, failed items have '_source'
                if '_source' in item:
                    failed_docs.append(item['_source'])
                elif 'document' in item:  # Fallback for backward compatibility
                    failed_docs.append(item['document'])

        logger.info(f"Indexados com sucesso {success} documentos no Elasticsearch")
        return success, failed_docs
    except Exception as e:
        logger.error(f"Erro durante a indexação em lote: {str(e)}")
        return 0, documents
