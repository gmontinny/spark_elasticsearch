"""
Módulo de utilitários Elasticsearch para a aplicação de Indexação de Documentos.
Contém funções para interagir com o Elasticsearch.
"""

import logging
from typing import List, Dict, Any, Tuple, Optional

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
            # Create index with mapping that supports sorting on text fields
            mapping = {
                "mappings": {
                    "properties": {
                        "file_name": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        },
                        "file_path": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 512
                                }
                            }
                        },
                        "file_type": {
                            "type": "keyword"
                        },
                        "file_size": {
                            "type": "long"
                        },
                        "content": {
                            "type": "text"
                        }
                    }
                }
            }
            es.indices.create(index=index_name, body=mapping, ignore=400)
            logger.info(f"Índice Elasticsearch criado com mapeamento personalizado: {index_name}")
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

def search_documents(
    es: Elasticsearch, 
    index_name: str, 
    query_text: str, 
    size: int = 10
) -> List[Dict[str, Any]]:
    """
    Realiza uma pesquisa de texto nos documentos indexados.

    Args:
        es: Cliente Elasticsearch
        index_name: Nome do índice a ser pesquisado
        query_text: Texto a ser pesquisado
        size: Número máximo de resultados a retornar

    Returns:
        Lista de documentos que correspondem à consulta
    """
    try:
        # Verificar se o índice existe
        if not es.indices.exists(index=index_name):
            logger.warning(f"Índice {index_name} não existe")
            return []

        # Construir a consulta de pesquisa
        query = {
            "query": {
                "multi_match": {
                    "query": query_text,
                    "fields": ["content", "file_name"],
                    "type": "best_fields",
                    "fuzziness": "AUTO"
                }
            },
            "highlight": {
                "fields": {
                    "content": {}
                }
            },
            "size": size
        }

        # Executar a pesquisa
        response = es.search(index=index_name, body=query)

        # Processar resultados
        hits = response.get('hits', {}).get('hits', [])
        results = []

        for hit in hits:
            doc = hit['_source']
            score = hit['_score']
            highlights = hit.get('highlight', {}).get('content', [])

            # Adicionar score e highlights ao documento
            doc['score'] = score
            doc['highlights'] = highlights

            results.append(doc)

        logger.info(f"Pesquisa por '{query_text}' retornou {len(results)} resultados")
        return results
    except Exception as e:
        logger.error(f"Erro durante a pesquisa: {str(e)}")
        return []

def advanced_search(
    es: Elasticsearch,
    index_name: str,
    query_text: Optional[str] = None,
    file_type: Optional[str] = None,
    min_size: Optional[int] = None,
    max_size: Optional[int] = None,
    sort_by: str = "score",
    sort_order: str = "desc",
    size: int = 10
) -> List[Dict[str, Any]]:
    """
    Realiza uma pesquisa avançada nos documentos indexados com filtros.

    Args:
        es: Cliente Elasticsearch
        index_name: Nome do índice a ser pesquisado
        query_text: Texto a ser pesquisado (opcional)
        file_type: Tipo de arquivo para filtrar (opcional)
        min_size: Tamanho mínimo do arquivo em bytes (opcional)
        max_size: Tamanho máximo do arquivo em bytes (opcional)
        sort_by: Campo para ordenação (score, file_size, file_name)
        sort_order: Ordem de classificação (asc, desc)
        size: Número máximo de resultados a retornar

    Returns:
        Lista de documentos que correspondem aos critérios de pesquisa
    """
    try:
        # Verificar se o índice existe
        if not es.indices.exists(index=index_name):
            logger.warning(f"Índice {index_name} não existe")
            return []

        # Construir a consulta
        query_parts = []

        # Adicionar consulta de texto se fornecida
        if query_text:
            query_parts.append({
                "multi_match": {
                    "query": query_text,
                    "fields": ["content", "file_name"],
                    "type": "best_fields",
                    "fuzziness": "AUTO"
                }
            })

        # Construir filtros
        filters = []

        if file_type:
            filters.append({"term": {"file_type": file_type}})

        # Adicionar filtros de tamanho se fornecidos
        size_filter = {}
        if min_size is not None:
            size_filter["gte"] = min_size
        if max_size is not None:
            size_filter["lte"] = max_size

        if size_filter:
            filters.append({"range": {"file_size": size_filter}})

        # Construir a consulta final
        query = {}

        if query_parts and filters:
            # Combinar consulta de texto e filtros
            query["query"] = {
                "bool": {
                    "must": query_parts,
                    "filter": filters
                }
            }
        elif query_parts:
            # Apenas consulta de texto
            query["query"] = {"bool": {"must": query_parts}}
        elif filters:
            # Apenas filtros
            query["query"] = {"bool": {"filter": filters}}
        else:
            # Sem consulta ou filtros, retornar todos os documentos
            query["query"] = {"match_all": {}}

        # Adicionar destaque se houver consulta de texto
        if query_text:
            query["highlight"] = {
                "fields": {
                    "content": {}
                }
            }

        # Adicionar ordenação
        if sort_by == "score":
            sort_field = "_score"
        elif sort_by == "file_name":
            # Use the keyword subfield for sorting on text fields
            sort_field = "file_name.keyword"
        else:
            sort_field = sort_by
        query["sort"] = [{sort_field: {"order": sort_order}}]

        # Definir tamanho
        query["size"] = size

        # Executar a pesquisa
        response = es.search(index=index_name, body=query)

        # Processar resultados
        hits = response.get('hits', {}).get('hits', [])
        results = []

        for hit in hits:
            doc = hit['_source']
            score = hit['_score']
            highlights = hit.get('highlight', {}).get('content', [])

            # Adicionar score e highlights ao documento
            doc['score'] = score
            if highlights:
                doc['highlights'] = highlights

            results.append(doc)

        logger.info(f"Pesquisa avançada retornou {len(results)} resultados")
        return results
    except Exception as e:
        logger.error(f"Erro durante a pesquisa avançada: {str(e)}")
        return []
