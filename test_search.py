"""
Script de teste para a funcionalidade de pesquisa da aplicação de Indexação de Documentos.
Este script testa as capacidades de pesquisa básica e avançada.
"""

import os
import sys
import logging
from elasticsearch import Elasticsearch

# Adiciona a raiz do projeto ao caminho Python
project_root = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, project_root)

# Importa configuração e utilitários do Elasticsearch
from config.config import ES_CONFIG
from src.elasticsearch_utils import (
    create_elasticsearch_client, 
    search_documents,
    advanced_search
)

# Configura o logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_elasticsearch_connection():
    """Testa a conexão com o Elasticsearch."""
    try:
        es = create_elasticsearch_client(
            ES_CONFIG['host'], 
            ES_CONFIG['port'], 
            ES_CONFIG['scheme']
        )
        if es.ping():
            logger.info("Conectado com sucesso ao Elasticsearch")
            return True, es
        else:
            logger.error("Falha ao fazer ping no Elasticsearch")
            return False, None
    except Exception as e:
        logger.error(f"Erro ao conectar ao Elasticsearch: {str(e)}")
        return False, None

def check_index_exists(es, index_name):
    """Verifica se o índice especificado existe no Elasticsearch."""
    try:
        if es.indices.exists(index=index_name):
            logger.info(f"Índice '{index_name}' existe")
            return True
        else:
            logger.error(f"Índice '{index_name}' não existe")
            return False
    except Exception as e:
        logger.error(f"Erro ao verificar se o índice existe: {str(e)}")
        return False

def test_basic_search(es, index_name, query_text="test"):
    """Testa a funcionalidade de pesquisa básica."""
    logger.info(f"Testando pesquisa básica com consulta: '{query_text}'")

    try:
        results = search_documents(es, index_name, query_text)

        logger.info(f"Pesquisa retornou {len(results)} resultados")

        if results:
            logger.info("Teste de pesquisa básica passou!")
            # Exibe o primeiro resultado
            doc = results[0]
            logger.info(f"Resultado principal: {doc['file_name']} (Pontuação: {doc['score']:.2f})")
            if 'highlights' in doc and doc['highlights']:
                logger.info(f"Destaque: {doc['highlights'][0][:100]}...")
            return True
        else:
            logger.warning("Teste de pesquisa básica não retornou resultados")
            return False
    except Exception as e:
        logger.error(f"Erro no teste de pesquisa básica: {str(e)}")
        return False

def test_advanced_search(es, index_name):
    """Testa a funcionalidade de pesquisa avançada com vários filtros."""
    logger.info("Testando pesquisa avançada com filtros")

    try:
        # Teste 1: Pesquisa por tipo de arquivo
        logger.info("Teste 1: Pesquisa por tipo de arquivo")
        results = advanced_search(es, index_name, file_type="pdf")
        logger.info(f"Encontrados {len(results)} arquivos PDF")

        # Teste 2: Pesquisa por texto e ordenação por tamanho de arquivo
        logger.info("Teste 2: Pesquisa por texto e ordenação por tamanho de arquivo")
        results = advanced_search(
            es, 
            index_name, 
            query_text="data", 
            sort_by="file_size", 
            sort_order="desc"
        )
        logger.info(f"Encontrados {len(results)} documentos contendo 'data', ordenados por tamanho de arquivo")

        # Teste 3: Pesquisa com filtro de tamanho de arquivo
        logger.info("Teste 3: Pesquisa com filtro de tamanho de arquivo")
        results = advanced_search(
            es, 
            index_name, 
            min_size=1000,  # Arquivos maiores que 1KB
            sort_by="file_size", 
            sort_order="asc"
        )
        logger.info(f"Encontrados {len(results)} documentos maiores que 1KB, ordenados por tamanho de arquivo (crescente)")

        logger.info("Testes de pesquisa avançada concluídos!")
        return True
    except Exception as e:
        logger.error(f"Erro no teste de pesquisa avançada: {str(e)}")
        return False

def run_search_tests():
    """Executa todos os testes de pesquisa."""
    logger.info("=== Iniciando testes de funcionalidade de pesquisa ===")

    # Testa conexão com Elasticsearch
    connection_success, es = test_elasticsearch_connection()
    if not connection_success:
        logger.error("Testes falharam: Não foi possível conectar ao Elasticsearch")
        return False

    # Verifica se o índice existe
    if not check_index_exists(es, ES_CONFIG['index']):
        logger.error(f"Testes falharam: Índice '{ES_CONFIG['index']}' não existe")
        logger.info("Por favor, execute 'python src/main.py index' primeiro para indexar documentos")
        return False

    # Testa pesquisa básica
    basic_search_success = test_basic_search(es, ES_CONFIG['index'])

    # Testa pesquisa avançada
    advanced_search_success = test_advanced_search(es, ES_CONFIG['index'])

    # Resultado geral dos testes
    if basic_search_success and advanced_search_success:
        logger.info("=== Todos os testes de pesquisa passaram! ===")
        return True
    else:
        logger.warning("=== Alguns testes de pesquisa falharam ou não retornaram resultados ===")
        return False

if __name__ == "__main__":
    try:
        success = run_search_tests()
        if success:
            print("\n\n=== RESULTADO DO TESTE DE PESQUISA: APROVADO ===\n\n")
            sys.exit(0)
        else:
            print("\n\n=== RESULTADO DO TESTE DE PESQUISA: PROBLEMAS DETECTADOS ===\n\n")
            sys.exit(1)
    except Exception as e:
        logger.error(f"=== Erro inesperado durante a execução do teste: {str(e)} ===")
        print("\n\n=== RESULTADO DO TESTE DE PESQUISA: ERRO ===\n\n")
        sys.exit(2)
