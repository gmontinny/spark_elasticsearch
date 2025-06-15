"""
Script de teste simples para verificar se a funcionalidade de indexação do Elasticsearch está funcionando corretamente.
Este script testa apenas a funcionalidade do cliente Elasticsearch direto, não a integração com o Spark.
"""

import os
import sys
import logging
import time
from elasticsearch import Elasticsearch

# Adicionar a raiz do projeto ao caminho Python
project_root = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, project_root)

# Importar configuração e utilitários do Elasticsearch
from config.config import ES_CONFIG
from src.elasticsearch_utils import create_elasticsearch_client, ensure_index_exists, index_documents
from src.file_processors import process_file

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_elasticsearch_connection():
    """Testar conexão com o Elasticsearch."""
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
    """Verificar se o índice especificado existe no Elasticsearch."""
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

def count_documents(es, index_name):
    """Contar o número de documentos no índice especificado."""
    try:
        # Atualizar o índice para garantir que todos os documentos estejam visíveis
        es.indices.refresh(index=index_name)

        # Contar documentos
        count = es.count(index=index_name)['count']
        logger.info(f"Encontrados {count} documentos no índice '{index_name}'")
        return count
    except Exception as e:
        logger.error(f"Erro ao contar documentos: {str(e)}")
        return 0

def process_directory(directory_path):
    """Processar todos os arquivos no diretório especificado e seus subdiretórios."""
    if not os.path.exists(directory_path):
        logger.error(f"Diretório não existe: {directory_path}")
        return []

    documents = []
    file_count = 0
    processed_count = 0

    logger.info(f"Processando arquivos no diretório: {directory_path}")

    for root, _, files in os.walk(directory_path):
        for file in files:
            file_path = os.path.join(root, file)
            file_count += 1

            # Processar o arquivo
            doc = process_file(file_path)
            if doc:
                documents.append(doc)
                processed_count += 1

    logger.info(f"Processados {processed_count} de {file_count} arquivos com sucesso")
    return documents

def run_simple_test():
    """Executar um teste simples para verificar a funcionalidade de indexação do Elasticsearch."""
    logger.info("=== Iniciando teste simples ===")

    # Testar conexão com o Elasticsearch
    connection_success, es = test_elasticsearch_connection()
    if not connection_success:
        logger.error("Teste falhou: Não foi possível conectar ao Elasticsearch")
        return False

    # Processar arquivos no diretório de entrada
    input_directory = os.path.join(os.getcwd(), "data")
    documents = process_directory(input_directory)

    if not documents:
        logger.warning("Nenhum documento válido encontrado para indexação")
        return False

    # Indexar documentos no Elasticsearch
    try:
        success, failed = index_documents(es, ES_CONFIG['index'], documents)
        logger.info(f"Indexados {success} documentos no Elasticsearch")

        if failed:
            logger.warning(f"Falha ao indexar {len(failed)} documentos")
    except Exception as e:
        logger.error(f"Erro ao indexar documentos: {str(e)}")
        return False

    # Dar algum tempo para o Elasticsearch processar os documentos
    logger.info("Aguardando o Elasticsearch processar os documentos...")
    time.sleep(2)

    # Verificar se o índice existe
    if not check_index_exists(es, ES_CONFIG['index']):
        logger.error(f"Teste falhou: Índice '{ES_CONFIG['index']}' não foi criado")
        return False

    # Contar documentos no índice
    doc_count = count_documents(es, ES_CONFIG['index'])
    if doc_count > 0:
        logger.info(f"Teste passou: Indexados com sucesso {doc_count} documentos usando o cliente Elasticsearch direto")
        return True
    else:
        logger.error("Teste falhou: Nenhum documento foi indexado")
        return False

if __name__ == "__main__":
    try:
        success = run_simple_test()
        if success:
            logger.info("=== Teste simples passou! A funcionalidade de indexação do Elasticsearch está funcionando corretamente. ===")
            print("\n\n=== RESULTADO DO TESTE: PASSOU ===\n\n")
            sys.exit(0)
        else:
            logger.error("=== Teste simples falhou! A funcionalidade de indexação do Elasticsearch não está funcionando corretamente. ===")
            print("\n\n=== RESULTADO DO TESTE: FALHOU ===\n\n")
            sys.exit(1)
    except Exception as e:
        logger.error(f"=== Erro inesperado durante a execução do teste: {str(e)} ===")
        print("\n\n=== RESULTADO DO TESTE: ERRO ===\n\n")
        sys.exit(2)
