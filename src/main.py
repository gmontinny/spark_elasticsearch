"""
Aplicação de Indexação de Documentos

Esta aplicação indexa documentos de vários formatos de arquivo no Elasticsearch
usando o Apache Spark para processamento.

Formatos de arquivo suportados:
- DOCX, DOC (Microsoft Word)
- XLSX, XLS (Microsoft Excel)
- PDF
- CSV

A aplicação extrai texto desses documentos e os indexa no Elasticsearch
para capacidades de pesquisa de texto completo.
"""

import os
import sys
import logging
from typing import List, Dict, Any

# Adicionar a raiz do projeto ao caminho Python
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

# Importar módulos utilitários
from config.config import ES_CONFIG, SPARK_CONFIG, ENV_VARS, APP_CONFIG
from src.file_processors import process_file
from src.elasticsearch_utils import create_elasticsearch_client, index_documents
from src.spark_utils import create_spark_session, create_dataframe_from_documents, write_dataframe_to_elasticsearch

# Configurar variáveis de ambiente conforme especificado
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Configurar logging
logging.basicConfig(
    level=getattr(logging, APP_CONFIG['log_level']),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def setup_environment() -> None:
    """Configurar variáveis de ambiente para Spark e Hadoop."""
    for key, value in ENV_VARS.items():
        if value and key not in ['PYSPARK_PYTHON', 'PYSPARK_DRIVER_PYTHON']:  # Definir apenas se o valor não estiver vazio e ainda não estiver definido
            os.environ[key] = value
            logger.debug(f"Variável de ambiente definida: {key}={value}")

def process_directory(directory_path: str) -> List[Dict[str, Any]]:
    """
    Processa todos os arquivos no diretório especificado e seus subdiretórios.

    Args:
        directory_path: Caminho para o diretório contendo arquivos a serem processados

    Returns:
        Lista de dicionários de documentos com conteúdo extraído
    """
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

def main() -> None:
    """Função principal para executar a aplicação de indexação de documentos."""
    try:
        # Configurar variáveis de ambiente
        setup_environment()

        # Criar cliente Elasticsearch
        es = create_elasticsearch_client(
            ES_CONFIG['host'], 
            ES_CONFIG['port'], 
            ES_CONFIG['scheme']
        )

        # Criar sessão Spark
        spark = create_spark_session(
            SPARK_CONFIG['app_name'],
            SPARK_CONFIG['packages'],
            SPARK_CONFIG['master']
        )

        # Processar arquivos no diretório de entrada
        input_directory = APP_CONFIG['input_directory']
        documents = process_directory(input_directory)

        if not documents:
            logger.warning("Nenhum documento válido encontrado para indexação")
            spark.stop()
            return

        # Indexar documentos no Elasticsearch usando a função auxiliar
        success, failed = index_documents(es, ES_CONFIG['index'], documents)
        logger.info(f"Indexados {success} documentos no Elasticsearch")

        if failed:
            logger.warning(f"Falha ao indexar {len(failed)} documentos")

        # Criar DataFrame e escrever no Elasticsearch usando Spark
        df = create_dataframe_from_documents(spark, documents)

        write_dataframe_to_elasticsearch(
            df,
            ES_CONFIG['host'],
            ES_CONFIG['port'],
            ES_CONFIG['index']
        )

        # Parar sessão Spark
        spark.stop()
        logger.info("Indexação de documentos concluída com sucesso")

    except Exception as e:
        logger.error(f"Erro na aplicação principal: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
