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

Uso:
  - Para indexar documentos: python src/main.py index
  - Para pesquisar documentos: python src/main.py search "texto para pesquisar"
  - Para pesquisa avançada: python src/main.py advanced-search --query "texto para pesquisar" --type pdf --min-size 1000 --max-size 5000000 --sort-by file_name --sort-order asc
"""

import os
import sys
import logging
import argparse
from typing import List, Dict, Any

# Adiciona a raiz do projeto ao caminho Python
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

# Importa módulos utilitários
from config.config import ES_CONFIG, SPARK_CONFIG, ENV_VARS, APP_CONFIG
from src.file_processors import process_file
from src.elasticsearch_utils import (
    create_elasticsearch_client, 
    index_documents, 
    search_documents,
    advanced_search
)
from src.spark_utils import create_spark_session, create_dataframe_from_documents, write_dataframe_to_elasticsearch

# Configurar variáveis de ambiente conforme especificado
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Configura o logging
logging.basicConfig(
    level=getattr(logging, APP_CONFIG['log_level']),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def setup_environment() -> None:
    """Configura variáveis de ambiente para Spark e Hadoop."""
    for key, value in ENV_VARS.items():
        if value and key not in ['PYSPARK_PYTHON', 'PYSPARK_DRIVER_PYTHON']:  # Só define se o valor não estiver vazio e não estiver já definido
            os.environ[key] = value
            logger.debug(f"Variável de ambiente definida: {key}={value}")

def process_directory(directory_path: str) -> List[Dict[str, Any]]:
    """
    Processa todos os arquivos no diretório especificado e seus subdiretórios.

    Args:
        directory_path: Caminho para o diretório contendo arquivos para processar

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

            # Processa o arquivo
            doc = process_file(file_path)
            if doc:
                documents.append(doc)
                processed_count += 1

    logger.info(f"Processados {processed_count} de {file_count} arquivos com sucesso")
    return documents

def index_documents_command() -> None:
    """Indexa documentos do diretório de entrada no Elasticsearch."""
    try:
        # Configura variáveis de ambiente
        setup_environment()

        # Cria cliente Elasticsearch
        es = create_elasticsearch_client(
            ES_CONFIG['host'], 
            ES_CONFIG['port'], 
            ES_CONFIG['scheme']
        )

        # Cria sessão Spark
        spark = create_spark_session(
            SPARK_CONFIG['app_name'],
            SPARK_CONFIG['packages'],
            SPARK_CONFIG['master']
        )

        # Processa arquivos no diretório de entrada
        input_directory = APP_CONFIG['input_directory']
        documents = process_directory(input_directory)

        if not documents:
            logger.warning("Nenhum documento válido encontrado para indexação")
            spark.stop()
            return

        # Indexa documentos no Elasticsearch usando a função auxiliar
        success, failed = index_documents(es, ES_CONFIG['index'], documents)
        logger.info(f"Indexados {success} documentos no Elasticsearch")

        if failed:
            logger.warning(f"Falha ao indexar {len(failed)} documentos")

        # Cria DataFrame e escreve no Elasticsearch usando Spark
        df = create_dataframe_from_documents(spark, documents)

        write_dataframe_to_elasticsearch(
            df,
            ES_CONFIG['host'],
            ES_CONFIG['port'],
            ES_CONFIG['index']
        )

        # Para a sessão Spark
        spark.stop()
        logger.info("Indexação de documentos concluída com sucesso")

    except Exception as e:
        logger.error(f"Erro na operação de indexação: {str(e)}", exc_info=True)
        sys.exit(1)

def search_documents_command(query_text: str, size: int = 10) -> None:
    """
    Pesquisa documentos que correspondem ao texto da consulta.

    Args:
        query_text: Texto a ser pesquisado
        size: Número máximo de resultados a retornar
    """
    try:
        # Cria cliente Elasticsearch
        es = create_elasticsearch_client(
            ES_CONFIG['host'], 
            ES_CONFIG['port'], 
            ES_CONFIG['scheme']
        )

        # Realiza a pesquisa
        results = search_documents(es, ES_CONFIG['index'], query_text, size)

        if not results:
            print(f"Nenhum documento encontrado correspondente a '{query_text}'")
            return

        # Exibe resultados
        print(f"\nEncontrados {len(results)} documentos correspondentes a '{query_text}':\n")

        for i, doc in enumerate(results, 1):
            print(f"{i}. {doc['file_name']} (Pontuação: {doc['score']:.2f})")
            print(f"   Tipo: {doc['file_type']}, Tamanho: {doc['file_size']} bytes")

            if 'highlights' in doc and doc['highlights']:
                print("   Destaques:")
                for highlight in doc['highlights'][:2]:  # Mostra no máximo 2 destaques
                    # Limpa o texto de destaque para exibição
                    highlight_text = highlight.replace('\n', ' ').strip()
                    if len(highlight_text) > 100:
                        highlight_text = highlight_text[:100] + "..."
                    print(f"   - {highlight_text}")

            print()  # Linha vazia entre resultados

    except Exception as e:
        logger.error(f"Erro na operação de pesquisa: {str(e)}", exc_info=True)
        sys.exit(1)

def advanced_search_command(
    query_text: str = None,
    file_type: str = None,
    min_size: int = None,
    max_size: int = None,
    sort_by: str = "score",
    sort_order: str = "desc",
    size: int = 10
) -> None:
    """
    Realiza pesquisa avançada com filtros.

    Args:
        query_text: Texto a ser pesquisado (opcional)
        file_type: Tipo de arquivo para filtrar (opcional)
        min_size: Tamanho mínimo do arquivo em bytes (opcional)
        max_size: Tamanho máximo do arquivo em bytes (opcional)
        sort_by: Campo para ordenação (score, file_size, file_name)
        sort_order: Ordem de classificação (asc, desc)
        size: Número máximo de resultados a retornar
    """
    try:
        # Cria cliente Elasticsearch
        es = create_elasticsearch_client(
            ES_CONFIG['host'], 
            ES_CONFIG['port'], 
            ES_CONFIG['scheme']
        )

        # Constrói descrição da pesquisa para saída
        search_desc = []
        if query_text:
            search_desc.append(f"texto '{query_text}'")
        if file_type:
            search_desc.append(f"tipo de arquivo '{file_type}'")
        if min_size is not None:
            search_desc.append(f"tamanho mínimo {min_size} bytes")
        if max_size is not None:
            search_desc.append(f"tamanho máximo {max_size} bytes")

        search_criteria = ", ".join(search_desc) if search_desc else "todos os documentos"

        # Realiza pesquisa avançada
        results = advanced_search(
            es, 
            ES_CONFIG['index'], 
            query_text, 
            file_type, 
            min_size, 
            max_size, 
            sort_by, 
            sort_order, 
            size
        )

        if not results:
            print(f"Nenhum documento encontrado correspondente aos critérios: {search_criteria}")
            return

        # Exibe resultados
        print(f"\nEncontrados {len(results)} documentos correspondentes aos critérios: {search_criteria}\n")
        print(f"Ordenados por: {sort_by} ({sort_order}endente)\n")

        for i, doc in enumerate(results, 1):
            score_display = f"{doc['score']:.2f}" if doc['score'] is not None else "N/A"
            print(f"{i}. {doc['file_name']} (Pontuação: {score_display})")
            print(f"   Tipo: {doc['file_type']}, Tamanho: {doc['file_size']} bytes")

            if 'highlights' in doc and doc['highlights']:
                print("   Destaques:")
                for highlight in doc['highlights'][:2]:  # Mostra no máximo 2 destaques
                    # Limpa o texto de destaque para exibição
                    highlight_text = highlight.replace('\n', ' ').strip()
                    if len(highlight_text) > 100:
                        highlight_text = highlight_text[:100] + "..."
                    print(f"   - {highlight_text}")

            print()  # Linha vazia entre resultados

    except Exception as e:
        logger.error(f"Erro na operação de pesquisa avançada: {str(e)}", exc_info=True)
        sys.exit(1)

def main() -> None:
    """Função principal para analisar argumentos e executar o comando apropriado."""
    parser = argparse.ArgumentParser(
        description="Aplicação de Indexação e Pesquisa de Documentos",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  Indexar documentos:
    python src/main.py index

  Pesquisar documentos:
    python src/main.py search "aprendizado de máquina"

  Pesquisa avançada:
    python src/main.py advanced-search --query "análise de dados" --type pdf --min-size 1000 --sort-by file_name
        """
    )

    subparsers = parser.add_subparsers(dest="command", help="Comando a ser executado")

    # Comando de indexação
    index_parser = subparsers.add_parser("index", help="Indexa documentos do diretório de entrada")

    # Comando de pesquisa
    search_parser = subparsers.add_parser("search", help="Pesquisa documentos")
    search_parser.add_argument("query", help="Texto a ser pesquisado")
    search_parser.add_argument("--size", type=int, default=10, help="Número máximo de resultados a retornar")

    # Comando de pesquisa avançada
    adv_search_parser = subparsers.add_parser("advanced-search", help="Pesquisa avançada com filtros")
    adv_search_parser.add_argument("--query", help="Texto a ser pesquisado")
    adv_search_parser.add_argument("--type", dest="file_type", help="Tipo de arquivo para filtrar (ex: pdf, docx)")
    adv_search_parser.add_argument("--min-size", type=int, help="Tamanho mínimo do arquivo em bytes")
    adv_search_parser.add_argument("--max-size", type=int, help="Tamanho máximo do arquivo em bytes")
    adv_search_parser.add_argument("--sort-by", choices=["score", "file_size", "file_name"], default="score", 
                                  help="Campo para ordenação")
    adv_search_parser.add_argument("--sort-order", choices=["asc", "desc"], default="desc", 
                                  help="Ordem de classificação (ascendente ou descendente)")
    adv_search_parser.add_argument("--size", type=int, default=10, help="Número máximo de resultados a retornar")

    # Analisa argumentos
    args = parser.parse_args()

    # Executa o comando apropriado
    if args.command == "index":
        index_documents_command()
    elif args.command == "search":
        search_documents_command(args.query, args.size)
    elif args.command == "advanced-search":
        advanced_search_command(
            args.query, 
            args.file_type, 
            args.min_size, 
            args.max_size, 
            args.sort_by, 
            args.sort_order, 
            args.size
        )
    else:
        parser.print_help()
        sys.exit(0)

    logger.info(f"Comando '{args.command}' concluído com sucesso")

if __name__ == "__main__":
    main()
