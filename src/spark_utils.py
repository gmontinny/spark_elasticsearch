"""
Módulo utilitário Spark para a aplicação de Indexação de Documentos.
Contém funções para interagir com o Apache Spark.
"""

import logging
import os
from typing import List, Dict, Any

# Importar findspark para ajudar a localizar a instalação do Spark
try:
    import findspark
    findspark.init(spark_home=os.environ.get('SPARK_HOME'))
except ImportError:
    # Se findspark não estiver disponível, tentaremos prosseguir sem ele
    pass

from pyspark.sql import SparkSession, DataFrame

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_spark_session(app_name: str, packages: str, master: str = 'local[*]') -> SparkSession:
    """
    Cria e retorna uma sessão Spark.

    Args:
        app_name: Nome da aplicação Spark
        packages: Lista de pacotes separados por vírgula para incluir
        master: URL do Spark master

    Returns:
        Objeto SparkSession
    """
    try:
        # Criar a SparkSession usando o padrão builder
        # A SparkSession criará ou reutilizará um SparkContext conforme necessário
        builder = SparkSession.builder \
            .appName(app_name) \
            .config("spark.jars.packages", packages) \
            .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
            .config("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8") \
            .master(master)

        try:
            # Tentar habilitar o suporte Hive, o que pode ajudar com certos problemas de inicialização
            builder = builder.enableHiveSupport()
        except Exception as e:
            logger.warning(f"Não foi possível habilitar o suporte Hive: {str(e)}")

        # Tentar criar a SparkSession
        try:
            # Primeira tentativa: usar o builder diretamente
            spark = builder.getOrCreate()
        except Exception as e:
            logger.warning(f"Primeira tentativa de criar SparkSession falhou: {str(e)}")

            # Segunda tentativa: tentar uma configuração mais simples
            try:
                logger.info("Tentando método alternativo de criação de SparkSession...")
                spark = SparkSession.builder \
                    .appName(app_name) \
                    .master(master) \
                    .getOrCreate()
            except Exception as e2:
                logger.error(f"Todas as tentativas de criar SparkSession falharam: {str(e2)}")
                raise

        # Definir nível de log após a sessão ser criada
        spark.sparkContext.setLogLevel("WARN")

        logger.info(f"Sessão Spark criada: {app_name}")
        return spark
    except Exception as e:
        logger.error(f"Falha ao criar sessão Spark: {str(e)}")
        raise

def create_dataframe_from_documents(spark: SparkSession, documents: List[Dict[str, Any]]) -> DataFrame:
    """
    Cria um DataFrame Spark a partir de uma lista de documentos.

    Args:
        spark: Objeto SparkSession
        documents: Lista de dicionários de documentos

    Returns:
        DataFrame Spark contendo os documentos
    """
    if not documents:
        logger.warning("Não há documentos para criar o DataFrame")
        # Criar um DataFrame vazio com o esquema esperado
        return spark.createDataFrame([], schema=["file_name", "file_path", "file_type", "content", "file_size"])

    try:
        df = spark.createDataFrame(documents)
        logger.info(f"DataFrame criado com {df.count()} documentos")
        return df
    except Exception as e:
        logger.error(f"Falha ao criar DataFrame: {str(e)}")
        raise

def write_dataframe_to_elasticsearch(
    df: DataFrame, 
    es_host: str, 
    es_port: int, 
    es_index: str,
    mode: str = "append"
) -> None:
    """
    Escreve um DataFrame Spark no Elasticsearch.

    Args:
        df: DataFrame Spark para escrever
        es_host: Host do Elasticsearch
        es_port: Porta do Elasticsearch
        es_index: Nome do índice do Elasticsearch
        mode: Modo de escrita (append, overwrite, etc.)
    """
    if df.rdd.isEmpty():
        logger.warning("DataFrame está vazio, nada para escrever no Elasticsearch")
        return

    try:
        df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", es_host) \
            .option("es.port", es_port) \
            .option("es.resource", es_index) \
            .option("es.mapping.id", "file_path") \
            .option("es.nodes.wan.only", "true") \
            .option("es.net.http.auth.user", "") \
            .option("es.net.http.auth.pass", "") \
            .mode(mode) \
            .save()

        logger.info(f"DataFrame escrito com sucesso no índice Elasticsearch: {es_index}")
    except Exception as e:
        logger.error(f"Falha ao escrever DataFrame no Elasticsearch: {str(e)}")
        raise
