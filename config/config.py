"""
Arquivo de configuração para a aplicação de Indexação de Documentos.
Contém todos os parâmetros configuráveis para a aplicação.
"""

import os

# Configuração do Elasticsearch
ES_CONFIG = {
    'host': 'localhost',
    'port': 9200,
    'index': 'document_index',
    'scheme': 'http'
}

# Configuração do Spark
SPARK_CONFIG = {
    'app_name': 'DocumentIndexer',
    'packages': 'org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0',
    'master': 'local[*]'  # Usa todos os núcleos disponíveis
}

# Variáveis de ambiente
ENV_VARS = {
    'PYSPARK_PYTHON': os.environ.get('PYSPARK_PYTHON', ''),
    'PYSPARK_DRIVER_PYTHON': os.environ.get('PYSPARK_DRIVER_PYTHON', ''),
    'JAVA_HOME': os.environ.get('JAVA_HOME', r'C:\java\jdk-11.0.25'),
    'SPARK_HOME': os.environ.get('SPARK_HOME', r'C:\Spark\spark-3.5.5-bin-hadoop3'),
    'HADOOP_HOME': os.environ.get('HADOOP_HOME', r'C:\hadoop')
}

# Configuração da aplicação
APP_CONFIG = {
    'input_directory': os.path.join(os.getcwd(), "data"),
    'log_level': 'INFO',
    'batch_size': 100  # Número de documentos para processar em um lote
}

# Tipos de arquivo suportados e suas extensões
SUPPORTED_FILE_TYPES = {
    'document': ['.docx', '.doc'],
    'spreadsheet': ['.xlsx', '.xls'],
    'pdf': ['.pdf'],
    'csv': ['.csv']
}
