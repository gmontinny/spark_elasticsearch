# Aplicação de Indexação de Documentos

Esta aplicação indexa documentos de vários formatos de arquivo no Elasticsearch usando o Apache Spark para processamento.

## Formatos de Arquivo Suportados

- DOCX, DOC (Microsoft Word)
- XLSX, XLS (Microsoft Excel)
- PDF
- CSV

## Estrutura do Projeto

```
projeto-python/spark/spark_elasticsearch/
├── config/
│   ├── config.py             # Configurações
│   └── deepseek_bash_*.sh    # Script de instalação
├── data/                     # Diretório contendo documentos para indexar
├── src/
│   ├── elasticsearch_utils.py # Utilitários do Elasticsearch
│   ├── file_processors.py     # Utilitários de processamento de arquivos
│   ├── main.py                # Aplicação principal
│   └── spark_utils.py         # Utilitários do Spark
├── docker-compose.yml        # Configuração Docker para Elasticsearch
├── README.md                 # Este arquivo
└── requirements.txt          # Dependências Python
```

## Configuração

### Pré-requisitos

- Python 3.8+
- Java 11+
- Apache Spark 3.5+
- Elasticsearch 8.5+

### Instalação

1. Instale os pacotes Python necessários:

```bash
pip install -r requirements.txt
```

Ou use o script fornecido:

```bash
bash config/deepseek_bash_*.sh
```

2. Inicie o Elasticsearch usando Docker:

```bash
docker-compose up -d
```

3. Configure a aplicação editando o arquivo `config/config.py` se necessário.

## Uso

1. Coloque seus documentos no diretório `data/`.

2. Indexe os documentos:

```bash
python src/main.py index
```

3. Pesquise documentos:

```bash
# Pesquisa básica
python src/main.py search "texto para pesquisar"

# Pesquisa avançada com filtros
python src/main.py advanced-search --query "texto para pesquisar" --type pdf --min-size 1000 --sort-by file_name
```

## Configuração

A aplicação pode ser configurada editando o arquivo `config/config.py`:

- `ES_CONFIG`: Configurações de conexão do Elasticsearch
- `SPARK_CONFIG`: Configuração do Apache Spark
- `ENV_VARS`: Variáveis de ambiente para Spark e Hadoop
- `APP_CONFIG`: Configurações da aplicação como diretório de entrada e nível de log
- `SUPPORTED_FILE_TYPES`: Tipos de arquivo suportados pela aplicação

## Detalhes de Implementação

### Processamento de Arquivos

A aplicação processa arquivos da seguinte maneira:

1. Escaneia o diretório de entrada para tipos de arquivo suportados
2. Extrai o conteúdo de texto de cada arquivo com base em seu tipo
3. Cria metadados do documento incluindo nome do arquivo, caminho, tipo e tamanho
4. Indexa os documentos no Elasticsearch

### Integração com Elasticsearch

Os documentos são indexados no Elasticsearch de duas maneiras:

1. Usando o cliente Python do Elasticsearch para indexação direta
2. Usando o conector Spark-Elasticsearch para processamento em lote

## Melhorias Realizadas

As seguintes melhorias foram feitas na implementação original:

1. **Organização do Código**:
   - Código separado em módulos para melhor manutenção
   - Funções utilitárias criadas para tarefas específicas

2. **Tratamento de Erros**:
   - Adicionado tratamento adequado de exceções em todo o código
   - Implementado registro de logs para melhor depuração

3. **Gerenciamento de Configuração**:
   - Movida toda a configuração para um arquivo separado
   - Variáveis de ambiente tornadas configuráveis

4. **Suporte a Arquivos**:
   - Adicionado suporte para arquivos PDF
   - Adicionado suporte para arquivos CSV

5. **Desempenho**:
   - Operações do Spark otimizadas
   - Adicionado processamento em lote para indexação no Elasticsearch

6. **Funcionalidade de Pesquisa**:
   - Implementada pesquisa de texto completo nos documentos indexados
   - Adicionada pesquisa avançada com filtros por tipo de arquivo e tamanho
   - Suporte para ordenação de resultados por relevância, tamanho ou nome do arquivo
   - Destaque (highlighting) de trechos relevantes nos resultados da pesquisa

7. **Interface de Linha de Comando**:
   - Adicionados comandos para indexação e pesquisa
   - Suporte para argumentos de linha de comando para configurar pesquisas
   - Melhor feedback ao usuário com formatação de resultados

8. **Documentação**:
   - Adicionadas docstrings abrangentes
   - Criado este arquivo README
   - Incluídos exemplos de uso para todas as funcionalidades
