"""
Simple test script to verify that the Elasticsearch indexing functionality is working correctly.
This script tests only the direct Elasticsearch client functionality, not the Spark integration.
"""

import os
import sys
import logging
import time
from elasticsearch import Elasticsearch

# Add the project root to the Python path
project_root = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, project_root)

# Import configuration and Elasticsearch utilities
from config.config import ES_CONFIG
from src.elasticsearch_utils import create_elasticsearch_client, ensure_index_exists, index_documents
from src.file_processors import process_file

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_elasticsearch_connection():
    """Test connection to Elasticsearch."""
    try:
        es = create_elasticsearch_client(
            ES_CONFIG['host'], 
            ES_CONFIG['port'], 
            ES_CONFIG['scheme']
        )
        if es.ping():
            logger.info("Successfully connected to Elasticsearch")
            return True, es
        else:
            logger.error("Failed to ping Elasticsearch")
            return False, None
    except Exception as e:
        logger.error(f"Error connecting to Elasticsearch: {str(e)}")
        return False, None

def check_index_exists(es, index_name):
    """Check if the specified index exists in Elasticsearch."""
    try:
        if es.indices.exists(index=index_name):
            logger.info(f"Index '{index_name}' exists")
            return True
        else:
            logger.error(f"Index '{index_name}' does not exist")
            return False
    except Exception as e:
        logger.error(f"Error checking if index exists: {str(e)}")
        return False

def count_documents(es, index_name):
    """Count the number of documents in the specified index."""
    try:
        # Refresh the index to ensure all documents are visible
        es.indices.refresh(index=index_name)
        
        # Count documents
        count = es.count(index=index_name)['count']
        logger.info(f"Found {count} documents in index '{index_name}'")
        return count
    except Exception as e:
        logger.error(f"Error counting documents: {str(e)}")
        return 0

def process_directory(directory_path):
    """Process all files in the specified directory and its subdirectories."""
    if not os.path.exists(directory_path):
        logger.error(f"Directory does not exist: {directory_path}")
        return []

    documents = []
    file_count = 0
    processed_count = 0

    logger.info(f"Processing files in directory: {directory_path}")

    for root, _, files in os.walk(directory_path):
        for file in files:
            file_path = os.path.join(root, file)
            file_count += 1

            # Process the file
            doc = process_file(file_path)
            if doc:
                documents.append(doc)
                processed_count += 1

    logger.info(f"Processed {processed_count} of {file_count} files successfully")
    return documents

def run_simple_test():
    """Run a simple test to verify the Elasticsearch indexing functionality."""
    logger.info("=== Starting simple test ===")
    
    # Test Elasticsearch connection
    connection_success, es = test_elasticsearch_connection()
    if not connection_success:
        logger.error("Test failed: Could not connect to Elasticsearch")
        return False
    
    # Process files in the input directory
    input_directory = os.path.join(os.getcwd(), "data")
    documents = process_directory(input_directory)
    
    if not documents:
        logger.warning("No valid documents found for indexing")
        return False
    
    # Index documents in Elasticsearch
    try:
        success, failed = index_documents(es, ES_CONFIG['index'], documents)
        logger.info(f"Indexed {success} documents in Elasticsearch")
        
        if failed:
            logger.warning(f"Failed to index {len(failed)} documents")
    except Exception as e:
        logger.error(f"Error indexing documents: {str(e)}")
        return False
    
    # Give Elasticsearch some time to process the documents
    logger.info("Waiting for Elasticsearch to process documents...")
    time.sleep(2)
    
    # Check if index exists
    if not check_index_exists(es, ES_CONFIG['index']):
        logger.error(f"Test failed: Index '{ES_CONFIG['index']}' was not created")
        return False
    
    # Count documents in the index
    doc_count = count_documents(es, ES_CONFIG['index'])
    if doc_count > 0:
        logger.info(f"Test passed: Successfully indexed {doc_count} documents using direct Elasticsearch client")
        return True
    else:
        logger.error("Test failed: No documents were indexed")
        return False

if __name__ == "__main__":
    try:
        success = run_simple_test()
        if success:
            logger.info("=== Simple test passed! The Elasticsearch indexing functionality is working correctly. ===")
            print("\n\n=== TEST RESULT: PASSED ===\n\n")
            sys.exit(0)
        else:
            logger.error("=== Simple test failed! The Elasticsearch indexing functionality is not working correctly. ===")
            print("\n\n=== TEST RESULT: FAILED ===\n\n")
            sys.exit(1)
    except Exception as e:
        logger.error(f"=== Unexpected error during test execution: {str(e)} ===")
        print("\n\n=== TEST RESULT: ERROR ===\n\n")
        sys.exit(2)