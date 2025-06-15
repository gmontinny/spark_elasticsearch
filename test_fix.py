import sys
import logging
from src.main import advanced_search_command

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Test the advanced search with the parameters from the error message
try:
    print("Testing advanced search with parameters from error message...")
    advanced_search_command(
        query_text="Controladoria Geral",
        file_type="pdf",
        min_size=1000,
        sort_by="file_name",
        sort_order="desc"
    )
    print("\nTest completed successfully!")
except Exception as e:
    print(f"\nError during test: {str(e)}")
    sys.exit(1)