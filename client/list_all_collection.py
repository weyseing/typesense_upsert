import os
import typesense
import logging

# configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# client
client = typesense.Client({
    'api_key': os.getenv('TYPESENSE_API_KEY'),
    'nodes': [{'host':  os.getenv('TYPESENSE_ENDPOINT'), 'port':  os.getenv('TYPESENSE_PORT'), 'protocol': 'http'}],
    'connection_timeout_seconds': 60
})
# retrieve
try:
    all_collections = client.collections.retrieve()
    logging.info("Typesense Collections and Document Counts:")
    logging.info("-" * 50)
    for collection in all_collections:
        name = collection.get('name', 'N/A')
        num_docs = collection.get('num_documents', 'N/A')
        logging.info(f"Collection: {name:<25} Documents: {num_docs}")
    logging.info("-" * 50)
except Exception as e:
    logging.info(f"An error occurred: {e}")