import os
import json
import time
import logging
import argparse
import typesense
import pandas as pd
import ujson as json

# logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Typesense client
client = typesense.Client({
    'api_key': os.getenv('TYPESENSE_API_KEY'),
    'nodes': [{'host':  os.getenv('TYPESENSE_ENDPOINT'), 'port':  os.getenv('TYPESENSE_PORT'), 'protocol': 'http'}],
    'connection_timeout_seconds': 2
})

def typesense_connect():
    try:
        collections = client.collections.retrieve()
        logging.info(f"Connected to Typesense! Found {len(collections)} collections.")
    except Exception as e:
        logging.error(f"Connection to Typesense failed: {e}")
        exit(1)

def split_by_pipe(arg_string):
    return arg_string.split('|')

def export_typesense_documents(include_fields=None, filter_by=None):
    logging.info(f"Starting export from collection...")
    start_time_export = time.time()
    
    # parameter
    export_parameters = {}
    if include_fields:
        export_parameters['include_fields'] = ",".join(include_fields)
    if filter_by:
        export_parameters['filter_by'] = filter_by

    try:
        # Export doc
        start_time_fetch = time.time()
        exported_data = client.collections["transaction"].documents.export(export_parameters)
        end_time_export = time.time()
        logging.info(f"Typesense export (fetch) completed in {end_time_export - start_time_fetch:.2f} seconds.")

        # Process doc
        documents = [json.loads(line) for line in exported_data.splitlines() if line.strip()]
        end_time_processing = time.time()
        logging.info(f"Finished process {len(documents)} documents in {end_time_processing - start_time_fetch:.2f} seconds.")

        if documents:
            return pd.DataFrame(documents)
        else:
            return pd.DataFrame() 
            
    except Exception as e:
        logging.error(f"Error exporting documents from Typesense: {e}")
        return None

if __name__ == "__main__":
    # Parse arg
    parser = argparse.ArgumentParser()
    parser.add_argument('--include', type=split_by_pipe, default=None, help='Selected fields (e.g., "id|amount|status").')
    parser.add_argument('--filter', type=str, default=None, help='Filter query string (e.g., "transaction_id:[60000..60999] && currency:JPY").')
    args = parser.parse_args()

    # Connect
    typesense_connect()

    # Export documents
    df_exported = export_typesense_documents(include_fields=args.include, filter_by=args.filter)

    # Display
    if df_exported is not None:
        logging.info(f"\nTotal documents exported: {len(df_exported)}")
        logging.info(f"Displaying last 10 exported documents:\n{df_exported.tail(10)}")