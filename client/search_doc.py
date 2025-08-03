import os
import time
import logging
import argparse
import typesense
import pandas as pd

# configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# client
client = typesense.Client({
    'api_key': os.getenv('TYPESENSE_API_KEY'),
    'nodes': [{'host':  os.getenv('TYPESENSE_ENDPOINT'), 'port':  os.getenv('TYPESENSE_PORT'), 'protocol': 'http'}],
    'connection_timeout_seconds': 2
})

def typesense_connect():
    try:
        collections = client.collections.retrieve()
        logging.info(f"Connected! Found {len(collections)} collections")
    except Exception as e:
        logging.error(f"Connection failed: {e}")
        exit(1)

def split_by_pipe(arg_string):
    return arg_string.split('|')

def query_typesense_documents(collection_name, search_query, query_by=None, limit=10, filter_string=None, sort_by=None, include_fields=None):
    all_hits = [] 
    page = 1
    typesense_per_page_limit = 250
    total_found_docs_from_typesense = 0
    
    logging.info(f"Starting to fetch up to {limit} documents from Typesense...")
    start_time_fetch = time.time()
    try:
        while True:
            # check if hit limit
            remaining_to_fetch = limit - len(all_hits)
            if remaining_to_fetch <= 0:
                logging.info(f"Reached desired limit of {limit} documents. Stopping fetch.")
                break

            # parameters
            current_per_page = min(typesense_per_page_limit, remaining_to_fetch)
            search_parameters = {
                'q': search_query,
                'per_page': current_per_page,
                'page': page,
            }
            if include_fields:
                search_parameters['include_fields'] = ",".join(include_fields)
            if filter_string:
                search_parameters['filter_by'] = filter_string
            if sort_by:
                search_parameters['sort_by'] = sort_by
            if query_by:
                search_parameters['query_by'] = ','.join(query_by_fields)

            # search doc
            logging.info(f"Fetching page {page} with per_page={current_per_page}")
            results = client.collections[collection_name].documents.search(search_parameters)
            
            # record total count
            if page == 1: 
                total_found_docs_from_typesense = results['found']
            
            # break if not found
            if not results['hits']:
                logging.info(f"Fetched 0 documents on page {page}. No more documents available from Typesense.")
                break 

            # append hits
            all_hits.extend(results['hits']) 

            # last page
            if len(results['hits']) < current_per_page:
                logging.info(f"Fetched {len(results['hits'])} documents on page {page}. Last page has fewer than requested, indicating end of results.")
                break

            page += 1
            logging.info(f"Fetched {len(results['hits'])} documents on page {page-1}. Total hits fetched so far: {len(all_hits)}")

        # log time
        end_time_fetch = time.time()
        logging.info(f"Finished fetching {len(all_hits)} documents in {end_time_fetch - start_time_fetch:.2f} seconds.")
        
        # result
        final_results = {
            'found': total_found_docs_from_typesense,
            'hits': all_hits
        }
        return final_results

    except Exception as e:
        logging.error(f"Error querying Typesense during pagination: {e}")
        return None

if __name__ == "__main__":
    # parse arg
    parser = argparse.ArgumentParser()
    parser.add_argument('--include', type=split_by_pipe, default=None, help='Selected fields (e.g., "id|amount|status").')
    parser.add_argument('--filter', type=str, default=None, help='The filter query string (e.g., "transaction_id:[60000..60999] && currency:JPY").')
    parser.add_argument('--limit', type=int, default=10, help='The maximum number of documents to fetch.')
    parser.add_argument('--sort-by', type=str, default=None, help='Sorting order (e.g., "transaction_id:desc,amount:asc").')
    parser.add_argument('--query-by', type=split_by_pipe, required=False, default=None,help='The fields to search against (e.g., "name|description|tags").')
    args = parser.parse_args()

    # connect
    typesense_connect()

    collection_name = 'transaction'
    search_query = '*' 
    query_by_fields = [] 

    # search
    logging.info(f"Starting to fetch up to {args.limit} documents from Typesense...")
    results = query_typesense_documents(
        collection_name=collection_name,
        search_query=search_query,
        query_by=args.query_by,
        limit=args.limit,
        filter_string=args.filter,
        sort_by=args.sort_by,
        include_fields=args.include
    )

    # display
    if results and results['hits']:
        documents_list = [hit['document'] for hit in results['hits']]
        df = pd.DataFrame(documents_list)
        logging.info(f"\nTotal documents found (matching query and limit): {len(df)}") 
        logging.info(f"Displaying latest 10 documents:\n{df.head(10)}")