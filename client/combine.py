import typesense
import json
import time
import argparse
import os
import pandas as pd
from dotenv import load_dotenv
import io

class TypesenseClient:
    def __init__(self, collection_name):
        load_dotenv()
        self.collection_name = collection_name
        api_key = os.getenv("TYPESENSE_API_KEY")
        host = os.getenv("TYPESENSE_ENDPOINT")
        port = os.getenv("TYPESENSE_PORT")
        protocol = "http"

        if not api_key:
            raise ValueError("TYPESENSE_API_KEY not found in environment variables or .env file.")

        try:
            self.client = typesense.Client({
                "api_key": api_key,
                "nodes": [{"host": host, "port": str(port), "protocol": protocol}],
                "connection_timeout_seconds": 5
            })
            self.collection = self.client.collections[self.collection_name]
        except Exception as e:
            print(f"[ERROR] Error initializing Typesense client: {e}")
            self.client = None
            self.collection = None

    def _log_execution_time(self, operation, start_time):
        """Log execution time for an operation."""
        execution_time = time.time() - start_time
        print(f"[INFO] Operation '{operation}' completed in {execution_time:.3f} seconds.")

    def _cached_search(self, params_json):
        if not self.collection:
            return None
        params = json.loads(params_json)
        return self.collection.documents.search(params)

    def search(
        self,
        query,
        query_by,
        include_fields=None,
        filter_by=None,
        sort_by=None,
        group_by=None,
        facet_by=None,
        per_page=250,
        page=None,
        limit=None,
        max_facet_values=50
    ):
        start_time = time.time()
        if not self.collection:
            return pd.DataFrame()

        auto_paginate = page is None

        search_parameters = {
            "q": query,
            "query_by": query_by,
            "per_page": per_page,
            "page": page or 1
        }
        if include_fields:
            search_parameters["include_fields"] = include_fields
        if filter_by:
            search_parameters["filter_by"] = filter_by
        if sort_by:
            search_parameters["sort_by"] = sort_by
        if group_by:
            search_parameters["group_by"] = group_by
        if facet_by:
            search_parameters["facet_by"] = facet_by
            search_parameters["max_facet_values"] = max_facet_values
            search_parameters["per_page"] = 0

        all_hits = []
        current_page = page or 1

        while True:
            params_json = json.dumps(search_parameters, sort_keys=True)
            results = self._cached_search(params_json)

            if not results:
                break
                
            is_grouped = "grouped_hits" in results
            if is_grouped:
                hits = results.get("grouped_hits", [])
                for group in hits:
                    group['documents'] = [doc['document'] for doc in group['hits']]
                    del group['hits']
                all_hits.extend(hits)
            else:
                hits = [hit.get("document", hit) for hit in results.get("hits", [])]
                all_hits.extend(hits)
            
            if not auto_paginate or (limit and len(all_hits) >= limit) or (len(hits) < per_page):
                if limit:
                    all_hits = all_hits[:limit]
                break
            
            current_page += 1
            search_parameters["page"] = current_page

        result_df = pd.DataFrame(all_hits)
        if facet_by and "facet_counts" in results:
            facet_data = []
            for count_data in results["facet_counts"]:
                facet_data.extend(count_data.get("counts", []))
            result_df = pd.DataFrame(facet_data)
        
        self._log_execution_time('search', start_time)
        return result_df

    def get_by_id(self, document_id):
        start_time = time.time()
        if not self.collection:
            return pd.DataFrame()
        try:
            result_dict = self.collection.documents[str(document_id)].retrieve()
            df = pd.DataFrame([result_dict])
            self._log_execution_time('get_by_id', start_time)
            return df
        except typesense.exceptions.ObjectNotFound:
            self._log_execution_time('get_by_id', start_time)
            return pd.DataFrame()
        except Exception as e:
            print(f"[ERROR] Get by ID failed: {e}")
            self._log_execution_time('get_by_id', start_time)
            return pd.DataFrame()

    def export(self, filter_by=None, include_fields=None, exclude_fields=None, export_format=None, output_file=None):
        start_time = time.time()
        if not self.collection:
            return pd.DataFrame()
        
        export_params = {}
        if filter_by:
            export_params["filter_by"] = filter_by
        if include_fields:
            export_params["include_fields"] = include_fields
        if exclude_fields:
            export_params["exclude_fields"] = exclude_fields
        
        try:
            export_response = self.collection.documents.export(export_params)
            
            if not export_response.strip():
                self._log_execution_time('export', start_time)
                return pd.DataFrame()

            df = pd.read_json(io.StringIO(export_response), lines=True)

            if export_format:
                if not output_file:
                    timestamp = int(time.time())
                    output_file = f"typesense_export_{self.collection_name}_{timestamp}.{export_format}"
                
                print(f"[INFO] Saving {len(df)} records to '{output_file}'...")
                
                if export_format.lower() == "jsonl":
                    with open(output_file, 'w') as f:
                        f.write(export_response)
                elif export_format.lower() == "csv":
                    df.to_csv(output_file, index=False)
                elif export_format.lower() == "json":
                    df.to_json(output_file, orient='records', indent=2)
                elif export_format.lower() == "excel":
                    df.to_excel(output_file, index=False)
                elif export_format.lower() == "parquet":
                    df.to_parquet(output_file, index=False)
                else:
                    print(f"[ERROR] Unsupported export format: {export_format}")
                
                if os.path.exists(output_file):
                    print(f"[SUCCESS] Export to '{output_file}' complete.")

            self._log_execution_time('export', start_time)
            return df
            
        except Exception as e:
            print(f"[ERROR] Export failed: {e}")
            self._log_execution_time('export', start_time)
            return pd.DataFrame()


def main():
    parser = argparse.ArgumentParser(
        description="A user-friendly command-line toolkit for Typesense.",
        formatter_class=argparse.RawTextHelpFormatter
    )

    # Argument definitions
    base_group = parser.add_argument_group('BASE ARGUMENTS')
    base_group.add_argument("--collection", required=True, help="Target Typesense collection user_id.")
    subparsers = parser.add_subparsers(dest="action", required=True, help="The action to perform.")
    search_parser = subparsers.add_parser("search", help="Perform a search query.")
    search_parser.add_argument("--query", default="*", help="The search query string. (Default: '*')")
    search_parser.add_argument("--query_by", default="*", help="Comma-separated fields to search in according to rank. (Default: '*')")
    search_parser.add_argument("--filter_by", help="Filter condition (e.g., 'amount:>100').")
    search_parser.add_argument("--sort_by", help="Sorting parameters (e.g., 'amount:desc').")
    search_parser.add_argument("--group_by", help="Field to group results by.")
    search_parser.add_argument("--facet_by", help="Field to generate facet counts for.")
    search_parser.add_argument("--include_fields", help="Comma-separated fields to return.")
    search_parser.add_argument("--limit", type=int, help="Max results to fetch. Enables auto-pagination.")
    search_parser.add_argument("--page", type=int, help="Fetch a specific page number. Disables auto-pagination.")
    search_parser.add_argument("--max_facet_values", type=int, default=50)
    get_parser = subparsers.add_parser("get", help="Fetch a single document by its unique ID.")
    get_parser.add_argument("id", help="The exact ID of the document to retrieve.")
    export_parser = subparsers.add_parser("export", help="Export data from a collection.")
    export_parser.add_argument("--filter_by", help="Filter condition (e.g., 'amount:>100').")
    export_parser.add_argument("--include_fields", help="Comma-separated fields to include in export.")
    export_parser.add_argument("--exclude_fields", help="Comma-separated fields to exclude from export.")
    export_parser.add_argument("--format", choices=["csv", "json", "jsonl", "excel", "parquet"], help="If specified, saves the export to a file of this format.")
    export_parser.add_argument("--output", help="Output filename. (Default: auto-generated)")
    args = parser.parse_args()

    try:
        analyzer = TypesenseClient(collection_name=args.collection)
    except ValueError as e:
        print(f"Configuration Error: {e}")
        return
    
    if not analyzer.client:
        return

    pd.set_option('display.max_rows', 100)
    pd.set_option('display.max_columns', 50)
    pd.set_option('display.width', 150)

    result_df = pd.DataFrame()

    if args.action == "search":
        if args.group_by and args.facet_by:
            print("[ERROR] Cannot use both --group_by and --facet_by. Choose one.")
            return
        
        result_df = analyzer.search(
            query=args.query, query_by=args.query_by, include_fields=args.include_fields,
            filter_by=args.filter_by, sort_by=args.sort_by, group_by=args.group_by,
            facet_by=args.facet_by, limit=args.limit, page=args.page, max_facet_values=args.max_facet_values
        )
            
    elif args.action == "get":
        result_df = analyzer.get_by_id(args.id)

    elif args.action == "export":
        result_df = analyzer.export(
            filter_by=args.filter_by, include_fields=args.include_fields, exclude_fields=args.exclude_fields,
            export_format=args.format, output_file=args.output
        )

    if not result_df.empty:
        print(result_df)
    else:
        print("No results found or an error occurred.")

if __name__ == "__main__":
    main()