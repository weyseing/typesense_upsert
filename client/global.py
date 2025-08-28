import typesense
import ujson as json  # Faster JSON parsing
import time
import argparse
import os
import pandas as pd
import io
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class TypesenseClient:
    def __init__(self, collection_name, logger: logging.Logger | None = None):
        self.collection_name = collection_name
        api_key = os.getenv("TYPESENSE_API_KEY")
        host = os.getenv("TYPESENSE_ENDPOINT")
        port = os.getenv("TYPESENSE_PORT")
        protocol = "http"
        self.logger = logger or logging.getLogger()
        # Runtime metrics
        self._last_duration = None  # seconds of last operation
        self._last_found = None     # total 'found' from last search

        if not api_key:
            raise ValueError("TYPESENSE_API_KEY not found in environment variables or .env file.")

        try:
            self.client = typesense.Client({
                "api_key": api_key,
                "nodes": [{"host": host, "port": str(port), "protocol": protocol}],
                "connection_timeout_seconds": 300
            })
            self.collection = self.client.collections[self.collection_name]
            collection_info = self.collection.retrieve()
            self.default_sorting_field = collection_info.get("default_sorting_field")
            # Find the first indexed string field to use as a safe default for query_by
            self.first_string_field = None
            for field in collection_info.get("fields", []):
                if field.get("type") == "string" and field.get("index", False):
                    self.first_string_field = field.get("name")
                    break
        except Exception as e:
            self.logger.error(f"typesense init_error collection={self.collection_name} error={e}")
            self.client = None
            self.collection = None
            self.default_sorting_field = None
            self.first_string_field = None
            self._last_found = None

    def _log_execution_time(self, operation, start_time):
        """Log execution time for an operation."""
        execution_time = time.time() - start_time
        self._last_duration = execution_time
        self.logger.info("Execution Time=%.3fs", execution_time)

    def _cached_search(self, params_json):
        if not self.collection:
            return None
        params = json.loads(params_json)
        return self.collection.documents.search(params)

    def search(
        self,
        query,
        query_by="*",
        include_fields=None,
        filter_by=None,
        sort_by=None,
        group_by=None,
        facet_by=None,
        per_page=250,
        page=None,
        limit=None,
        max_facet_values=50,
        return_found: bool = False
    ):
        start_time = time.time()
        self._last_found = None  # reset for this invocation
        if not self.collection:
            return (pd.DataFrame(), None) if return_found else pd.DataFrame()

        auto_paginate = page is None
        if query == "*" and filter_by:
            if query_by == "*" and self.first_string_field:
                query_by = self.first_string_field
                self.logger.info(
                    "optimizing query: changing query_by to safe string field '%s' for match-all filter.",
                    query_by
                )
            if not sort_by and self.default_sorting_field:
                sort_by = f"{self.default_sorting_field}:asc"
                self.logger.info(
                    "optimizing query: setting sort_by to '%s' to disable relevancy ranking.",
                    sort_by
                )

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

        collected = []
        current_page = page or 1
        results = None

        while True:
            params_json = json.dumps(search_parameters, sort_keys=True)
            results = self._cached_search(params_json)
            if not results:
                break

            if self._last_found is None:
                self._last_found = results.get("found")
                
            is_grouped = "grouped_hits" in results
            if is_grouped:
                hits = results.get("grouped_hits", [])
                for group in hits:
                    group['documents'] = [doc['document'] for doc in group['hits']]
                    del group['hits']
                collected.extend(hits)
            else:
                hits = [hit.get("document", hit) for hit in results.get("hits", [])]
                collected.extend(hits)
            
            if not auto_paginate or (limit and len(collected) >= limit) or (len(hits) < per_page):
                if limit:
                    collected = collected[:limit]
                break
            
            current_page += 1
            search_parameters["page"] = current_page

        result_df = pd.DataFrame(collected)
        if facet_by and "facet_counts" in results:
            facet_data = []
            for count_data in results["facet_counts"]:
                facet_data.extend(count_data.get("counts", []))
            result_df = pd.DataFrame(facet_data)
        
        self._log_execution_time('search', start_time)
        self.logger.info(
            "action=search | collection=%s | query=\"%s\" | query_by=\"%s\" | filter_by=\"%s\" | sort_by=\"%s\" | group_by=\"%s\" | facet_by=\"%s\" | page=%s | per_page=%s | rows=%s | found=%s",
            self.collection_name,
            query,
            query_by,
            filter_by or "",
            sort_by or "",
            group_by or "",
            facet_by or "",
            page or ("all" if auto_paginate else 1),
            per_page,
            len(result_df),
            self._last_found if self._last_found is not None else ""
        )
        if return_found:
            return result_df, self._last_found
        return result_df

    # ----- Convenience accessors -----
    def last_found(self) -> int | None:
        """Return the 'found' count from the most recent search (or None)."""
        return self._last_found

    def get_by_id(self, document_id):
        start_time = time.time()
        if not self.collection:
            return pd.DataFrame()
        try:
            result_dict = self.collection.documents[str(document_id)].retrieve()
            df = pd.DataFrame([result_dict])
            self._log_execution_time('get_by_id', start_time)
            self.logger.info(f"action=get_by_id | collection={self.collection_name} | id={document_id} | rows=1")
            return df
        except Exception as e:
            self.logger.error(f"action=get_by_id | collection={self.collection_name} | id={document_id} | error={e}")
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
            documents = [json.loads(line) for line in export_response.splitlines() if line.strip()]
            df = pd.DataFrame(documents)
            self._log_execution_time('export', start_time)
            self.logger.info(
                "action=export | collection=%s | filter_by=\"%s\" | include=\"%s\" | exclude=\"%s\" | format=%s | rows=%s",
                self.collection_name, filter_by or '', include_fields or '', exclude_fields or '', (export_format or 'raw'), len(df)
            )

            if export_format:
                if not output_file:
                    timestamp = int(time.time())
                    output_file = f"typesense_export_{self.collection_name}_{timestamp}.{export_format}"

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
                    self.logger.error(f"[ERROR] Unsupported export format: {export_format}")
                
                if os.path.exists(output_file):
                    self.logger.info(f"[SUCCESS] Export to '{output_file}' complete.")
            
            return df
            
        except Exception as e:
            self.logger.error(f"action=export | collection={self.collection_name} | error={e}")
            self._log_execution_time('export', start_time)
            return pd.DataFrame()

def main():
    logger = logging.getLogger()

    parser = argparse.ArgumentParser(
        description="Typesense CLI: action first then options. Examples:\n  search --collection transaction --query *\n  export --collection transaction --format csv\n  get --collection transaction --id 123",
        formatter_class=argparse.RawTextHelpFormatter
    )

    # Positional action
    parser.add_argument("action", choices=["search", "export", "get"], help="Action to perform")

    # Common / later options
    parser.add_argument("--collection", required=True, help="Target Typesense collection name")
    parser.add_argument("--logger-name", help="Existing logger name to attach Typesense logs to")

    # Search options
    parser.add_argument("--query", default="*", help="Search query string (search)")
    parser.add_argument("--query_by", default="*", help="Fields to search (search)")
    parser.add_argument("--filter_by", help="Filter condition")
    parser.add_argument("--sort_by", help="Sort specification")
    parser.add_argument("--group_by", help="Group results by field (search)")
    parser.add_argument("--facet_by", help="Facet field (search)")
    parser.add_argument("--include_fields", help="Comma-separated fields to return / export")
    parser.add_argument("--limit", type=int, help="Max results to fetch (search)")
    parser.add_argument("--page", type=int, help="Specific page number (search)")
    parser.add_argument("--max_facet_values", type=int, default=50, help="Max facet values (search)")
    parser.add_argument("--per_page", type=int, default=250, help="Results per page (search)")

    # Get options
    parser.add_argument("--id", help="Document ID (get)")

    # Export options
    parser.add_argument("--exclude_fields", help="Exclude fields (export)")
    parser.add_argument("--format", choices=["csv", "json", "jsonl", "excel", "parquet"], help="Export file format (export)")
    parser.add_argument("--output", help="Output filename (export)")

    args = parser.parse_args()

    # Attach to provided logger if any
    if args.logger_name:
        logger = logging.getLogger(args.logger_name)
    logger.info("Typesense CLI Start")

    try:
        analyzer = TypesenseClient(collection_name=args.collection, logger=logger)
    except ValueError as e:
        logger.error(f"Configuration Error: {e}")
        return
    
    if not analyzer.client:
        return

    pd.set_option('display.max_rows', 100)
    pd.set_option('display.max_columns', 50)
    pd.set_option('display.width', 150)

    result_df = pd.DataFrame()

    if args.action == "search":
        if args.group_by and args.facet_by:
            logger.error("Cannot use both --group_by and --facet_by together")
            return
        result_df = analyzer.search(
            query=args.query,
            query_by=args.query_by,
            include_fields=args.include_fields,
            filter_by=args.filter_by,
            sort_by=args.sort_by,
            group_by=args.group_by,
            facet_by=args.facet_by,
            limit=args.limit,
            page=args.page,
            max_facet_values=args.max_facet_values,
            per_page=args.per_page
        )
    elif args.action == "get":
        if not args.id:
            logger.error("--id is required for get action")
            return
        result_df = analyzer.get_by_id(args.id)
    elif args.action == "export":
        result_df = analyzer.export(
            filter_by=args.filter_by,
            include_fields=args.include_fields,
            exclude_fields=args.exclude_fields,
            export_format=args.format,
            output_file=args.output
        )

    if not result_df.empty:
        print(result_df)
    else:
        logger.info(f"[{args.action}] no_results")

if __name__ == "__main__":
    main()