import os
import logging
import typesense

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_payment_collection():
    try:
        client = typesense.Client({
            'nodes': [{'host': os.environ.get('TYPESENSE_ENDPOINT'), 'port': os.environ.get('TYPESENSE_PORT'), 'protocol': 'http' }],
            'api_key': os.environ.get('TYPESENSE_API_KEY'),
            'connection_timeout_seconds': 60
        })

        existing_collections = client.collections.retrieve()
        collection_names = [col['name'] for col in existing_collections]

        if 'transaction' in collection_names:
            logging.info("✅ Collection already exists!")
            return

        schema = {
            'name': 'transaction',
            'fields': [
                {'name': 'id', 'type': 'string', 'facet': False, 'optional': False},
                {'name': 'TRANID', 'type': 'int64', 'index': True, 'sort': True, 'optional': False},
                {'name': 'ORDER_ID', 'type': 'string', 'index': True, 'sort': True, 'optional': True},
                {'name': 'BILL_AMT', 'type': 'float', 'optional': True},
                {'name': 'CUR_ACTUAL', 'type': 'string', 'facet': True, 'optional': True},
                {'name': 'ACTUAL_AMT', 'type': 'float', 'optional': True},
                {'name': 'STATUS', 'type': 'string', 'facet': True, 'optional': True},
                {'name': 'TRANKEY', 'type': 'string', 'index': True, 'optional': True},
                {'name': 'CREATE_DATE', 'type': 'int64', 'sort': True, 'optional': True},
                {'name': 'CHARGEBACK_DATE', 'type': 'int64', 'sort': True, 'optional': True},
                {'name': 'PAID_DATE', 'type': 'int64', 'sort': True, 'optional': True},
                {'name': 'CHANNEL', 'type': 'string', 'facet': True, 'optional': True},
                {'name': 'MERCHANTID', 'type': 'string', 'index': True, 'facet': True, 'optional': True},
                {'name': 'BILLING_NAME', 'type': 'string', 'optional': True},
                {'name': 'BILLING_EMAIL', 'type': 'string', 'optional': True},
                {'name': 'BILLING_MOBILE', 'type': 'string', 'optional': True},
                {'name': 'BILLING_INFO', 'type': 'string', 'optional': True},
                {'name': 'APP_CODE', 'type': 'string', 'optional': True},
                {'name': 'STATUS_DESC', 'type': 'string', 'optional': True},
                {'name': 'REFUND_AMT', 'type': 'float', 'optional': True},
                {'name': 'HISTORY', 'type': 'string', 'optional': True},
                {'name': 'BIN', 'type': 'int32', 'optional': True},
                {'name': 'IP', 'type': 'string', 'facet': True, 'optional': True},
                {'name': 'DEF_AMT', 'type': 'float', 'optional': True}
            ],
            'default_sorting_field': 'TRANID'
        }

        client.collections.create(schema)
        logging.info("✅ Collection created successfully!")

    except Exception as e:
        logging.error(f"❌ Failed to create collection 'payment': {e}", exc_info=True)

if __name__ == '__main__':
    create_payment_collection()
