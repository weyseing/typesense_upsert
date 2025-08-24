import os
import time
import uuid
import logging
import calendar
import typesense
from datetime import date, timedelta
from watchtower import CloudWatchLogHandler
from typesense.exceptions import ObjectNotFound
from dateutil.relativedelta import relativedelta

from utils import func_collection

# logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
if os.environ.get('ENVIRONMENT') == 'PRODUCTION':
    logger.addHandler(CloudWatchLogHandler(log_group='/ecs/typesense', stream_name='manage_collection'))

def main():
    process_id = uuid.uuid4()

    # typesense client
    client = typesense.Client({
        'nodes': [{'host': os.environ.get('TYPESENSE_ENDPOINT'), 'port': os.environ.get('TYPESENSE_PORT'), 'protocol': 'http' }],
        'api_key': os.environ.get('TYPESENSE_API_KEY'),
        'connection_timeout_seconds': 600
    })

    # month keys
    today = date.today()
    two_months_ago = today + relativedelta(months=-2)
    two_months_ago_str = two_months_ago.strftime('%Y%m')
    last_month_day = today.replace(day=1) - timedelta(days=1)
    last_month_str = last_month_day.strftime('%Y%m')
    current_month_str = today.strftime('%Y%m')
    next_month = today + relativedelta(months=1)
    next_month_str = next_month.strftime('%Y%m')

    # collection prefix
    collection_prefix = 'transaction_month__'

    # delete old collection
    old_collection_name = collection_prefix + two_months_ago_str
    func_collection.delete_old_collection(logger, process_id, client, old_collection_name)

    # check & create collection
    months_to_check = [last_month_str, current_month_str, next_month_str]
    for month_str in months_to_check:
        collection_name = collection_prefix + month_str
        schema = {
            'name': collection_name,
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
        func_collection.check_and_create_collection(logger, process_id, client, collection_name, schema)
    
if __name__ == "__main__":
    main()