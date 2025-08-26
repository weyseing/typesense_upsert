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
    collection_prefix = 'status_count_mins_month__'

    # delete old collection
    old_collection_name = collection_prefix + two_months_ago_str
    func_collection.delete_old_collection(logger, process_id, client, old_collection_name)

    # check & create collection
    months_to_check = [last_month_str, current_month_str, next_month_str]
    for month_str in months_to_check:
        collection_name = collection_prefix + month_str
        schema = {
            "name": collection_name,
            "fields": [
                {"name": "id", "type": "string", "facet": False},
                {"name": "MERCHANTID", "type": "string", "index": True, "facet": True},
                {"name": "CHANNEL", "type": "string", "index": True, "facet": True},
                {"name": "L_VERSION", "type": "string", "index": True, "facet": True},
                {"name": "UPDATE_DATE", "type": "int64", "sort": True, 'index': True},
                {"name": "WINDOW_START", "type": "int64", "sort": True, 'index': True},
                {"name": "WINDOW_END", "type": "int64", "sort": True},
                {"name": "COUNT_AUTHORIZED", "type": "int32"},
                {"name": "COUNT_CAPTURED", "type": "int32"},
                {"name": "COUNT_HOLD", "type": "int32"},
                {"name": "COUNT_CHARGEBACK", "type": "int32"},
                {"name": "COUNT_CANCELLED", "type": "int32"},
                {"name": "COUNT_BLOCKED", "type": "int32"},
                {"name": "COUNT_FAILED", "type": "int32"},
                {"name": "COUNT_SETTLED", "type": "int32"},
                {"name": "COUNT_REQCANCEL", "type": "int32"},
                {"name": "COUNT_UNKNOWN", "type": "int32"},
                {"name": "COUNT_PENDING", "type": "int32"},
                {"name": "COUNT_RELEASE", "type": "int32"},
                {"name": "COUNT_REJECT", "type": "int32"},
                {"name": "COUNT_TESTOK", "type": "int32"},
                {"name": "COUNT_REQCHARGEBACK", "type": "int32"},
                {"name": "BILL_AMT", "type": "float"},
                {"name": "CURRENCY", "type": "string", "index": True, "facet": True}
            ],
            "default_sorting_field": "WINDOW_START"
        }
        func_collection.check_and_create_collection(logger, process_id, client, collection_name, schema)
    
if __name__ == "__main__":
    main()