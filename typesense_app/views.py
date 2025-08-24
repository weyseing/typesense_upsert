import os
import json
import time
import uuid
import struct
import base64
import logging
import typesense
import subprocess
from decimal import Decimal
from datetime import datetime
from django.shortcuts import render
from collections import defaultdict
from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from rest_framework.decorators import api_view,authentication_classes

from framework.authentication.api_key_auth import TypesenseKeyAuth

# logger
logger = logging.getLogger(__name__)

# typesense client
client = typesense.Client({
    'nodes': [{'host': os.environ.get('TYPESENSE_ENDPOINT'), 'port': os.environ.get('TYPESENSE_PORT'), 'protocol': 'http' }],
    'api_key': os.environ.get('TYPESENSE_API_KEY'),
    'connection_timeout_seconds': 300
})

def healthcheck(request):
    return JsonResponse({'status': 'ok'})

def avro_decimal_from_base64(b64, scale):
    raw_bytes = base64.b64decode(b64)
    int_value = int.from_bytes(raw_bytes, byteorder='big', signed=True)
    return Decimal(int_value).scaleb(-scale)

def log_process_time(start_time, log_msg):
    process_time = time.time() - start_time
    logger.info(f"{log_msg} in {process_time:.2f}sec")

def handle_response(process_id, total_start_time, processed_count, errors, error_message=None, status_code=200):
    # log time
    total_response_time = time.time() - total_start_time
    log_message = f'[PID:{process_id}] Total response time: Completed {processed_count} document(s) in {total_response_time:.2f}sec'

    # log
    logger.info(log_message)
        
    # response
    response_data = {'response_time': f'{total_response_time:.2f} seconds'}

    if errors:
        response_data['status'] = 'partial_success'
        response_data['message'] = log_message
        response_data['errors'] = errors
        status_code = 207
    elif error_message:
        response_data['status'] = 'error'
        response_data['message'] = error_message
        status_code = 500
    else:
        response_data['status'] = 'ok'
        response_data['message'] = log_message
        status_code = 200
    return JsonResponse(response_data, status=status_code)


@csrf_exempt
@api_view(['POST'])
@authentication_classes([TypesenseKeyAuth])
def transaction(request):
    process_id = uuid.uuid4()
    total_start_time = time.time()
    processed_count = 0
    errors = []

    # sharding dict
    doc_upsert_month = defaultdict(list)

    try:
        payloads_list = json.loads(request.body)
        
        # pre-process doc
        start_time = time.time()
        for payload in payloads_list:
            document_to_insert = payload.copy()

            # month key
            create_date_timestamp = document_to_insert.get('CREATE_DATE')
            dt_obj = datetime.fromtimestamp(create_date_timestamp / 1000)
            year_month = dt_obj.strftime('%Y%m')
            
            # doc ID
            document_id = str(payload['TRANID'])
            document_to_insert['id'] = document_id
            
            # decode base64 & convert float
            fields_to_convert = ['BILL_AMT', 'ACTUAL_AMT', 'REFUND_AMT', 'DEF_AMT', 'CUR_AMT', 'TRANSACTION_COST', 'CHANNEL_COST']
            for field in fields_to_convert:
                if field in document_to_insert and isinstance(document_to_insert[field], str):
                    document_to_insert[field] = float(avro_decimal_from_base64(document_to_insert[field], 2))
            
            # append to sharding dict
            doc_upsert_month[year_month].append(document_to_insert)
        log_process_time(start_time, f"[PID:{process_id}] Completed pre-processing {len(payloads_list)} docs")

        # sharding config
        sharding_configs = {
            "YYYYMM": {"data": doc_upsert_month, "prefix": "transaction_month__"}
        }

        # import upsert
        for config_name, config in sharding_configs.items():
            shard_start_time = time.time()
            documents_to_upsert_dict = config["data"]
            collection_prefix = config["prefix"]
            
            for sharding_key, documents_to_upsert in documents_to_upsert_dict.items():
                collection_name = collection_prefix + sharding_key
                start_time = time.time()
                response = client.collections[collection_name].documents.import_(documents_to_upsert, {'action': 'upsert'})
                for doc_response in response:
                    if doc_response['success']:
                        processed_count += 1
                    else:
                        errors.append(f"Failed to upsert document: {doc_response.get('error')}")
                log_process_time(start_time, f"[PID:{process_id}] Completed upsert SINGLE-collection ({collection_name})")
            log_process_time(shard_start_time, f"[PID:{process_id}] Completed upsert SHARD-collection ({config_name})")

        # response
        return handle_response(process_id, total_start_time=total_start_time, processed_count=processed_count, errors=errors, error_message=None, status_code=200)

    except Exception as e:
        error_message = str(e)
        logger.error(f"ERROR: {error_message}", exc_info=True)
        return handle_response(process_id, total_start_time=total_start_time, processed_count=processed_count, errors=errors, error_message=error_message, status_code=500)

