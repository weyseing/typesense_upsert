import time
import typesense
from typesense.exceptions import ObjectNotFound


def log_process_time(logger, start_time, log_msg):
    process_time = time.time() - start_time
    logger.info(f"{log_msg} in {process_time:.2f}sec")

def check_and_create_collection(logger, process_id, client, collection_name, schema):
    try:
        # check collection exist
        try:
            start_time = time.time()
            client.collections[collection_name].retrieve()
            log_process_time(logger, start_time, f"[PID:{process_id}] Checked collection '{collection_name}' already exists")
            return
        except ObjectNotFound:
            log_process_time(logger, start_time, f"[PID:{process_id}] Checked collection '{collection_name}' NOT exists")

        # create collection
        start_time = time.time()
        client.collections.create(schema)
        log_process_time(logger, start_time, f"[PID:{process_id}] Collection '{collection_name}' created successfully")
    except Exception as e:
        raise

def delete_old_collection(logger, process_id, client, collection_name):
    try:
        start_time = time.time()
        client.collections[collection_name].delete()
        log_process_time(logger, start_time, f"[PID:{process_id}] Collection '{collection_name}' deleted successfully")
    except ObjectNotFound:
        log_process_time(logger, start_time, f"[PID:{process_id}] Collection '{collection_name}' NOT found, skipping delete")