import json
import logging
import os  # <-- Import the os module
import time
from typing import Any, Dict, List

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# --- Constants ---
API_POOL_NAME = "api_processing_pool"

# --- Configuration from Environment Variable ---
# Use os.getenv to read the S3 bucket name from an environment variable.
# Provide a fallback default if the variable is not set.
S3_BUCKET_NAME = os.getenv("DATA_PIPELINE_S3_BUCKET", "container-network-etl")

# --- Task Definitions ---
@task(task_id="fetch_and_upload_to_s3")
def fetch_and_upload_to_s3() -> Dict[str, Any]:
    # ... (The rest of this task's code remains exactly the same) ...
    # It will automatically use the S3_BUCKET_NAME defined above.
    
    # --- Part 1: Fetch all containers from the API (same as before) ---
    try:
        tl_url = Variable.get("TL_URL")
        access_key = Variable.get("PC_IDENTITY")
        access_secret = Variable.get("PC_SECRET")
    except KeyError as e:
        raise AirflowException(f"Missing required Airflow Variable: {e}")

    auth_url = f"{tl_url}/api/v1/authenticate"
    auth_body = {"username": access_key, "password": access_secret}
    response = requests.post(auth_url, json=auth_body, timeout=60, verify=False)
    response.raise_for_status()
    token = response.json().get("token")

    containers_url = f"{tl_url}/api/v1/containers"
    headers = {"Authorization": f"Bearer {token}"}
    all_containers = []
    offset = 0
    limit = 100
    while True:
        params = {"offset": offset, "limit": limit}
        response = requests.get(containers_url, headers=headers, params=params, timeout=60, verify=False)
        response.raise_for_status()
        containers_page = response.json()
        if not containers_page:
            break
        all_containers.extend(containers_page)
        if len(containers_page) < limit:
            break
        offset += limit
    
    logging.info(f"Finished fetching all containers. Total found: {len(all_containers)}")

    if not all_containers:
        logging.warning("No containers found. Downstream tasks will be skipped.")
        return {"s3_uri": "", "indices": []}

    # --- Part 2: Manually upload the data to S3 ---
    s3_hook = S3Hook()
    data_string = json.dumps(all_containers)
    s3_key = f"container_data/run_{{{{ ts_nodash }}}}.json"
    
    logging.info(f"Uploading data to s3://{S3_BUCKET_NAME}/{s3_key}")
    s3_hook.load_string(
        string_data=data_string,
        key=s3_key,
        bucket_name=S3_BUCKET_NAME,
        replace=True,
    )
    
    s3_uri = f"s3://{S3_BUCKET_NAME}/{s3_key}"
    return {"s3_uri": s3_uri, "indices": list(range(len(all_containers)))}


@task(task_id="process_container_from_s3", pool=API_POOL_NAME)
def process_container_from_s3(s3_uri: str, container_index: int) -> Dict[str, Any]:
    # ... (This task's code remains exactly the same) ...
    if not s3_uri:
        return {}

    s3_hook = S3Hook()
    bucket, key = s3_hook.parse_s3_url(s3_uri)
    
    # Each mapped task downloads the full file
    data_string = s3_hook.read_key(key=key, bucket_name=bucket)
    all_containers = json.loads(data_string)
    container = all_containers[container_index]

    # Process the specific container
    container_id = container.get("_id")
    open_ports = []
    network = container.get("network", {})
    for port in network.get("ports", []):
        open_ports.append({"port": port.get("container"), "type": "network"})
    
    if open_ports:
        return {"id": container_id, "open_ports": open_ports}
    return {}

@task(task_id="load_network_info")
def load_network_info_task(all_container_info: List[Dict[str, Any]]):
    # ... (This task's code remains exactly the same) ...
    count = sum(1 for info in all_container_info if info)
    logging.info(f"--- Aggregated Results ---")
    logging.info(f"Total containers with network info processed: {count}")

@task
def cleanup_s3_file_task(s3_uri: str):
    # ... (This task's code remains exactly the same) ...
    if not s3_uri:
        logging.info("No S3 URI provided, skipping cleanup.")
        return
    
    s3_hook = S3Hook()
    logging.info(f"Cleaning up S3 object: {s3_uri}")
    bucket, key = s3_hook.parse_s3_url(s3_uri)
    s3_hook.delete_objects(bucket=bucket, keys=key)

# --- DAG Definition ---
@dag(
    dag_id="container_network_etl_manual_s3",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    doc_md="""
    ### Container ETL DAG with Manual S3 Data Passing
    This DAG fetches a large dataset, manually uploads it to S3, and passes
    the S3 URI to downstream tasks that process the data in parallel.
    """,
    tags=["api", "refactor", "s3", "manual"],
)
def container_network_etl_dag():
    # ... (This function's code remains exactly the same) ...
    s3_references = fetch_and_upload_to_s3()
    
    processed_containers = process_container_from_s3.partial(
        s3_uri=s3_references["s3_uri"]
    ).expand(container_index=s3_references["indices"])
    
    load_op = load_network_info_task(all_container_info=processed_containers)
    
    cleanup_op = cleanup_s3_file_task(s3_uri=s3_references["s3_uri"])
    
    load_op >> cleanup_op

container_network_etl_dag()
