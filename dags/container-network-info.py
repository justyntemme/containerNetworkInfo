import datetime
import json
import logging
import os
import time
from typing import Any, Dict, List, Tuple

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# --- Constants ---
API_POOL_NAME = "api_processing_pool"
S3_BUCKET_NAME = os.getenv("DATA_PIPELINE_S3_BUCKET", "your-default-bucket-name")
CHUNK_SIZE = 10000  # Process 10,000 containers per parallel task

# --- Task Definitions ---
@task(task_id="fetch_and_upload_to_s3")
def fetch_and_upload_to_s3() -> str:
    """
    Fetches all container data, uploads it to S3, and returns the S3 URI.
    """
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
    rate_limit_count = 30
    rate_limit_period_seconds = 30
    request_count = 0
    start_time = time.time()
    while True:
        if request_count >= rate_limit_count:
            elapsed_time = time.time() - start_time
            if elapsed_time < rate_limit_period_seconds:
                sleep_time = rate_limit_period_seconds - elapsed_time
                logging.info(f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds.")
                time.sleep(sleep_time)
            request_count = 0
            start_time = time.time()
        params = {"offset": offset, "limit": limit}
        response = requests.get(containers_url, headers=headers, params=params, timeout=60, verify=False)
        response.raise_for_status()
        request_count += 1
        containers_page = response.json()
        if not containers_page:
            break
        all_containers.extend(containers_page)
        if len(containers_page) < limit:
            break
        offset += limit
    logging.info(f"Finished fetching all containers. Total found: {len(all_containers)}")

    if not all_containers:
        return ""

    s3_hook = S3Hook()
    data_string = json.dumps(all_containers)
    
    # Generate a timestamp using Python for the exact current time in UTC.
    current_time_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S")
    s3_key = f"container_data/run_{current_time_str}.json"
    
    logging.info(f"Uploading data to s3://{S3_BUCKET_NAME}/{s3_key}")
    s3_hook.load_string(
        string_data=data_string, key=s3_key, bucket_name=S3_BUCKET_NAME, replace=True
    )
    return f"s3://{S3_BUCKET_NAME}/{s3_key}"

@task(task_id="generate_chunks_from_s3_file")
def generate_chunks_from_s3_file(s3_uri: str) -> List[Tuple[int, int]]:
    """
    Reads the S3 file, counts the items, and returns a list of chunk
    definitions (start_index, end_index) for mapping.
    """
    if not s3_uri:
        return []

    s3_hook = S3Hook()
    data_string = s3_hook.read_key(key=s3_hook.parse_s3_url(s3_uri)[1], bucket_name=S3_BUCKET_NAME)
    total_items = len(json.loads(data_string))
    
    chunks = []
    for i in range(0, total_items, CHUNK_SIZE):
        start_index = i
        end_index = min(i + CHUNK_SIZE, total_items)
        chunks.append((start_index, end_index))
    
    logging.info(f"Generated {len(chunks)} chunks of size {CHUNK_SIZE} for {total_items} total items.")
    return chunks

@task(task_id="process_chunk_from_s3", pool=API_POOL_NAME)
def process_chunk_from_s3(s3_uri: str, chunk: Tuple[int, int]) -> List[Dict[str, Any]]:
    """
    Downloads the full dataset from S3 and processes one chunk of it.
    """
    if not s3_uri:
        return []

    s3_hook = S3Hook()
    start_index, end_index = chunk
    logging.info(f"Processing chunk from index {start_index} to {end_index}.")
    
    data_string = s3_hook.read_key(key=s3_hook.parse_s3_url(s3_uri)[1], bucket_name=S3_BUCKET_NAME)
    all_containers = json.loads(data_string)
    
    containers_to_process = all_containers[start_index:end_index]
    
    processed_results = []
    for container in containers_to_process:
        container_id = container.get("_id")
        open_ports = []
        network = container.get("network", {})
        for port in network.get("ports", []):
            open_ports.append({"port": port.get("container"), "type": "network"})
        if open_ports:
            processed_results.append({"id": container_id, "open_ports": open_ports})
            
    return processed_results

@task(task_id="load_network_info")
def load_network_info_task(all_container_info_chunks: List[List[Dict[str, Any]]]):
    """
    Receives a list of lists (from all chunks), flattens it, and logs the result.
    """
    all_container_info = [item for sublist in all_container_info_chunks for item in sublist]
    
    count = len(all_container_info)
    logging.info(f"--- Aggregated Results ---")
    logging.info(f"Total containers with network info processed: {count}")

@task
def cleanup_s3_file_task(s3_uri: str):
    """Deletes the temporary data file from S3."""
    if not s3_uri:
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
    ### Robust Container ETL DAG using Chunking
    This DAG processes a very large dataset by fetching all data to a single S3
    file, then dynamically creating parallel tasks to process the data in chunks.
    This avoids XCom limits for both the data and the mapping arguments.
    """,
    tags=["api", "s3", "chunking", "dynamic-mapping"],
)
def container_network_etl_dag():
    """Defines the ETL workflow using the chunking pattern."""
    s3_uri = fetch_and_upload_to_s3()
    
    chunk_list = generate_chunks_from_s3_file(s3_uri=s3_uri)
    
    processed_chunks = process_chunk_from_s3.partial(
        s3_uri=s3_uri
    ).expand(chunk=chunk_list)
    
    load_op = load_network_info_task(all_container_info_chunks=processed_chunks)
    
    cleanup_op = cleanup_s3_file_task(s3_uri=s3_uri)
    
    load_op >> cleanup_op

container_network_etl_dag()
