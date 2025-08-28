# dags/refactored_container_etl_dag.py

import json
import logging
import time
from typing import Any, Dict, List

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models import Variable

# --- Constants ---
API_POOL_NAME = "api_processing_pool"

# --- Task Definitions ---

@task(task_id="get_all_containers")
def get_all_containers_task() -> List]:
    """
    Fetches all container data from the paginated API, handling rate limiting
    for the sequential fetch process. This task's sole responsibility is to
    gather the complete dataset for downstream processing.
    """
    # This task re-implements the logic from the original script's
    # get_all_containers function, but only for data extraction.
    try:
        tl_url = Variable.get("TL_URL")
        access_key = Variable.get("PC_IDENTITY")
        access_secret = Variable.get("PC_SECRET")
    except KeyError as e:
        logging.error(f"Airflow Variable {e} not found.")
        raise AirflowException(f"Missing required Airflow Variable: {e}")

    # Authenticate to get token
    auth_url = f"{tl_url}/api/v1/authenticate"
    auth_body = {"username": access_key, "password": access_secret}
    try:
        response = requests.post(auth_url, json=auth_body, timeout=60, verify=False)
        response.raise_for_status()
        token = response.json().get("token")
        if not token:
            raise AirflowException("Token not found in API response.")
    except requests.exceptions.RequestException as e:
        raise AirflowException(f"Token generation failed: {e}")

    # Setup for paginated fetch
    containers_url = f"{tl_url}/api/v1/containers"
    headers = {"Authorization": f"Bearer {token}"}
    all_containers =
    offset = 0
    limit = 100
    rate_limit = 30
    rate_limit_period = 31
    request_count = 0
    start_time = time.time()

    while True:
        # This self-contained rate limiting is acceptable here because this task
        # runs sequentially as a single worker process. It is not blocking
        # a large pool of parallel tasks.
        if request_count >= rate_limit:
            elapsed_time = time.time() - start_time
            if elapsed_time < rate_limit_period:
                sleep_time = rate_limit_period - elapsed_time
                logging.info(f"Sequential fetch rate limit reached. Sleeping for {sleep_time:.2f}s.")
                time.sleep(sleep_time)
            request_count = 0
            start_time = time.time()

        params = {"offset": offset, "limit": limit}
        try:
            response = requests.get(containers_url, headers=headers, params=params, timeout=60, verify=False)
            response.raise_for_status()
            request_count += 1
            
            containers_page = response.json()
            if not containers_page:
                logging.info("No more containers to fetch.")
                break

            all_containers.extend(containers_page)
            logging.info(f"Fetched {len(containers_page)} containers. Total so far: {len(all_containers)}")

            if len(containers_page) < limit:
                logging.info("Reached the last page of containers.")
                break
            
            # This is the core pagination logic, ensuring the next page is requested.
            offset += limit

        except requests.exceptions.RequestException as e:
            raise AirflowException(f"API request for containers failed: {e}")

    logging.info(f"Finished fetching all containers. Total found: {len(all_containers)}")
    return all_containers


@task(task_id="extract_network_info", pool=API_POOL_NAME)
def extract_network_info_task(container: Dict[str, Any]) -> Dict[str, Any]:
    """
    Processes a single container to extract network info. This task is
    dynamically mapped and its concurrency is controlled by an Airflow Pool.
    """
    # This is the same processing logic from the original script.
    container_id = container.get("_id")
    open_ports =
    network = container.get("network", {})
    for port in network.get("ports",):
        open_ports.append({"port": port.get("container"), "type": "network"})
    
    if open_ports:
        return {"id": container_id, "open_ports": open_ports}
    return {}


@task(task_id="load_network_info")
def load_network_info_task(all_container_info: List]):
    """Mocks loading data by logging the processed container info."""
    count = sum(1 for info in all_container_info if info)
    logging.info(f"--- Aggregated Results ---")
    logging.info(f"Total containers with network info processed: {count}")
    # In a real scenario, this task would perform a bulk load to a database.


# --- DAG Definition ---

@dag(
    dag_id="refactored_container_network_etl",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    doc_md="""
    ### Refactored Container Network Info ETL DAG
    This DAG extracts all container data in a single task, then uses dynamic
    task mapping to process each container in parallel. Concurrency of the
    parallel processing is controlled by an Airflow Pool to respect API rate limits.
    """,
    tags=["api", "refactor", "dynamic-mapping", "pools"],
)
def container_network_etl_dag():
    """Defines the refactored ETL workflow."""
    containers_list = get_all_containers_task()
    processed_containers = extract_network_info_task.expand(container=containers_list)
    load_network_info_task(all_container_info=processed_containers)

container_network_etl_dag()
