import json
import logging
from typing import Any, Dict, List

import pendulum
import requests
import urllib3
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models.variable import Variable
from airflow.providers.mysql.hooks.mysql import MySqlHook

# Suppress InsecureRequestWarning from logs for cleaner output
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# --- Task Definitions ---

@task(task_id="generate_cwp_token")
def generate_cwp_token() -> str:
    """
    Authenticates with the CWP API using credentials stored in Airflow Variables
    and returns a session token.
    """
    try:
        access_key = Variable.get("pcIdentity")
        access_secret = Variable.get("pcSecret")
        tl_url = Variable.get("tlUrl")
    except KeyError as e:
        logging.error(f"Airflow Variable {e} not found.")
        raise AirflowException(f"Missing required Airflow Variable: {e}")

    auth_url = f"{tl_url}/api/v1/authenticate"
    headers = {
        "accept": "application/json; charset=UTF-8",
        "content-type": "application/json",
    }
    body = {"username": access_key, "password": access_secret}

    try:
        response = requests.post(
            auth_url, headers=headers, json=body, timeout=60, verify=False
        )
        response.raise_for_status()
        token = response.json().get("token")
        if not token:
            raise AirflowException("Token not found in API response.")
        logging.info("Successfully acquired CWP authentication token.")
        return token
    except requests.exceptions.RequestException as e:
        logging.error(f"Unable to acquire token. Error: {e}")
        raise AirflowException(f"Token generation failed: {e}")


@task(task_id="extract_vulnerability_scans")
def get_scans(token: str) -> Dict[str, Any]:
    """
    Fetches vulnerability scan data for a specific CVE from the API.
    """
    tl_url = Variable.get("tlUrl")
    # The CVE was hardcoded in the original script
    scan_url = f"{tl_url}/api/v1/stats/vulnerabilities/impacted-resources?cve=ubuntu-custom-vuln&resourceType=image"
    headers = {
        "accept": "application/json; charset=UTF-8",
        "Authorization": f"Bearer {token}",
    }

    try:
        response = requests.get(scan_url, headers=headers, timeout=60, verify=False)
        response.raise_for_status()
        logging.info(f"Successfully fetched scan data. Status: {response.status_code}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching scan data: {e}")
        raise AirflowException(f"API request for scan data failed: {e}")
    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON from response: {e}")
        raise AirflowException("Invalid JSON response from API.")


@task(task_id="transform_filter_debian_packages")
def filter_debian_packages(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Filters a list of scanned images to remove any that contain Debian packages.
    """
    images_without_debian = []
    
    if 'images' not in data or not isinstance(data.get('images'), list):
        logging.warning("'images' key not found or is not a list. Returning empty list.")
        return []

    for image in data['images']:
        # Default to including the image unless a debian package is found
        include_image = True
        
        if 'packages' in image and isinstance(image.get('packages'), list):
            for pkg in image['packages']:
                # Check if package name contains 'deb'
                if isinstance(pkg, dict) and 'package' in pkg and isinstance(pkg.get('package'), str) and 'deb' in pkg['package']:
                    include_image = False
                    # Found a debian package, no need to check others in this image
                    break
        
        if include_image:
            images_without_debian.append(image)
            
    logging.info(f"Original image count: {len(data['images'])}. Filtered count: {len(images_without_debian)}.")
    return images_without_debian


@task(task_id="load_data_to_mysql")
def load_data_to_mysql(filtered_data: List[Dict[str, Any]], mysql_conn_id: str = "rds-1"):
    """
    Connects to a MySQL database and inserts the filtered vulnerability data.
    This task is idempotent: it uses 'REPLACE INTO' to overwrite existing records
    based on the primary key (repo, tag, cve).
    """
    target_table = "cwp_impacted_resources"
    cve_id = "ubuntu-custom-vuln" # As hardcoded in the get_scans task

    if not filtered_data:
        logging.info("No data to load into MySQL.")
        return

    # The connection 'mysql_conn_id' must be configured in the Airflow UI.
    # For AWS RDS with IAM auth (as hinted by the SSO requirement), ensure your
    # Airflow worker has the necessary IAM role and the connection 'Extra'
    # field is configured, e.g., {"iam": true}
    hook = MySqlHook(mysql_conn_id=mysql_conn_id)

    # SQL to create the table if it doesn't exist.
    # A composite primary key ensures each image-tag-cve combination is unique.
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
        repo VARCHAR(255) NOT NULL,
        tag VARCHAR(255) NOT NULL,
        cve VARCHAR(100) NOT NULL,
        resource_id VARCHAR(255),
        last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        full_resource_data JSON,
        PRIMARY KEY (repo, tag, cve)
    );
    """
    hook.run(create_table_sql)
    logging.info(f"Ensured table '{target_table}' exists.")

    # Prepare rows for insertion using .get() for safety against missing keys
    rows_to_insert = [
        (
            resource.get('repo'),
            resource.get('tag'),
            cve_id,
            resource.get('id'),
            json.dumps(resource)
        )
        for resource in filtered_data
        # Ensure repo and tag are present, as they are part of the primary key
        if resource.get('repo') and resource.get('tag')
    ]

    if rows_to_insert:
        target_fields = [
            'repo',
            'tag',
            'cve',
            'resource_id',
            'full_resource_data'
        ]
        # Using replace=True makes the operation idempotent (uses MySQL's REPLACE INTO)
        hook.insert_rows(
            table=target_table,
            rows=rows_to_insert,
            target_fields=target_fields,
            replace=True
        )
        logging.info(f"Successfully replaced {len(rows_to_insert)} rows in '{target_table}'.")
    else:
        logging.info("No valid rows to insert after final processing.")


# --- DAG Definition ---

@dag(
    dag_id="vulnerability_scan_etl_to_mysql",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    doc_md="""
    ### Vulnerability Scan ETL DAG
    This DAG fetches vulnerability scan data for a specific CVE, filters out any
    impacted images that contain Debian packages, and loads the final list into a MySQL table.
    
    **Required Airflow Variables**: `pcIdentity`, `pcSecret`, `tlUrl`.
    
    **Required Airflow Connection**: A MySQL connection with the ID `mysql_default` (or as specified).
    """,
    tags=["etl", "taskflow", "security", "vulnerability", "mysql"],
)
def vulnerability_scan_etl_dag():
    """
    Defines the ETL workflow for filtering vulnerability scan data and loading to MySQL.
    """
    # 1. Get authentication token
    auth_token = generate_cwp_token()

    # 2. Extract raw scan data from the API
    raw_scan_data = get_scans(token=auth_token)

    # 3. Transform the data by filtering out images with debian packages
    filtered_scan_data = filter_debian_packages(data=raw_scan_data)

    # 4. Load the final, filtered data into MySQL
    load_data_to_mysql(filtered_data=filtered_scan_data)

# Instantiate the DAG to make it discoverable by Airflow
vulnerability_scan_etl_dag()
