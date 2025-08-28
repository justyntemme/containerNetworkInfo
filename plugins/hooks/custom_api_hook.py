# plugins/hooks/custom_api_hook.py

import requests
from requests.adapters import HTTPAdapter, Retry

from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException

class CustomApiHook(BaseHook):
    """
    A custom hook to interact with a generic REST API.

    This hook manages connection details from Airflow's connection backend
    and provides methods for making robust API requests.

    :param conn_id: The Airflow connection ID for the API.
    """
    default_conn_name = "custom_api_default"

    def __init__(self, conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.conn_id = conn_id
        self.connection = self.get_connection(self.conn_id)
        self.session = None

    def _get_session(self) -> requests.Session:
        """
        Initializes and returns a requests.Session object with retry logic.
        This promotes connection reuse and resilience.
        """
        if self.session is None:
            self.session = requests.Session()
            
            # Configure retry strategy
            retries = Retry(
                total=3,
                backoff_factor=1,
                status_forcelist=[500, 502, 503, 504]
            )
            adapter = HTTPAdapter(max_retries=retries)
            self.session.mount("http://", adapter)
            self.session.mount("https://", adapter)

            # Set authentication headers from the connection password field
            if self.connection.password:
                self.log.info("Setting Authorization header from connection password.")
                self.session.headers.update({"Authorization": f"Bearer {self.connection.password}"})
        
        return self.session

    def make_request(self, endpoint: str, params: dict = None) -> dict:
        """
        Makes a GET request to the specified API endpoint.

        :param endpoint: The API endpoint to call (e.g., '/users').
        :param params: A dictionary of query parameters for the request.
        :return: The JSON response as a dictionary.
        """
        session = self._get_session()
        base_url = self.connection.host
        url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"

        self.log.info(f"Making GET request to {url} with params: {params}")
        
        try:
            response = session.get(url, params=params, timeout=15)
            response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)
            return response.json()
        except requests.exceptions.RequestException as e:
            self.log.error(f"API request failed: {e}")
            raise AirflowException(f"Failed to fetch data from API endpoint {endpoint}: {e}")
