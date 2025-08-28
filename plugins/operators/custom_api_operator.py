# plugins/operators/custom_api_operator.py

from typing import Any, Dict

from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowException

from ..hooks.custom_api_hook import CustomApiHook
from plugins.triggers.rate_limit_trigger import RateLimitTrigger

class CustomApiOperator(BaseOperator):
    """
    A custom operator to make an API call, with deferrable support for polling.

    :param api_conn_id: The Airflow connection ID for the API.
    :param endpoint: The API endpoint to call.
    :param params: A dictionary of query parameters.
    """
    template_fields = ("params",)

    def __init__(
        self,
        *,
        api_conn_id: str,
        endpoint: str,
        params: Dict[str, Any],
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.api_conn_id = api_conn_id
        self.endpoint = endpoint
        self.params = params

    def execute(self, context: Dict[str, Any]) -> Any:
        """
        Executes the API call. If the API response indicates waiting is needed,
        it defers execution.
        """
        hook = CustomApiHook(conn_id=self.api_conn_id)
        response = hook.make_request(endpoint=self.endpoint, params=self.params)

        # Example logic: Defer if the API response indicates a job is still processing
        if response.get("status") == "processing":
            polling_interval = response.get("polling_interval_seconds", 30)
            self.log.info(f"API job is processing. Deferring task for {polling_interval} seconds.")
            
            self.defer(
                trigger=RateLimitTrigger(wait_seconds=float(polling_interval)),
                method_name="execute_complete",
            )
        elif response.get("status") == "complete":
            self.log.info("API job complete on first call. Processing results.")
            # Process the final data here
            return response.get("data")
        else:
            raise AirflowException(f"API returned an unexpected status: {response.get('status')}")

    def execute_complete(self, context: Dict[str, Any], event: Dict[str, Any] = None) -> Any:
        """
        Callback method executed when the trigger fires.
        This method makes the final API call to retrieve the results.
        """
        if event is None or event.get("status")!= "success":
            raise AirflowException(f"Trigger event failed: {event}")
        
        self.log.info("Resuming task after deferral. Fetching final results.")
        hook = CustomApiHook(conn_id=self.api_conn_id)
        
        # In a real scenario, you might call a different endpoint or the same one
        # to get the final status/result.
        final_response = hook.make_request(endpoint=self.endpoint, params=self.params)

        if final_response.get("status") == "complete":
            self.log.info("Successfully fetched final results.")
            return final_response.get("data")
        else:
            # Implement retry logic or fail the task
            raise AirflowException(f"Job still not complete after deferral. Status: {final_response.get('status')}")
