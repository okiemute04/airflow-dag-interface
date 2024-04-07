import logging
from airflow_dag_interface.utils import RequestHandler

class AirflowDAGInterface:
    """
    A class to interact with the Airflow DAG interface API.

    Attributes:
        request_handler (RequestHandler): An instance of RequestHandler for making API requests.
        logger (Logger): A logger instance for logging messages.
    """

    def __init__(self, base_url):
        """
        Initialize the AirflowDAGInterface.

        Args:
            base_url (str): The base URL of the Airflow API.
        """
        self.request_handler = RequestHandler(base_url)
        self.logger = logging.getLogger(__name__)




    def create_dag(self, dag_id, config, auth=None):
        """
        Create a new DAG run for the specified DAG.

        Args:
            dag_id (str): The ID of the DAG.
            config (dict): Configuration for the new DAG run.
            auth (str, optional): Authorization token (if required). Defaults to None.

        Returns:
            dict: Response from the API.
        """
        endpoint = f"dags/{dag_id}/dag_runs"
        try:
            response = self.request_handler.post_request(endpoint, {'conf': config}, auth)

            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                return {'success': f'DAG run for {dag_id} created successfully'}
            else:
                return {'error': f'Failed to create DAG run for {dag_id}'}
        except Exception as e:
            self.logger.error(f"Failed to create DAG run for {dag_id}: {e}")
            return {'error': f'Failed to create DAG run for {dag_id}'}



    def get_dag_runs(self, dag_id, auth=None):
        """
        Get a list of DAG runs for the specified DAG.

        Args:
            dag_id (str): The ID of the DAG.
            auth (str, optional): Authorization token (if required). Defaults to None.

        Returns:
            list: List of DAG runs.
        """
        endpoint = f"dags/{dag_id}/dag_runs"
        try:
            response = self.request_handler.get_request(endpoint, auth)
            return response.json()
        except Exception as e:
            self.logger.error(f"Failed to get DAG runs for {dag_id}: {e}")
            return {'error': f'Failed to get DAG runs for {dag_id}'}

    def get_dag_run_info(self, dag_id, execution_date, auth=None):
        """
        Get information about a specific DAG run.

        Args:
            dag_id (str): The ID of the DAG.
            execution_date (str): The execution date of the DAG run.
            auth (str, optional): Authorization token (if required). Defaults to None.

        Returns:
            dict: Information about the DAG run.
        """
        endpoint = f"dags/{dag_id}/dag_runs/{execution_date}"
        try:
            response = self.request_handler.get_request(endpoint, auth)
            return response.json()
        except Exception as e:
            self.logger.error(f"Failed to get DAG run info for {dag_id} - {execution_date}: {e}")
            return {'error': f'Failed to get DAG run info for {dag_id} - {execution_date}'}

    def test_api_server(self):
        """
        Test the connection to the API server.

        Returns:
            dict: Response from the API server.
        """
        endpoint = "test"
        try:
            response = self.request_handler.get_request(endpoint)
            return response.json()
        except Exception as e:
            self.logger.error(f"Failed to test API server connection: {e}")
            return {'error': 'Failed to test API server connection'}

    def get_task_info(self, dag_id, task_id, auth=None):
        """
        Get information about a specific task in a DAG.

        Args:
            dag_id (str): The ID of the DAG.
            task_id (str): The ID of the task.
            auth (str, optional): Authorization token (if required). Defaults to None.

        Returns:
            dict: Information about the task.
        """
        endpoint = f"dags/{dag_id}/tasks/{task_id}"
        try:
            response = self.request_handler.get_request(endpoint, auth)
            return response.json()
        except Exception as e:
            self.logger.error(f"Failed to get task info for {dag_id} - {task_id}: {e}")
            return {'error': f'Failed to get task info for {dag_id} - {task_id}'}

    def get_task_instance_info(self, dag_id, execution_date, task_id, auth=None):
        """
        Get information about a specific task instance in a DAG run.

        Args:
            dag_id (str): The ID of the DAG.
            execution_date (str): The execution date of the DAG run.
            task_id (str): The ID of the task.
            auth (str, optional): Authorization token (if required). Defaults to None.

        Returns:
            dict: Information about the task instance.
        """
        endpoint = f"dags/{dag_id}/dag_runs/{execution_date}/tasks/{task_id}"
        try:
            response = self.request_handler.get_request(endpoint, auth)
            return response.json()
        except Exception as e:
            self.logger.error(f"Failed to get task instance info for {dag_id} - {execution_date} - {task_id}: {e}")
            return {'error': f'Failed to get task instance info for {dag_id} - {execution_date} - {task_id}'}

    def get_dag_paused_state(self, dag_id, auth=None):
        """
        Get the paused state of a DAG.

        Args:
            dag_id (str): The ID of the DAG.
            auth (str, optional): Authorization token (if required). Defaults to None.

        Returns:
            dict: Paused state of the DAG.
        """
        endpoint = f"dags/{dag_id}/paused"
        try:
            response = self.request_handler.get_request(endpoint, auth)
            return response.json()
        except Exception as e:
            self.logger.error(f"Failed to get paused state for {dag_id}: {e}")
            return {'error': f'Failed to get paused state for {dag_id}'}

    def pause_dag(self, dag_id, auth=None):
        """
        Pause a DAG.

        Args:
            dag_id (str): The ID of the DAG.
            auth (str, optional): Authorization token (if required). Defaults to None.

        Returns:
            dict: Response from the API.
        """
        endpoint = f"dags/{dag_id}/paused/true"
        try:
            response = self.request_handler.get_request(endpoint, auth)
            return response.json()
        except Exception as e:
            self.logger.error(f"Failed to pause DAG {dag_id}: {e}")
            return {'error': f'Failed to pause DAG {dag_id}'}

    def unpause_dag(self, dag_id, auth=None):
        """
        Unpause a DAG.

        Args:
            dag_id (str): The ID of the DAG.
            auth (str, optional): Authorization token (if required). Defaults to None.

        Returns:
            dict: Response from the API.
        """
        endpoint = f"dags/{dag_id}/paused/false"
        try:
            response = self.request_handler.get_request(endpoint, auth)
            return response.json()
        except Exception as e:
            self.logger.error(f"Failed to unpause DAG {dag_id}: {e}")
            return {'error': f'Failed to unpause DAG {dag_id}'}

    def get_latest_dag_runs(self):
        """
        Get the latest DAG runs.

        Returns:
            dict: Latest DAG runs.
        """
        endpoint = "latest_runs"
        try:
            response = self.request_handler.get_request(endpoint)
            return response.json()
        except Exception as e:
            self.logger.error(f"Failed to get latest DAG runs: {e}")
            return {'error': 'Failed to get latest DAG runs'}

    def get_all_pools(self):
        """
        Get information about all pools.

        Returns:
            dict: Information about all pools.
        """
        endpoint = "pools"
        try:
            response = self.request_handler.get_request(endpoint)
            return response.json()
        except Exception as e:
            self.logger.error(f"Failed to get all pools: {e}")
            return {'error': 'Failed to get all pools'}

    def get_pool_by_name(self, pool_name):
        """
        Get information about a specific pool by name.

        Args:
            pool_name (str): The name of the pool.

        Returns:
            dict: Information about the pool.
        """
        endpoint = f"pools/{pool_name}"
        try:
            response = self.request_handler.get_request(endpoint)
            return response.json()
        except Exception as e:
            self.logger.error(f"Failed to get pool {pool_name}: {e}")
            return {'error': f'Failed to get pool {pool_name}'}

    def create_pool(self, pool_data):
        """
        Create a new pool.

        Args:
            pool_data (dict): Data for creating the pool.

        Returns:
            dict: Response from the API.
        """
        endpoint = "pools"
        try:
            response = self.request_handler.post_request(endpoint, pool_data)
            if response.status_code == 200:
                return response.json()
            else:
                self.logger.error(f"Failed to create pool: {response.text}")
                return {'error': f"Failed to create pool: {response.text}"}
        except Exception as e:
            self.logger.error(f"Failed to create pool: {e}")
            return {'error': 'Failed to create pool'}

    def delete_pool(self, pool_name):
        """
        Delete a pool.

        Args:
            pool_name (str): The name of the pool to delete.

        Returns:
            dict: Response from the API.
        """
        endpoint = f"pools/{pool_name}"
        try:
            response = self.request_handler.delete_request(endpoint)
            if response.status_code == 200:
                return response.json()
            else:
                self.logger.error(f"Failed to delete pool {pool_name}: {response.text}")
                return {'error': f"Failed to delete pool {pool_name}: {response.text}"}
        except Exception as e:
            self.logger.error(f"Failed to delete pool {pool_name}: {e}")
            return {'error': f'Failed to delete pool {pool_name}'}
