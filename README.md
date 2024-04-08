# Airflow DAG Interface Python Library

This Python library provides a user-friendly interface to interact with the Airflow API, specifically focusing on managing DAGs (Directed Acyclic Graphs). It simplifies tasks like creating DAGs, retrieving DAG information, and managing pools.

## Key Features:

- **Create DAG Runs**: Programmatically initiate DAG runs using the `create_dag` function.
  
- **Manage DAG Information**: Retrieve details about DAG runs (`get_dag_runs`), individual DAG run information (`get_dag_run_info`), and the paused state of a DAG (`get_dag_paused_state`).
  
- **Pause/Unpause DAGs**: Control the execution state of DAGs using `pause_dag` and `unpause_dag`.
  
- **Retrieve Task Information**: Get details about specific tasks within a DAG (`get_task_info`, `get_task_instance_info`).
  
- **Test API Server Connection**: Verify connectivity with the Airflow API server.
  
- **Manage Pools**: Interact with Airflow's pool system for resource allocation:
  - List all pools (`get_all_pools`).
  - Retrieve information about a specific pool (`get_pool_by_name`).
  - Create new pools (`create_pool`).
  - Delete pools (`delete_pool`).
    
  

## Installation:

1. Clone or download this repository. git clone https://github.com/okiemute04/airflow-dag-interface
2. Navigate to the project directory in your terminal.
3. Install dependencies using `pip install -r requirements.txt`.

## Usage:

```python
>>>>>>> main
from airflow_dag_interface import AirflowDAGInterface

# Initialize the interface with the base URL of the Airflow API
interface = AirflowDAGInterface("http://localhost:8080")

# Example: Create a new DAG run
dag_id = "example_dag"
config = {"key": "value"}  # Configuration for the new DAG run
result = interface.create_dag(dag_id, config)
print(result)

# Example: Retrieve DAG runs for a specific DAG
dag_runs = interface.get_dag_runs(dag_id)
print(dag_runs)
```




## Testing:

This library includes unit tests written with pytest. To run the tests, navigate to the project directory and execute:

```bash
pytest
```

## Contributing:

Contributions are welcome! Please feel free to submit issues or pull requests.

## License:

This project is licensed under the MIT License - see the LICENSE file for details.


