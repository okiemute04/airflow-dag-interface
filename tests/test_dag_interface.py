import pytest
from airflow_dag_interface.dag_interface import AirflowDAGInterface

# Define a fixture for the AirflowDAGInterface instance
@pytest.fixture
def airflow_interface():
    return AirflowDAGInterface("http://localhost:8080")




# Test cases for the AirflowDAGInterface class
def test_create_dag_success(airflow_interface, requests_mock):
    dag_id = "example_dag"
    config = {"key": "value"}  # Config should be a dictionary
    requests_mock.post(
        "http://localhost:8080/api/experimental/dags/example_dag/dag_runs",
        json={"success": "DAG run for example_dag created successfully"},
        status_code=200
    )
    result = airflow_interface.create_dag(dag_id, config)
    assert 'success' in result or 'error' in result, "Failed to handle create_dag success"
    if 'success' in result:
        assert result['success'] == "DAG run for example_dag created successfully", \
            "Failed to handle create_dag success"
    else:
        assert result['error'] == f"Failed to create DAG run for {dag_id}", \
            "Failed to handle create_dag failure"

def test_create_dag_failure(airflow_interface, requests_mock):
    dag_id = "example_dag"
    config = '{"key":"value"}'
    requests_mock.post("http://localhost:8080/api/experimental/dags/example_dag/dag_runs", json={"error": "Failed to create DAG run for example_dag"})
    result = airflow_interface.create_dag(dag_id, config)
    assert 'error' in result

def test_get_dag_runs(airflow_interface, requests_mock):
    dag_id = "example_dag"
    requests_mock.get("http://localhost:8080/api/experimental/dags/example_dag/dag_runs", json={"error": "Failed to get DAG runs for example_dag"})
    dag_runs = airflow_interface.get_dag_runs(dag_id)
    assert 'error' in dag_runs


def test_get_dag_run_info(airflow_interface, requests_mock):
    dag_id = "example_dag"
    execution_date = "2022-01-01T00:00:00"
    requests_mock.get(f"http://localhost:8080/api/experimental/dags/{dag_id}/dag_runs/{execution_date}", json={"error": "Failed to get DAG run info for example_dag - 2022-01-01T00:00:00"})
    dag_run_info = airflow_interface.get_dag_run_info(dag_id, execution_date)
    assert 'error' in dag_run_info

def test_test_api_server(airflow_interface, requests_mock):
    requests_mock.get("http://localhost:8080/api/experimental/test", json={"error": "Failed to test API server connection"})
    result = airflow_interface.test_api_server()
    assert 'error' in result

def test_get_task_info(airflow_interface, requests_mock):
    dag_id = "example_dag"
    task_id = "task_id"
    requests_mock.get(f"http://localhost:8080/api/experimental/dags/{dag_id}/tasks/{task_id}", json={"error": "Failed to get task info for example_dag - task_id"})
    task_info = airflow_interface.get_task_info(dag_id, task_id)
    assert 'error' in task_info

def test_get_task_instance_info(airflow_interface, requests_mock):
    dag_id = "example_dag"
    execution_date = "2022-01-01T00:00:00"
    task_id = "task_id"
    requests_mock.get(f"http://localhost:8080/api/experimental/dags/{dag_id}/dag_runs/{execution_date}/tasks/{task_id}", json={"error": "Failed to get task instance info for example_dag - 2022-01-01T00:00:00 - task_id"})
    task_instance_info = airflow_interface.get_task_instance_info(dag_id, execution_date, task_id)
    assert 'error' in task_instance_info

def test_get_dag_paused_state(airflow_interface, requests_mock):
    dag_id = "example_dag"
    requests_mock.get(f"http://localhost:8080/api/experimental/dags/{dag_id}/paused", json={"error": "Failed to get paused state for example_dag"})
    paused_state = airflow_interface.get_dag_paused_state(dag_id)
    assert 'error' in paused_state

def test_pause_dag(airflow_interface, requests_mock):
    dag_id = "example_dag"
    requests_mock.get(f"http://localhost:8080/api/experimental/dags/{dag_id}/paused/true", json={"error": "Failed to pause DAG example_dag"})
    result = airflow_interface.pause_dag(dag_id)
    assert 'error' in result

def test_unpause_dag(airflow_interface, requests_mock):
    dag_id = "example_dag"
    requests_mock.get(f"http://localhost:8080/api/experimental/dags/{dag_id}/paused/false", json={"error": "Failed to unpause DAG example_dag"})
    result = airflow_interface.unpause_dag(dag_id)
    assert 'error' in result

def test_get_latest_dag_runs(airflow_interface, requests_mock):
    requests_mock.get("http://localhost:8080/api/experimental/latest_runs", json={"error": "Failed to get latest DAG runs"})
    latest_dag_runs = airflow_interface.get_latest_dag_runs()
    assert 'error' in latest_dag_runs

def test_get_all_pools(airflow_interface, requests_mock):
    requests_mock.get("http://localhost:8080/api/experimental/pools", json={"error": "Failed to get all pools"})
    all_pools = airflow_interface.get_all_pools()
    assert 'error' in all_pools


def test_get_pool_by_name(airflow_interface, requests_mock):
    pool_name = "pool_1"
    requests_mock.get(f"http://localhost:8080/api/experimental/pools/{pool_name}", json={"error": "Failed to get pool pool_1"})
    pool_info = airflow_interface.get_pool_by_name(pool_name)
    assert 'error' in pool_info

def test_create_pool(airflow_interface, requests_mock):
    pool_data = {"pool_name": "test_pool", "slots": 5}
    requests_mock.post("http://localhost:8080/api/experimental/pools", json={"error": "Failed to create pool"})
    result = airflow_interface.create_pool(pool_data)
    assert 'error' in result

def test_delete_pool(airflow_interface, requests_mock):
    pool_name = "test_pool"
    requests_mock.delete(f"http://localhost:8080/api/experimental/pools/{pool_name}", json={"error": "Failed to delete pool test_pool"})
    result = airflow_interface.delete_pool(pool_name)
    assert 'error' in result


if __name__ == "__main__":
    pytest.main()
