import pytest
import requests_mock
from airflow_dag_interface.utils import RequestHandler

@pytest.fixture
def request_handler():
    return RequestHandler("http://example.com")

class TestRequestHandler:
    def test_get_request_success(self, request_handler):
        with requests_mock.Mocker() as m:
            m.get('http://example.com/api/experimental/endpoint', json={'key': 'value'}, headers={'content-type': 'application/json'})
            response = request_handler.get_request("endpoint")
        assert response == {'key': 'value'}

    def test_get_request_failure(self, request_handler):
        with requests_mock.Mocker() as m:
            m.get('http://example.com/api/experimental/endpoint', status_code=404)
            response = request_handler.get_request("endpoint")
        assert 'error' in response

    def test_post_request_success(self, request_handler):
        with requests_mock.Mocker() as m:
            m.post('http://example.com/api/experimental/endpoint', json={'key': 'value'}, headers={'content-type': 'application/json'})
            response = request_handler.post_request("endpoint", {'key': 'value'})
        assert response == {'key': 'value'}

    def test_post_request_failure(self, request_handler):
        with requests_mock.Mocker() as m:
            m.post('http://example.com/api/experimental/endpoint', status_code=500)
            response = request_handler.post_request("endpoint", {'key': 'value'})
        assert 'error' in response

    def test_delete_request_success(self, request_handler):
        with requests_mock.Mocker() as m:
            m.delete('http://example.com/api/experimental/endpoint', json={'message': 'deleted'},
                     headers={'content-type': 'application/json'})
            response = request_handler.delete_request("endpoint")
        assert response == {'message': 'deleted'}


    def test_delete_request_failure(self, request_handler):
        with requests_mock.Mocker() as m:
            m.delete('http://example.com/api/experimental/endpoint', status_code=500)
            response = request_handler.delete_request("endpoint")
        assert 'error' in response

if __name__ == "__main__":
    pytest.main()
