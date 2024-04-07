import requests
import logging

class RequestHandler:
    def __init__(self, base_url):
        self.base_url = base_url.rstrip('/') + '/api/experimental/'
        self.logger = logging.getLogger(__name__)
        self.headers = {'Content-Type': 'application/json', 'Cache-Control': 'no-cache'}

    def _handle_request(self, method, endpoint, data=None, auth=None):
        url = self.base_url + endpoint
        if auth:
            self.headers['Authorization'] = auth

        try:
            response = method(url, headers=self.headers, json=data)
            response.raise_for_status()
            return response.json() if response.headers['content-type'] == 'application/json' else response.text
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"Failed to perform {method.__name__.upper()} request for endpoint {endpoint}: {e}")
            return {'error': f'Failed to perform {method.__name__.upper()} request for endpoint {endpoint}'}

    def get_request(self, endpoint, auth=None):
        return self._handle_request(requests.get, endpoint, auth=auth)

    def post_request(self, endpoint, data, auth=None):
        return self._handle_request(requests.post, endpoint, data, auth=auth)

    def delete_request(self, endpoint, auth=None):
        return self._handle_request(requests.delete, endpoint, auth=auth)
