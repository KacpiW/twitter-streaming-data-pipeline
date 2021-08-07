from requests.auth import AuthBase


class OAuth2Bearer(AuthBase):

    def __init__(self, bearer_token: str):
        self.bearer_token = bearer_token

    def __call__(self, request):
        request.headers['Authorization'] = "Bearer " + self.bearer_token
        return request
