import pytest
import requests_mock
from urllib.parse import urljoin


@pytest.fixture
def local_url():
    return "http://localhost:3000/api/v3/"


@pytest.fixture
def catamel_token():
    return "a_token"


@pytest.fixture
def mock_request(local_url, catamel_token):
    with requests_mock.Mocker() as mock:
        mock.post(
            urljoin(local_url, "Users/login"),
            json={"id": catamel_token},
        )
        yield mock


@pytest.fixture
def client(mock_request, local_url):
    from ..client import ScicatClient

    return ScicatClient(
        base_url=local_url,
        username="Zaphod",
        password="heartofgold",
    )
