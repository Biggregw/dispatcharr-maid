import importlib

import pytest


@pytest.fixture(autouse=True)
def clear_env(monkeypatch):
    for key in ["DISPATCHARR_BASE_URL", "DISPATCHARR_USER", "DISPATCHARR_PASS", "DISPATCHARR_TOKEN"]:
        monkeypatch.delenv(key, raising=False)


def _reload_api_utils():
    import api_utils

    return importlib.reload(api_utils)


def test_dispatcharr_api_allows_token_without_credentials(monkeypatch):
    monkeypatch.setenv("DISPATCHARR_BASE_URL", "https://example.com")
    monkeypatch.setenv("DISPATCHARR_TOKEN", "abc123")

    api_utils = _reload_api_utils()

    api = api_utils.DispatcharrAPI()

    assert api.base_url == "https://example.com"
    assert api.token == "abc123"
    assert api.username is None
    assert api.password is None


def test_dispatcharr_api_requires_credentials_when_no_token(monkeypatch):
    monkeypatch.setenv("DISPATCHARR_BASE_URL", "https://example.com")

    api_utils = _reload_api_utils()

    with pytest.raises(ValueError):
        api_utils.DispatcharrAPI()


def test_login_requires_username_and_password(monkeypatch):
    monkeypatch.setenv("DISPATCHARR_BASE_URL", "https://example.com")
    monkeypatch.setenv("DISPATCHARR_TOKEN", "abc123")

    api_utils = _reload_api_utils()

    api = api_utils.DispatcharrAPI()
    api.username = None
    api.password = None

    with pytest.raises(ValueError):
        api.login()
