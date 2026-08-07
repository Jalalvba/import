"""Regression test for the highest-priority test in the repo: api/index.py's
token check must genuinely reject a wrong/missing token before touching
Drive/Mongo, and accept a correct one. Opus's mutation testing proved this
exact check (mutating hmac.compare_digest away) "turns the live endpoint
into an open trigger for production Mongo writes" with zero detection --
this test exists specifically to catch that class of regression.

Instantiates the handler without a real socket (BaseHTTPRequestHandler's
__init__ needs one) via object.__new__, then calls _handle_trigger()
directly with a monkeypatched _respond() to capture the response instead
of writing to a socket."""
from unittest.mock import patch

import api.index as api_index


def _make_handler(path: str):
    handler = object.__new__(api_index.handler)
    handler.path = path
    captured = {}

    def fake_respond(status_code, body):
        captured["status"] = status_code
        captured["body"] = body

    handler._respond = fake_respond
    return handler, captured


def test_wrong_token_is_rejected_before_touching_pipelines(monkeypatch):
    monkeypatch.setenv("PIPELINE_TRIGGER_SECRET", "the-real-secret")
    handler, captured = _make_handler("/api?token=wrong-guess")

    with patch.object(api_index, "_run_all_pipelines") as mock_run:
        handler._handle_trigger()

    assert captured["status"] == 401
    assert captured["body"]["success"] is False
    mock_run.assert_not_called()  # never touches Drive/Mongo on a bad token


def test_missing_token_is_rejected(monkeypatch):
    monkeypatch.setenv("PIPELINE_TRIGGER_SECRET", "the-real-secret")
    handler, captured = _make_handler("/api")

    with patch.object(api_index, "_run_all_pipelines") as mock_run:
        handler._handle_trigger()

    assert captured["status"] == 401
    mock_run.assert_not_called()


def test_missing_server_side_secret_is_rejected(monkeypatch):
    # No PIPELINE_TRIGGER_SECRET configured at all -- must fail closed,
    # never treat an unset secret as "any token is fine."
    monkeypatch.delenv("PIPELINE_TRIGGER_SECRET", raising=False)
    handler, captured = _make_handler("/api?token=anything")

    with patch.object(api_index, "_run_all_pipelines") as mock_run:
        handler._handle_trigger()

    assert captured["status"] == 401
    mock_run.assert_not_called()


def test_correct_token_is_accepted(monkeypatch):
    monkeypatch.setenv("PIPELINE_TRIGGER_SECRET", "the-real-secret")
    handler, captured = _make_handler("/api?token=the-real-secret")

    with patch.object(api_index, "_run_all_pipelines", return_value=[]), \
         patch.object(api_index, "check_trigger_rate_limit", return_value=True):
        handler._handle_trigger()

    assert captured["status"] == 200
    assert captured["body"]["success"] is True
