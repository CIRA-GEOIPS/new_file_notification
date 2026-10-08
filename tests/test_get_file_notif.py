from functools import partial
import importlib
import logging
import sys
import types
from unittest.mock import MagicMock

import pika
import pytest
import stamina


@pytest.fixture(autouse=True, scope="session")
def limit_retries():
    stamina.set_testing(True, attempts=10)
    yield
    stamina.set_testing(False)


@pytest.fixture
def notif_module(monkeypatch):
    """Import target module with a stubbed data_inv_api dependency."""
    data_inv_api = types.ModuleType("data_inv_api")
    diapi = types.ModuleType("data_inv_api.pg_di_client")
    errors = types.ModuleType("data_inv_api.errors")

    class FakeDIClient:
        def __init__(self, user):
            self.user = user

    class FakeDIClientError(Exception):
        pass

    class FakeDIClientPgError(Exception):
        pass

    data_inv_api.DIClient = FakeDIClient
    errors.DIClientError = FakeDIClientError
    errors.DIClientPgError = FakeDIClientPgError

    monkeypatch.setitem(sys.modules, "data_inv_api", data_inv_api)
    monkeypatch.setitem(sys.modules, "data_inv_api.pg_di_client", diapi)
    monkeypatch.setitem(sys.modules, "data_inv_api.errors", errors)

    get_file_notif = sys.modules.pop("new_file_notification.get_file_notif", None)
    module = importlib.import_module("new_file_notification.get_file_notif")
    yield module

    sys.modules["new_file_notification.get_file_notif"] = get_file_notif


def test_connect_to_queue_declares_and_binds_callback(notif_module, monkeypatch):
    config = {"Settings": {"RMQ_HOST": "rabbitmq-host"}}
    fake_channel = MagicMock()
    fake_connection = MagicMock()
    fake_connection.channel.return_value = fake_channel

    captured = {}

    def fake_connection_params(host):
        captured["host"] = host
        return f"params:{host}"

    def fake_blocking_connection(params):
        captured["params"] = params
        return fake_connection

    monkeypatch.setattr(notif_module.pika, "ConnectionParameters", fake_connection_params)
    monkeypatch.setattr(notif_module.pika, "BlockingConnection", fake_blocking_connection)

    channel = notif_module.connect_to_queue(config)

    assert channel is fake_channel
    assert captured["host"] == "rabbitmq-host"
    assert captured["params"] == "params:rabbitmq-host"
    fake_channel.queue_declare.assert_called_once_with(queue="file_notif_queue", durable=True)
    fake_channel.basic_qos.assert_called_once_with(prefetch_count=1)
    fake_channel.basic_consume.assert_called_once()

    kwargs = fake_channel.basic_consume.call_args.kwargs
    assert kwargs["queue"] == "file_notif_queue"
    assert isinstance(kwargs["on_message_callback"], partial)
    assert kwargs["on_message_callback"].func is notif_module.notif_callback
    assert kwargs["on_message_callback"].keywords["dic"].user == "geoips"


def test_consume_notification_reconnects_on_amqp_connection_error(notif_module, monkeypatch):
    """Retry a failed connection and succeed on the second attempt."""
    config = {"Settings": {"RMQ_HOST": "rabbitmq-host"}}
    fake_connect_to_queue = MagicMock()
    fake_channel = MagicMock()
    fake_connect_to_queue.side_effect = [
        pika.exceptions.AMQPConnectionError("lost connection"),
        fake_channel,
    ]

    monkeypatch.setattr(notif_module, "connect_to_queue", fake_connect_to_queue)

    notif_module.consume_notification(config)

    assert fake_connect_to_queue.call_count == 2
    fake_channel.start_consuming.assert_called_once()
