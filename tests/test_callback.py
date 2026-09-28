"""Tests of robustness with respect to message correctness and database
availability.
"""

import json
import logging
from unittest.mock import Mock

from data_inv_api.errors import DIClientError, DIClientPgError
from pika.spec import Basic, BasicProperties
import pytest

from new_file_notification import get_file_notif


def test_callback_not_json(caplog):
    """Callback nacks with no requeuing when message contains no JSON."""
    pika_channel = Mock()
    data_inv_client = Mock()
    method = Basic.Deliver()
    properties = BasicProperties()
    body = "bogus".encode("utf-8")

    caplog.set_level(logging.ERROR)

    get_file_notif.notif_callback(
        pika_channel, method, properties, body, data_inv_client
    )

    pika_channel.basic_nack.assert_called_with(
        delivery_tag=method.delivery_tag, requeue=False
    )
    assert "Rejected non-JSON message" in caplog.text


@pytest.mark.parametrize("info", [[], {}, 5, "bogus"])
def test_callback_non_conforming_json(caplog, info):
    """Callback nacks with no requeuing when JSON message doesn't match file info spec."""
    pika_channel = Mock()
    data_inv_client = Mock()
    method = Basic.Deliver()
    properties = BasicProperties()
    body = json.dumps(info).encode("utf-8")
    caplog.set_level(logging.ERROR)

    get_file_notif.notif_callback(
        pika_channel, method, properties, body, data_inv_client
    )

    pika_channel.basic_nack.assert_called_with(
        delivery_tag=method.delivery_tag, requeue=False
    )
    assert "Rejected JSON message" in caplog.text


def test_callback_upsert_failure_bad_message(caplog):
    """Callback nacks with no requeuing when the file info msg can't be processed."""
    pika_channel = Mock()
    data_inv_client = Mock()
    conf = {"find_files.return_value": [], "upsert_file.side_effect": DIClientError}
    data_inv_client.configure_mock(**conf)
    method = Basic.Deliver()
    properties = BasicProperties()

    file_info = {"filepath": "bogus", "data_store": "bogus"}
    body = json.dumps(file_info).encode("utf-8")
    caplog.set_level(logging.ERROR)

    get_file_notif.notif_callback(
        pika_channel, method, properties, body, data_inv_client
    )

    pika_channel.basic_nack.assert_called_with(
        delivery_tag=method.delivery_tag, requeue=False
    )
    assert "Rejected unprocessable message" in caplog.text


def test_callback_upsert_failure_database_error(caplog):
    """Callback nacks with requeuing when the file database doesn't respond."""
    pika_channel = Mock()
    data_inv_client = Mock()
    conf = {"find_files.return_value": [], "upsert_file.side_effect": DIClientPgError}
    data_inv_client.configure_mock(**conf)
    method = Basic.Deliver()
    properties = BasicProperties()

    file_info = {"filepath": "bogus", "data_store": "bogus"}
    body = json.dumps(file_info).encode("utf-8")
    caplog.set_level(logging.ERROR)

    get_file_notif.notif_callback(
        pika_channel, method, properties, body, data_inv_client
    )

    pika_channel.basic_nack.assert_called_with(
        delivery_tag=method.delivery_tag, requeue=True
    )
    assert "Database connection failed" in caplog.text


def test_callback_find_files_failure_database_error(caplog):
    """Callback nacks with requeuing when the file database doesn't respond."""
    pika_channel = Mock()
    data_inv_client = Mock()
    conf = {"find_files.side_effect": DIClientPgError}
    data_inv_client.configure_mock(**conf)
    method = Basic.Deliver()
    properties = BasicProperties()

    file_info = {"filepath": "bogus", "data_store": "bogus"}
    body = json.dumps(file_info).encode("utf-8")
    caplog.set_level(logging.ERROR)

    get_file_notif.notif_callback(
        pika_channel, method, properties, body, data_inv_client
    )

    pika_channel.basic_nack.assert_called_with(
        delivery_tag=method.delivery_tag, requeue=True
    )
    assert "Database connection failed" in caplog.text


def test_callback_reraise_unexpected_exceptions(caplog):
    """Re-raise unexpected exceptions after nack with requeue."""
    pika_channel = Mock()
    data_inv_client = Mock()
    conf = {"find_files.side_effect": RuntimeError}
    data_inv_client.configure_mock(**conf)
    method = Basic.Deliver()
    properties = BasicProperties()

    file_info = {"filepath": "bogus", "data_store": "bogus"}
    body = json.dumps(file_info).encode("utf-8")
    caplog.set_level(logging.ERROR)

    with pytest.raises(RuntimeError):
        get_file_notif.notif_callback(
            pika_channel, method, properties, body, data_inv_client
        )

    pika_channel.basic_nack.assert_called_with(
        delivery_tag=method.delivery_tag, requeue=True
    )
    assert "callback failed" in caplog.text
