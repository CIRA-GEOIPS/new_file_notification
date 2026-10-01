"""Tests of robustness with respect to message correctness and database
availability.
"""

import json
import logging
from unittest.mock import Mock, patch

from data_inv_api.errors import DIClientError, DIClientPgError
from pika.spec import Basic, BasicProperties
import pytest

from new_file_notification import get_file_notif


@pytest.mark.parametrize("body", ["bogus", 5, '"bögus"'.encode("Windows-1252")])
def test_callback_not_json(caplog, body):
    """Callback nacks with no requeuing when message contains no JSON."""
    pika_channel = Mock()
    data_inv_client = Mock()
    method = Basic.Deliver()
    properties = BasicProperties()

    caplog.set_level(logging.ERROR)

    get_file_notif.notif_callback(
        pika_channel, method, properties, body, dic=data_inv_client
    )

    pika_channel.basic_nack.assert_called_with(
        delivery_tag=method.delivery_tag, requeue=False
    )
    assert "Rejected message that was not bytes, not UTF, or not JSON" in caplog.text


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
        pika_channel, method, properties, body, dic=data_inv_client
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
        pika_channel, method, properties, body, dic=data_inv_client
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
    caplog.set_level(logging.INFO)

    get_file_notif.notif_callback(
        pika_channel,
        method,
        properties,
        body,
        dic=data_inv_client,
    )

    pika_channel.basic_nack.assert_called_with(
        delivery_tag=method.delivery_tag, requeue=True
    )
    assert "Database connection failed" in caplog.text
    assert "Nack sent after delay" in caplog.text


@pytest.mark.parametrize("delivery_count", [0, 1])
@pytest.mark.parametrize(
    "config",
    [
        {
            "Settings": {
                "REQUEUE_DELAY_SECONDS": 3.0,
                "REQUEUE_BACKOFF_FACTOR": 10.0,
                "REQUEUE_DELAY_LIMIT_SECONDS": 60.0,
            }
        },
        {
            "Settings": {
                "REQUEUE_DELAY_SECONDS": 10.0,
                "REQUEUE_BACKOFF_FACTOR": 10.0,
                "REQUEUE_DELAY_LIMIT_SECONDS": 60.0,
            }
        },
    ],
)
@patch("new_file_notification.get_file_notif.time")
def test_callback_upsert_failure_database_error_backoff(
    time, config, delivery_count, caplog
):
    """Callback nacks with configurable backoff when the file database doesn't respond."""
    pika_channel = Mock()
    data_inv_client = Mock()
    conf = {"find_files.return_value": [], "upsert_file.side_effect": DIClientPgError}
    data_inv_client.configure_mock(**conf)
    method = Basic.Deliver()
    properties = BasicProperties(headers={"x-delivery-count": delivery_count})

    file_info = {"filepath": "bogus", "data_store": "bogus"}
    body = json.dumps(file_info).encode("utf-8")
    caplog.set_level(logging.INFO)

    get_file_notif.notif_callback(
        pika_channel,
        method,
        properties,
        body,
        dic=data_inv_client,
        config=config,
    )

    # Check the delay.
    time.sleep.assert_called_once()
    _, (seconds,), _ = time.sleep.mock_calls[0]
    requeue_delay_seconds = config["Settings"]["REQUEUE_DELAY_SECONDS"]
    requeue_backoff_factor = config["Settings"]["REQUEUE_BACKOFF_FACTOR"]
    requeue_delay_limit_seconds = config["Settings"]["REQUEUE_DELAY_LIMIT_SECONDS"]
    assert (
        requeue_delay_seconds
        <= seconds
        <= min(
            requeue_delay_limit_seconds,
            requeue_delay_seconds * (requeue_backoff_factor**delivery_count) + 1.0,
        )
    )

    pika_channel.basic_nack.assert_called_with(
        delivery_tag=method.delivery_tag, requeue=True
    )
    assert "Database connection failed" in caplog.text
    assert "Nack sent after delay" in caplog.text


def test_callback_find_files_database_error(caplog):
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
        pika_channel, method, properties, body, dic=data_inv_client
    )

    pika_channel.basic_nack.assert_called_with(
        delivery_tag=method.delivery_tag, requeue=True
    )
    assert "Database connection failed" in caplog.text


def test_callback_find_files_mount_info_failure(caplog):
    """Callback nacks with requeuing when mount info can't be read."""
    pika_channel = Mock()
    data_inv_client = Mock()
    conf = {"find_files.side_effect": FileNotFoundError}
    data_inv_client.configure_mock(**conf)
    method = Basic.Deliver()
    properties = BasicProperties()

    file_info = {"filepath": "bogus", "data_store": "bogus"}
    body = json.dumps(file_info).encode("utf-8")
    caplog.set_level(logging.ERROR)

    get_file_notif.notif_callback(
        pika_channel, method, properties, body, dic=data_inv_client
    )

    pika_channel.basic_nack.assert_called_with(
        delivery_tag=method.delivery_tag, requeue=True
    )
    assert "Mount info not found" in caplog.text


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
            pika_channel, method, properties, body, dic=data_inv_client
        )

    pika_channel.basic_nack.assert_called_with(
        delivery_tag=method.delivery_tag, requeue=True
    )
    assert "callback failed" in caplog.text


def test_callback_success(caplog):
    """On success ack is sent."""
    pika_channel = Mock()
    data_inv_client = Mock()
    conf = {"find_files.return_value": []}
    data_inv_client.configure_mock(**conf)
    method = Basic.Deliver()
    properties = BasicProperties()

    file_info = {"filepath": "bogus", "data_store": "bogus"}
    body = json.dumps(file_info).encode("utf-8")
    caplog.set_level(logging.INFO)

    get_file_notif.notif_callback(
        pika_channel, method, properties, body, dic=data_inv_client
    )

    pika_channel.basic_ack.assert_called_with(delivery_tag=method.delivery_tag)
    assert "[x] Done" in caplog.text
