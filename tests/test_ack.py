"""Tests of robustness with respect to message correctness and database
availability.
"""

import logging
from unittest.mock import Mock

from pika.spec import Basic, BasicProperties

from new_file_notification import get_file_notif


def test_callback_nack_not_json(caplog):
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
