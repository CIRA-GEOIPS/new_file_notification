"""Unit tests for new_file_notification/get_file_notif.py"""
import json
import sys
import types
import unittest
from unittest.mock import MagicMock, call, patch


# ---------------------------------------------------------------------------
# Stub out the unavailable third-party / internal modules before the import
# ---------------------------------------------------------------------------
def _make_data_inv_api_stub():
    """Return a stub for the data_inv_api package and its sub-module."""
    pkg = types.ModuleType("data_inv_api")
    pkg.DIClient = MagicMock()

    errors = types.ModuleType("data_inv_api.errors")
    errors.DIClientError = type("DIClientError", (Exception,), {})
    errors.DIClientPgError = type("DIClientPgError", (Exception,), {})

    pkg.errors = errors
    sys.modules["data_inv_api"] = pkg
    sys.modules["data_inv_api.errors"] = errors
    return pkg


_make_data_inv_api_stub()

# pika is installed; just ensure consistent mocking per-test via patch.
import pika  # noqa: E402  (must be after stub setup)

# Import the module under test
sys.path.insert(0, "new_file_notification")
import get_file_notif  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _make_channel():
    ch = MagicMock()
    method = MagicMock()
    method.delivery_tag = "tag-1"
    return ch, method


def _body(filepath="/data/store/file.nc", data_store="store1"):
    return json.dumps({"filepath": filepath, "data_store": data_store}).encode()


# ---------------------------------------------------------------------------
# Tests for notif_callback
# ---------------------------------------------------------------------------
class TestNotifCallback(unittest.TestCase):

    def setUp(self):
        self.dic = MagicMock()
        self.dic.find_files.return_value = []
        self.dic.upsert_file.return_value = {"status": "ok"}

    def _call(self, body):
        ch, method = _make_channel()
        get_file_notif.notif_callback(ch, method, MagicMock(), body, self.dic)
        return ch, method

    # --- success path -------------------------------------------------------

    def test_success_calls_upsert_file(self):
        ch, method = self._call(_body("/data/store/file.nc", "store1"))
        self.dic.upsert_file.assert_called_once_with("/data/store/file.nc", "store1")

    def test_success_calls_find_files_twice(self):
        ch, method = self._call(_body("/data/store/file.nc", "store1"))
        self.assertEqual(self.dic.find_files.call_count, 2)
        self.dic.find_files.assert_any_call(filenames="file.nc")

    def test_success_acks_message(self):
        ch, method = _make_channel()
        get_file_notif.notif_callback(
            ch, method, MagicMock(), _body(), self.dic
        )
        ch.basic_ack.assert_called_once_with(delivery_tag=method.delivery_tag)

    def test_success_find_files_logs_rows(self):
        row = {"file_name": "file.nc", "location": "/data", "dir_path": "/data/store"}
        self.dic.find_files.return_value = [row]
        # Should not raise
        self._call(_body())

    # --- exception path -----------------------------------------------------

    def test_exception_still_acks_message(self):
        self.dic.upsert_file.side_effect = RuntimeError("DB error")
        ch, method = _make_channel()
        get_file_notif.notif_callback(
            ch, method, MagicMock(), _body(), self.dic
        )
        ch.basic_ack.assert_called_once_with(delivery_tag=method.delivery_tag)

    def test_exception_uses_filepath_in_log(self):
        self.dic.upsert_file.side_effect = RuntimeError("DB error")
        with self.assertLogs("get_file_notif", level="ERROR"):
            self._call(_body("/some/path/file.nc", "store1"))

    def test_exception_with_none_data_store(self):
        """data_store = None should not raise a second exception."""
        self.dic.upsert_file.side_effect = RuntimeError("DB error")
        ch, method = _make_channel()
        body = json.dumps({"filepath": "/some/file.nc", "data_store": None}).encode()
        # Should not raise
        get_file_notif.notif_callback(ch, method, MagicMock(), body, self.dic)
        ch.basic_ack.assert_called_once()

    def test_exception_with_none_filepath(self):
        """filepath = None should not raise a second exception."""
        self.dic.upsert_file.side_effect = RuntimeError("DB error")
        ch, method = _make_channel()
        body = json.dumps({"filepath": None, "data_store": "store1"}).encode()
        # Should not raise
        get_file_notif.notif_callback(ch, method, MagicMock(), body, self.dic)
        ch.basic_ack.assert_called_once()


# ---------------------------------------------------------------------------
# Tests for connect_to_queue
# ---------------------------------------------------------------------------
class TestConnectToQueue(unittest.TestCase):

    def _config(self, host="rmq-host"):
        cfg = MagicMock()
        cfg.__getitem__.return_value = {"RMQ_HOST": host}
        return cfg

    @patch("get_file_notif.pika.BlockingConnection")
    @patch("get_file_notif.pika.ConnectionParameters")
    @patch("get_file_notif.DIClient")
    def test_returns_channel(self, mock_dic, mock_params, mock_conn):
        mock_channel = MagicMock()
        mock_conn.return_value.channel.return_value = mock_channel
        result = get_file_notif.connect_to_queue(self._config("rmq-host"))
        self.assertIs(result, mock_channel)

    @patch("get_file_notif.pika.BlockingConnection")
    @patch("get_file_notif.pika.ConnectionParameters")
    @patch("get_file_notif.DIClient")
    def test_connection_uses_configured_host(self, mock_dic, mock_params, mock_conn):
        mock_conn.return_value.channel.return_value = MagicMock()
        get_file_notif.connect_to_queue(self._config("my-rmq-server"))
        mock_params.assert_called_once_with(host="my-rmq-server")

    @patch("get_file_notif.pika.BlockingConnection")
    @patch("get_file_notif.pika.ConnectionParameters")
    @patch("get_file_notif.DIClient")
    def test_queue_declared_durable(self, mock_dic, mock_params, mock_conn):
        mock_channel = MagicMock()
        mock_conn.return_value.channel.return_value = mock_channel
        get_file_notif.connect_to_queue(self._config())
        mock_channel.queue_declare.assert_called_once_with(
            queue="file_notif_queue", durable=True
        )

    @patch("get_file_notif.pika.BlockingConnection")
    @patch("get_file_notif.pika.ConnectionParameters")
    @patch("get_file_notif.DIClient")
    def test_diclient_created(self, mock_dic, mock_params, mock_conn):
        mock_conn.return_value.channel.return_value = MagicMock()
        get_file_notif.connect_to_queue(self._config())
        mock_dic.assert_called_once_with(user="geoips")

    @patch("get_file_notif.pika.BlockingConnection")
    @patch("get_file_notif.pika.ConnectionParameters")
    @patch("get_file_notif.DIClient")
    def test_basic_qos_and_consume_registered(self, mock_dic, mock_params, mock_conn):
        mock_channel = MagicMock()
        mock_conn.return_value.channel.return_value = mock_channel
        get_file_notif.connect_to_queue(self._config())
        mock_channel.basic_qos.assert_called_once_with(prefetch_count=1)
        mock_channel.basic_consume.assert_called_once()
        _, kwargs = mock_channel.basic_consume.call_args
        self.assertEqual(kwargs.get("queue"), "file_notif_queue")


# ---------------------------------------------------------------------------
# Tests for consume_notification
# ---------------------------------------------------------------------------
class TestConsumeNotification(unittest.TestCase):

    def _config(self):
        cfg = MagicMock()
        cfg.__getitem__.return_value = {"RMQ_HOST": "host"}
        return cfg

    @patch("get_file_notif.connect_to_queue")
    def test_starts_consuming(self, mock_connect):
        mock_channel = MagicMock()
        # Raise KeyboardInterrupt after the first start_consuming to break loop
        mock_channel.start_consuming.side_effect = KeyboardInterrupt
        mock_connect.return_value = mock_channel

        with self.assertRaises(KeyboardInterrupt):
            get_file_notif.consume_notification(self._config())

        mock_channel.start_consuming.assert_called_once()

    @patch("get_file_notif.connect_to_queue")
    def test_reconnects_on_os_error(self, mock_connect):
        channel1 = MagicMock()
        channel2 = MagicMock()
        # First call returns channel1, second returns channel2
        mock_connect.side_effect = [channel1, channel2]
        # channel1 raises OSError once, then channel2 raises KeyboardInterrupt
        channel1.start_consuming.side_effect = OSError("connection lost")
        channel2.start_consuming.side_effect = KeyboardInterrupt

        with self.assertRaises(KeyboardInterrupt):
            get_file_notif.consume_notification(self._config())

        self.assertEqual(mock_connect.call_count, 2)

    @patch("get_file_notif.connect_to_queue")
    def test_reconnects_on_connection_reset_error(self, mock_connect):
        channel1 = MagicMock()
        channel2 = MagicMock()
        mock_connect.side_effect = [channel1, channel2]
        channel1.start_consuming.side_effect = ConnectionResetError("reset")
        channel2.start_consuming.side_effect = KeyboardInterrupt

        with self.assertRaises(KeyboardInterrupt):
            get_file_notif.consume_notification(self._config())

        self.assertEqual(mock_connect.call_count, 2)


# ---------------------------------------------------------------------------
# Tests for main
# ---------------------------------------------------------------------------
class TestMain(unittest.TestCase):

    @patch("get_file_notif.consume_notification")
    @patch("get_file_notif.configparser.ConfigParser")
    def test_main_calls_consume_notification(self, mock_cfg_cls, mock_consume):
        mock_cfg = MagicMock()
        mock_cfg_cls.return_value = mock_cfg
        with patch("sys.argv", ["get_file_notif.py"]):
            get_file_notif.main()
        mock_consume.assert_called_once_with(mock_cfg)

    @patch("get_file_notif.consume_notification")
    @patch("get_file_notif.configparser.ConfigParser")
    def test_main_reads_config_ini(self, mock_cfg_cls, mock_consume):
        mock_cfg = MagicMock()
        mock_cfg_cls.return_value = mock_cfg
        with patch("sys.argv", ["get_file_notif.py"]):
            get_file_notif.main()
        mock_cfg.read.assert_called_once_with("config.ini")

    @patch("get_file_notif.consume_notification")
    @patch("get_file_notif.configparser.ConfigParser")
    @patch("logging.basicConfig")
    def test_main_verbose_flag_sets_debug(self, mock_logging, mock_cfg_cls, mock_consume):
        mock_cfg_cls.return_value = MagicMock()
        with patch("sys.argv", ["get_file_notif.py", "--verbose"]):
            get_file_notif.main()
        mock_logging.assert_called_once()
        _, kwargs = mock_logging.call_args
        self.assertEqual(kwargs.get("level"), "DEBUG")

    @patch("get_file_notif.consume_notification")
    @patch("get_file_notif.configparser.ConfigParser")
    @patch("logging.basicConfig")
    def test_main_default_log_level_is_info(self, mock_logging, mock_cfg_cls, mock_consume):
        mock_cfg_cls.return_value = MagicMock()
        with patch("sys.argv", ["get_file_notif.py"]):
            get_file_notif.main()
        _, kwargs = mock_logging.call_args
        self.assertEqual(kwargs.get("level"), "INFO")


if __name__ == "__main__":
    unittest.main()
