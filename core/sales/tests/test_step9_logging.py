import json
import logging

from sales.log_format import JsonFormatter


def test_json_log_line_includes_order_context():
    record = logging.LogRecord("sales.tasks", logging.INFO, __file__, 1, "order %s", ("confirmed",), None)
    record.order_id = "abc-123"
    record.sale_id = 7

    line = json.loads(JsonFormatter().format(record))

    assert line["msg"] == "order confirmed"
    assert line["level"] == "INFO"
    assert line["logger"] == "sales.tasks"
    assert line["order_id"] == "abc-123"
    assert line["sale_id"] == 7


def test_json_log_line_includes_exceptions():
    try:
        raise ValueError("boom")
    except ValueError:
        import sys

        record = logging.LogRecord("x", logging.ERROR, __file__, 1, "failed", (), sys.exc_info())
    line = json.loads(JsonFormatter().format(record))
    assert "ValueError: boom" in line["exc"]
