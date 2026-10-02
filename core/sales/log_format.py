import json
import logging

# Extra fields we attach with log.info(..., extra={...}) and want as top-level JSON keys.
CONTEXT_FIELDS = ("order_id", "sale_id", "user_id")


class JsonFormatter(logging.Formatter):
    """One JSON object per line, so `docker compose logs` can be grepped by order_id."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": round(record.created, 3),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for field in CONTEXT_FIELDS:
            if hasattr(record, field):
                entry[field] = getattr(record, field)
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)
