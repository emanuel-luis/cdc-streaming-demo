"""JSON-lines logging to stdout with UTC ISO timestamps."""

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any

_FIELDS_ATTR = "cdc_fields"


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
        }
        payload.update(getattr(record, _FIELDS_ATTR, {}))
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: int = logging.INFO) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
    # py4j is chatty at INFO and irrelevant for the demo.
    logging.getLogger("py4j").setLevel(logging.WARNING)


def log_event(logger: logging.Logger, event: str, **fields: Any) -> None:
    """Log one event with structured fields (run_id, batch_id, counts...)."""
    logger.info(event, extra={_FIELDS_ATTR: fields})
