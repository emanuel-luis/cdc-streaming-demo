"""Compare the checked-in Protobuf value schema with the one in Schema Registry.

Standard library only: the mutator image has no pyspark and runs this too.
"""

import difflib
import json
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from cdc_demo.proto import value_proto_text

CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"
TIMEOUT_SECONDS = 10

Opener = Callable[[urllib.request.Request], Any]


class RegistryError(RuntimeError):
    """The registry could not be reached or answered with an error."""


@dataclass(frozen=True)
class CheckResult:
    subject: str
    schema_id: int
    version: int
    compatible: bool
    messages: list[str]
    identical: bool
    diff: str

    @property
    def ok(self) -> bool:
        return self.compatible and self.identical


def normalize(schema: str) -> list[str]:
    """Schema lines without full-line // comments, trailing whitespace and blank lines."""
    lines = (line.rstrip() for line in schema.splitlines())
    return [line for line in lines if line and not line.lstrip().startswith("//")]


def schema_diff(local: str, registered: str) -> str:
    return "\n".join(
        difflib.unified_diff(
            normalize(registered),
            normalize(local),
            fromfile="registered",
            tofile="checked-in",
            lineterm="",
        )
    )


def _call(opener: Opener, url: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(
        url,
        data=data,
        method="GET" if body is None else "POST",
        headers={"Accept": CONTENT_TYPE, "Content-Type": CONTENT_TYPE},
    )
    try:
        with opener(request) as response:
            result: dict[str, Any] = json.load(response)
            return result
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")[:300]
        raise RegistryError(f"{request.get_method()} {url} -> HTTP {exc.code}: {detail}") from exc
    except (urllib.error.URLError, OSError, ValueError) as exc:
        raise RegistryError(f"{request.get_method()} {url} failed: {exc}") from exc


def check_schema(
    registry_url: str,
    subject: str,
    local: str | None = None,
    opener: Opener | None = None,
) -> CheckResult:
    local = value_proto_text() if local is None else local
    opener = opener or (lambda req: urllib.request.urlopen(req, timeout=TIMEOUT_SECONDS))
    base = registry_url.rstrip("/")
    latest = _call(opener, f"{base}/subjects/{subject}/versions/latest")
    if latest.get("schemaType") != "PROTOBUF":
        raise RegistryError(f"subject {subject} is {latest.get('schemaType')}, expected PROTOBUF")
    compat = _call(
        opener,
        f"{base}/compatibility/subjects/{subject}/versions/latest?verbose=true",
        {"schemaType": "PROTOBUF", "schema": local},
    )
    diff = schema_diff(local, latest["schema"])
    return CheckResult(
        subject=subject,
        schema_id=latest["id"],
        version=latest["version"],
        compatible=bool(compat.get("is_compatible")),
        messages=[str(m) for m in compat.get("messages", [])],
        identical=not diff,
        diff=diff,
    )


def format_report(result: CheckResult) -> str:
    lines = [
        f"subject:    {result.subject}",
        f"registered: id {result.schema_id}, version {result.version}",
        f"compatible: {'yes' if result.compatible else 'no'}",
        *(f"  {m}" for m in result.messages),
        f"identical:  {'yes' if result.identical else 'no'}",
    ]
    if result.diff:
        lines.append(result.diff)
    lines.append("OK" if result.ok else "FAILED: update the .proto and run `make proto`")
    return "\n".join(lines)
