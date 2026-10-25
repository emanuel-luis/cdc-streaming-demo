import io
import json
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from cdc_demo import contract
from cdc_demo.cli import app
from cdc_demo.proto import value_proto_text

REGISTERED: dict[str, Any] = json.loads(
    (Path(__file__).parent / "fixtures" / "registry" / "orders-value-latest.json").read_text(
        encoding="utf-8"
    )
)
SUBJECT = "cdc.public.orders-value"


class FakeResponse(io.BytesIO):
    pass


def fake_registry(
    registered: dict[str, Any] | None = None,
    compatible: bool = True,
    calls: list[Any] | None = None,
) -> contract.Opener:
    def opener(req: urllib.request.Request) -> FakeResponse:
        if calls is not None:
            calls.append(req)
        if "/compatibility/" in req.full_url:
            body = {
                "is_compatible": compatible,
                "messages": [] if compatible else ["field removed"],
            }
        else:
            body = registered or REGISTERED
        return FakeResponse(json.dumps(body).encode())

    return opener


def test_normalize_drops_comments_blanks_and_trailing_space() -> None:
    assert contract.normalize("// c\nsyntax = 1;  \n\n  // x\nmessage A {}\n") == [
        "syntax = 1;",
        "message A {}",
    ]


def test_checked_in_proto_equals_the_registry_fixture() -> None:
    assert contract.normalize(value_proto_text()) == contract.normalize(REGISTERED["schema"])


def test_check_passes_and_sends_the_expected_requests() -> None:
    calls: list[Any] = []
    result = contract.check_schema("http://sr:8081/", SUBJECT, opener=fake_registry(calls=calls))
    assert result.ok and (result.schema_id, result.version) == (2, 1)
    get, post = calls
    assert get.get_method() == "GET"
    assert get.full_url == f"http://sr:8081/subjects/{SUBJECT}/versions/latest"
    assert post.get_method() == "POST"
    assert post.full_url.endswith(f"/compatibility/subjects/{SUBJECT}/versions/latest?verbose=true")
    assert post.get_header("Content-type") == contract.CONTENT_TYPE
    body = json.loads(post.data)
    assert body["schemaType"] == "PROTOBUF" and body["schema"] == value_proto_text()


def test_a_changed_schema_is_reported_with_a_diff() -> None:
    changed = {
        **REGISTERED,
        "schema": REGISTERED["schema"].replace(
            "string op = 5;", "string op = 5;\n  string extra = 9;"
        ),
    }
    result = contract.check_schema("http://sr", SUBJECT, opener=fake_registry(changed))
    assert result.compatible and not result.identical and not result.ok
    assert "-  string extra = 9;" in result.diff
    assert "identical:  no" in contract.format_report(result)


def test_incompatible_is_reported_with_messages() -> None:
    result = contract.check_schema("http://sr", SUBJECT, opener=fake_registry(compatible=False))
    assert not result.compatible and not result.ok
    assert "field removed" in contract.format_report(result)


def test_non_protobuf_subject_is_an_error() -> None:
    avro = {**REGISTERED, "schemaType": "AVRO"}
    with pytest.raises(contract.RegistryError, match="expected PROTOBUF"):
        contract.check_schema("http://sr", SUBJECT, opener=fake_registry(avro))


def test_http_error_becomes_registry_error() -> None:
    def opener(req: urllib.request.Request) -> FakeResponse:
        raise urllib.error.HTTPError(
            req.full_url, 404, "Not Found", None, io.BytesIO(b'{"error_code":40401}')
        )  # type: ignore[arg-type]

    with pytest.raises(contract.RegistryError, match="HTTP 404"):
        contract.check_schema("http://sr", SUBJECT, opener=opener)


def test_cli_exit_codes(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    monkeypatch.setenv("SCHEMA_REGISTRY_URL", "http://sr:8081")
    monkeypatch.setattr(
        contract.urllib.request, "urlopen", lambda req, timeout=None: fake_registry()(req)
    )
    ok = runner.invoke(app, ["check-schema"])
    assert ok.exit_code == 0 and "OK" in ok.output

    def down(req: urllib.request.Request, timeout: float | None = None) -> FakeResponse:
        raise urllib.error.URLError("connection refused")

    monkeypatch.setattr(contract.urllib.request, "urlopen", down)
    bad = runner.invoke(app, ["check-schema"])
    assert bad.exit_code == 1
