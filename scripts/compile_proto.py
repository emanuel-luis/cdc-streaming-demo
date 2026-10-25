"""Compile src/cdc_demo/proto/orders_value.proto into the committed descriptor set.

Run through `make proto`. Needs the dev dependency grpcio-tools.
"""

import sys
from pathlib import Path

from grpc_tools import protoc

PROTO_DIR = Path(__file__).resolve().parent.parent / "src" / "cdc_demo" / "proto"


def compile_descriptor(proto_dir: Path, out: Path) -> None:
    code = protoc.main(
        [
            "protoc",
            f"--proto_path={proto_dir}",
            f"--descriptor_set_out={out}",
            "--include_imports",
            str(proto_dir / "orders_value.proto"),
        ]
    )
    if code != 0:
        raise SystemExit(code)


if __name__ == "__main__":
    compile_descriptor(PROTO_DIR, PROTO_DIR / "orders_value.desc")
    sys.stdout.write(f"wrote {PROTO_DIR / 'orders_value.desc'}\n")
