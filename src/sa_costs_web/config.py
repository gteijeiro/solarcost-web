from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path


def default_db_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "energy_costs.sqlite3"


@dataclass(slots=True)
class WebConfig:
    bridge_url: str
    bind_host: str
    bind_port: int
    db_path: Path
    secret_key: str
    log_level: str
    http_timeout: float

    @classmethod
    def from_args(cls, argv: list[str] | None = None) -> "WebConfig":
        parser = argparse.ArgumentParser(
            description="Web app para calcular el costo de la luz usando el bridge de SolarAssistant."
        )
        parser.add_argument("--bridge-url", default=os.getenv("SA_COSTS_BRIDGE_URL", "http://127.0.0.1:8765"))
        parser.add_argument("--bind-host", default=os.getenv("SA_COSTS_BIND_HOST", "0.0.0.0"))
        parser.add_argument("--bind-port", type=int, default=int(os.getenv("SA_COSTS_BIND_PORT", "8890")))
        parser.add_argument(
            "--db-path",
            default=os.getenv("SA_COSTS_DB_PATH", str(default_db_path())),
        )
        parser.add_argument(
            "--secret-key",
            default=os.getenv("SA_COSTS_SECRET_KEY", "change-me"),
        )
        parser.add_argument("--log-level", default=os.getenv("SA_COSTS_LOG_LEVEL", "INFO"))
        parser.add_argument(
            "--http-timeout",
            type=float,
            default=float(os.getenv("SA_COSTS_HTTP_TIMEOUT", "10")),
        )
        args = parser.parse_args(argv)

        return cls(
            bridge_url=args.bridge_url.rstrip("/"),
            bind_host=args.bind_host,
            bind_port=args.bind_port,
            db_path=Path(args.db_path).expanduser(),
            secret_key=args.secret_key,
            log_level=args.log_level.upper(),
            http_timeout=args.http_timeout,
        )
