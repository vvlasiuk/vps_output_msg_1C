from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os

from dotenv import load_dotenv


@dataclass(frozen=True)
class AppConfig:
    rabbitmq_host: str
    rabbitmq_port: int
    rabbitmq_user: str
    rabbitmq_password: str
    rabbitmq_vhost: str
    rabbitmq_source_queue: str
    rabbitmq_result_queue: str
    rabbitmq_result_exchange: str
    rabbitmq_result_routing_key: str
    rabbitmq_heartbeat: int
    onec_server: str
    onec_ref: str
    onec_user: str
    onec_password: str
    onec_connector_prog_id: str
    onec_task_name: str
    poll_interval_sec: float
    task_timeout_sec: float
    loop_sleep_sec: float
    log_level: str
    log_max_bytes: int
    log_backup_count: int
    log_file: Path


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Missing required env var: {name}")
    return value


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)).strip())


def _float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)).strip())


def load_config(env_path: str | None, log_path_override: str | None) -> AppConfig:
    project_root = Path(__file__).resolve().parent
    fallback_env = project_root / ".env"
    resolved_env = Path(env_path).expanduser().resolve() if env_path else fallback_env

    if resolved_env.exists():
        load_dotenv(resolved_env)

    default_log = project_root / os.getenv("LOG_FILE", "service.log")
    log_file = Path(log_path_override).expanduser().resolve() if log_path_override else default_log

    return AppConfig(
        rabbitmq_host=_required("RABBITMQ_HOST"),
        rabbitmq_port=_int("RABBITMQ_PORT", 5672),
        rabbitmq_user=_required("RABBITMQ_USER"),
        rabbitmq_password=_required("RABBITMQ_PASSWORD"),
        rabbitmq_vhost=os.getenv("RABBITMQ_VHOST", "/"),
        rabbitmq_source_queue=os.getenv("RABBITMQ_SOURCE_QUEUE", "output_1c.queue"),
        rabbitmq_result_queue=os.getenv("RABBITMQ_RESULT_QUEUE", "input.queue"),
        rabbitmq_result_exchange=_required("RABBITMQ_RESULT_EXCHANGE"),
        rabbitmq_result_routing_key=os.getenv("RABBITMQ_RESULT_ROUTING_KEY", os.getenv("RABBITMQ_RESULT_QUEUE", "input.queue")),
        rabbitmq_heartbeat=_int("RABBITMQ_HEARTBEAT", 60),
        onec_server=_required("ONEC_SERVER"),
        onec_ref=_required("ONEC_REF"),
        onec_user=_required("ONEC_USER"),
        onec_password=_required("ONEC_PASSWORD"),
        onec_connector_prog_id=os.getenv("ONEC_CONNECTOR_PROG_ID", "V83.COMConnector"),
        poll_interval_sec=_float("POLL_INTERVAL_SEC", 2.0),
        task_timeout_sec=_float("TASK_TIMEOUT_SEC", 300.0),
        loop_sleep_sec=_float("LOOP_SLEEP_SEC", 0.2),
        log_level=os.getenv("LOG_LEVEL", "ERROR").upper(),
        log_max_bytes=_int("LOG_MAX_BYTES", 1_048_576),
        log_backup_count=_int("LOG_BACKUP_COUNT", 5),
        log_file=log_file,
    )
