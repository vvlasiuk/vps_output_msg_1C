from __future__ import annotations

import logging
import time
from dataclasses import asdict
from typing import Any
from uuid import uuid4

from config import AppConfig
from models import IncomingMessage, OutgoingMessage, TaskRecord
from onec_client import OneCClient
from rabbit_client import RabbitClient
import json

class Processor:
    def __init__(self, cfg: AppConfig, logger: logging.Logger):
        self._cfg = cfg
        self._logger = logger
        self._rabbit = RabbitClient(cfg)
        self._onec = OneCClient(cfg)
        self._tasks: list[TaskRecord] = []

    def run_forever(self) -> None:
        self._rabbit.connect()
        self._onec.connect()

        try:
            while True:
                self._try_consume_and_create_task()
                self._poll_tasks()
                time.sleep(self._cfg.loop_sleep_sec)
        finally:
            self._onec.close()
            self._rabbit.close()

    def _try_consume_and_create_task(self) -> None:
        payload: dict[str, Any] | None = None
        incoming: IncomingMessage | None = None
        try:
            payload = self._rabbit.get_one_and_ack_early()
            if payload is None:
                return
            incoming = self._parse_incoming(payload)

            task_id, storage = self._onec.create_task(incoming.command_name, incoming.params)

            self._tasks.append(
                TaskRecord(
                    message_id=incoming.message_id,
                    task_id=task_id,
                    storage=storage,
                    command_name=incoming.command_name,
                    started_monotonic=time.monotonic(),
                    next_poll_monotonic=time.monotonic(),
                    params=incoming.params,
                    source=incoming.source,
                    destination=incoming.destination,
                )
            )
        except Exception as exc:
            self._logger.error("Failed to create 1C task: %s", exc, exc_info=True)
            source = incoming.source if incoming is not None else None
            destination = incoming.destination if incoming is not None else None

            if payload is not None:
                if source is None:
                    source = Processor._normalize_route_tag(payload.get("source"))
                if destination is None:
                    raw_destination = payload.get("destination", payload.get("destanation"))
                    destination = Processor._normalize_route_tag(raw_destination)

            outgoing = OutgoingMessage(
                error=str(exc),
                source=source,
                destination=destination,
            )
            self._safe_publish(outgoing)
            # Prevent tight error loop when Rabbit/1C is temporarily unavailable.
            time.sleep(1.0)

    def _poll_tasks(self) -> None:
        if not self._tasks:
            return

        now = time.monotonic()
        remaining: list[TaskRecord] = []

        for item in self._tasks:
            elapsed = now - item.started_monotonic
            if elapsed > self._cfg.task_timeout_sec:
                outgoing = OutgoingMessage(
                    error="Task status timeout",
                    source=item.source,
                    destination=item.destination,
                )
                self._safe_publish(outgoing)
                continue

            if now < item.next_poll_monotonic:
                remaining.append(item)
                continue

            try:
                is_done, error, command, data = self._onec.get_task_state(item.task_id, item.storage)
            except Exception as exc:
                self._logger.error("Failed to poll task %s: %s", item.task_id, exc, exc_info=True)
                outgoing = OutgoingMessage(
                    error=str(exc),
                    source=item.source,
                    destination=item.destination,
                )
                self._safe_publish(outgoing)
                continue

            if not is_done:
                item.next_poll_monotonic = now + self._cfg.poll_interval_sec
                remaining.append(item)
                continue

            if command is not None:
                command = {
                    "name": command,
                    "params": {},
                }

            data = json.loads(data)

            outgoing = OutgoingMessage(
                command=command,
                data=data,
                error=error,
                source=item.source,
                destination=item.destination,
            )
            self._safe_publish(outgoing)

        self._tasks = remaining

    @staticmethod
    def _parse_incoming(payload: dict[str, Any]) -> IncomingMessage:
        message_id = Processor._extract_message_id(payload)

        command = payload.get("command")
        raw_command_name = payload.get("command_name")
        raw_params = payload.get("params")

        if isinstance(command, dict):
            raw_command_name = command.get("name", raw_command_name)
            raw_params = command.get("params", raw_params)

        command_name = str(raw_command_name or "").strip()
        if not command_name:
            raise ValueError("Missing required field: command_name")

        params = raw_params
        if params is None:
            params = {}
        if not isinstance(params, dict):
            raise ValueError("Field 'params' must be an object")

        normalized_params: dict[str, Any] = {}
        for raw_key, value in params.items():
            key = str(raw_key).strip()
            if not key:
                continue
            normalized_params[key] = value

        if not normalized_params:
            raise ValueError("Field 'params' must contain at least one key-value pair")

        source = Processor._normalize_route_tag(payload.get("source"))
        destination = Processor._normalize_route_tag(
            payload.get("destination", payload.get("destanation"))
        )

        return IncomingMessage(
            message_id=message_id,
            command_name=command_name,
            params=normalized_params,
            source=source,
            destination=destination,
        )

    @staticmethod
    def _extract_message_id(payload: dict[str, Any] | None) -> str:
        if payload is None:
            return str(uuid4())

        raw_message_id = payload.get("message_id")
        if raw_message_id is None:
            source = payload.get("source")
            if isinstance(source, dict):
                raw_message_id = source.get("message_id")

        if raw_message_id is None:
            return str(uuid4())

        message_id = str(raw_message_id).strip()
        return message_id or str(uuid4())

    @staticmethod
    def _normalize_route_tag(value: Any) -> Any | None:
        if value is None:
            return None

        if isinstance(value, dict):
            normalized: dict[str, Any] = {}
            for raw_key, item_value in value.items():
                key = str(raw_key).strip()
                if not key:
                    continue
                normalized[key] = item_value
            return normalized or None

        if isinstance(value, str):
            normalized_text = value.strip()
            return normalized_text or None

        return value

    def _safe_publish(self, outgoing: OutgoingMessage) -> None:
        try:
            payload = {k: v for k, v in asdict(outgoing).items() if v is not None}
            self._rabbit.publish_result(payload)
        except Exception as exc:
            self._logger.error("Failed to publish result: %s", exc, exc_info=True)
