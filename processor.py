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
                    started_monotonic=time.monotonic(),
                    next_poll_monotonic=time.monotonic(),
                    source=incoming.source,
                    destination=incoming.destination,
                )
            )
        except Exception as exc:
            self._logger.error("Failed to create 1C task: %s", exc, exc_info=True)
            fallback_message_id = str(payload.get("message_id", "")) if payload is not None else str(uuid4())
            source = incoming.source if incoming is not None else None
            destination = incoming.destination if incoming is not None else None

            if payload is not None:
                if source is None:
                    raw_source = payload.get("source")
                    source = None if raw_source is None else str(raw_source).strip() or None
                if destination is None:
                    raw_destination = payload.get("destination", payload.get("destanation"))
                    destination = None if raw_destination is None else str(raw_destination).strip() or None

            outgoing = OutgoingMessage(
                message_id=fallback_message_id,
                task_id=None,
                storage=None,
                status="ERROR",
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
                    message_id=item.message_id,
                    task_id=item.task_id,
                    storage=item.storage,
                    status="ERROR",
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
                is_done, status, error = self._onec.get_task_state(item.task_id, item.storage)
            except Exception as exc:
                self._logger.error("Failed to poll task %s: %s", item.task_id, exc, exc_info=True)
                outgoing = OutgoingMessage(
                    message_id=item.message_id,
                    task_id=item.task_id,
                    storage=item.storage,
                    status="ERROR",
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

            outgoing = OutgoingMessage(
                message_id=item.message_id,
                task_id=item.task_id,
                storage=item.storage,
                status=status,
                error=error,
                source=item.source,
                destination=item.destination,
            )
            self._safe_publish(outgoing)

        self._tasks = remaining

    @staticmethod
    def _parse_incoming(payload: dict[str, Any]) -> IncomingMessage:
        message_id = str(payload.get("message_id") or uuid4())

        command_name = str(payload.get("command_name") or "").strip()
        if not command_name:
            raise ValueError("Missing required field: command_name")

        params = payload.get("params")
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

        source = payload.get("source")
        destination = payload.get("destination", payload.get("destanation"))

        source = None if source is None else str(source).strip() or None
        destination = None if destination is None else str(destination).strip() or None

        return IncomingMessage(
            message_id=message_id,
            command_name=command_name,
            params=normalized_params,
            source=source,
            destination=destination,
        )

    def _safe_publish(self, outgoing: OutgoingMessage) -> None:
        try:
            self._rabbit.publish_result(asdict(outgoing))
        except Exception as exc:
            self._logger.error("Failed to publish result: %s", exc, exc_info=True)
