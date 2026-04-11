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
        try:
            payload = self._rabbit.get_one_and_ack_early()
            if payload is None:
                return
            incoming = self._parse_incoming(payload)

            command_name = "ЗАЛИШКИТОВАРА"    

            task_id, storage = self._onec.create_task(command_name, incoming.params)

            self._tasks.append(
                TaskRecord(
                    message_id=incoming.message_id,
                    task_id=task_id,
                    storage=storage,
                    started_monotonic=time.monotonic(),
                    next_poll_monotonic=time.monotonic(),
                )
            )
        except Exception as exc:
            self._logger.error("Failed to create 1C task: %s", exc, exc_info=True)
            fallback_message_id = str(payload.get("message_id", "")) if payload is not None else str(uuid4())
            outgoing = OutgoingMessage(
                message_id=fallback_message_id,
                task_id=None,
                storage=None,
                status="ERROR",
                error=str(exc),
            )
            self._safe_publish(outgoing)

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
                )
                self._safe_publish(outgoing)
                continue

            if now < item.next_poll_monotonic:
                remaining.append(item)
                continue

            try:
                is_done, status, error = self._onec.get_task_state(item.task_id)
            except Exception as exc:
                self._logger.error("Failed to poll task %s: %s", item.task_id, exc, exc_info=True)
                outgoing = OutgoingMessage(
                    message_id=item.message_id,
                    task_id=item.task_id,
                    storage=item.storage,
                    status="ERROR",
                    error=str(exc),
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
            )
            self._safe_publish(outgoing)

        self._tasks = remaining

    @staticmethod
    def _parse_incoming(payload: dict[str, Any]) -> IncomingMessage:
        message_id = str(payload.get("message_id") or uuid4())

        article = payload.get("article") or payload.get("Артикул")
        if not article:
            raise ValueError("Missing required field: article")

        params = payload.get("params")
        if params is None:
            params = {}
        if not isinstance(params, dict):
            raise ValueError("Field 'params' must be an object")

        # Always keep article inside 1C structure.
        if "Артикул" not in params:
            params["Артикул"] = article

        return IncomingMessage(message_id=message_id, article=str(article), params=params)

    def _safe_publish(self, outgoing: OutgoingMessage) -> None:
        try:
            self._rabbit.publish_result(asdict(outgoing))
        except Exception as exc:
            self._logger.error("Failed to publish result: %s", exc, exc_info=True)
