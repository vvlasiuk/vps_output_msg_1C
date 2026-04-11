from __future__ import annotations

import json
from typing import Any

import pika
from pika.adapters.blocking_connection import BlockingChannel

from config import AppConfig


class RabbitClient:
    def __init__(self, cfg: AppConfig):
        credentials = pika.PlainCredentials(cfg.rabbitmq_user, cfg.rabbitmq_password)
        self._params = pika.ConnectionParameters(
            host=cfg.rabbitmq_host,
            port=cfg.rabbitmq_port,
            virtual_host=cfg.rabbitmq_vhost,
            credentials=credentials,
            heartbeat=cfg.rabbitmq_heartbeat,
        )
        self._source_queue = cfg.rabbitmq_source_queue
        self._result_queue = cfg.rabbitmq_result_queue
        self._connection: pika.BlockingConnection | None = None
        self._channel: BlockingChannel | None = None

    def connect(self) -> None:
        self._connection = pika.BlockingConnection(self._params)
        self._channel = self._connection.channel()

    def close(self) -> None:
        if self._connection and self._connection.is_open:
            self._connection.close()

    def get_one_and_ack_early(self) -> dict[str, Any] | None:
        if self._channel is None:
            raise RuntimeError("Rabbit channel is not connected")

        method_frame, _, body = self._channel.basic_get(queue=self._source_queue, auto_ack=False)
        if method_frame is None:
            return None

        # Requirement decision: acknowledge immediately after read.
        self._channel.basic_ack(delivery_tag=method_frame.delivery_tag)

        payload = json.loads(body.decode("utf-8"))
        return payload

    def publish_result(self, message: dict[str, Any]) -> None:
        if self._channel is None:
            raise RuntimeError("Rabbit channel is not connected")

        body = json.dumps(message, ensure_ascii=False).encode("utf-8")
        self._channel.basic_publish(
            exchange="",
            routing_key=self._result_queue,
            body=body,
            properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
        )
