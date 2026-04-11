from __future__ import annotations

import json
import time
from typing import Any

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import (
    AMQPConnectionError,
    ChannelWrongStateError,
    DuplicateGetOkCallback,
    StreamLostError,
)

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
        self._result_exchange = cfg.rabbitmq_result_exchange
        self._result_routing_key = cfg.rabbitmq_result_routing_key
        self._connection: pika.BlockingConnection | None = None
        self._channel: BlockingChannel | None = None

    def connect(self) -> None:
        self._connection = pika.BlockingConnection(self._params)
        self._channel = self._connection.channel()

    def close(self) -> None:
        if self._connection and self._connection.is_open:
            self._connection.close()
        self._connection = None
        self._channel = None

    def _is_connected(self) -> bool:
        return bool(
            self._connection
            and self._connection.is_open
            and self._channel
            and self._channel.is_open
        )

    def _ensure_connected(self) -> None:
        if self._is_connected():
            return
        self.close()
        self.connect()

    def _reconnect(self) -> None:
        self.close()
        time.sleep(0.2)
        self.connect()

    def get_one_and_ack_early(self) -> dict[str, Any] | None:
        self._ensure_connected()
        assert self._channel is not None

        try:
            method_frame, _, body = self._channel.basic_get(
                queue=self._source_queue,
                auto_ack=False,
            )
        except (DuplicateGetOkCallback, ChannelWrongStateError, AMQPConnectionError, StreamLostError):
            self._reconnect()
            assert self._channel is not None
            method_frame, _, body = self._channel.basic_get(
                queue=self._source_queue,
                auto_ack=False,
            )

        if method_frame is None:
            return None

        self._channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        return json.loads(body.decode("utf-8"))

    def publish_result(self, message: dict[str, Any]) -> None:
        self._ensure_connected()
        assert self._channel is not None

        body = json.dumps(message, ensure_ascii=False).encode("utf-8")
        try:
            self._channel.basic_publish(
                exchange=self._result_exchange,
                routing_key=self._result_routing_key,
                body=body,
                properties=pika.BasicProperties(
                    content_type="application/json",
                    delivery_mode=2,
                ),
            )
        except (ChannelWrongStateError, AMQPConnectionError, StreamLostError):
            self._reconnect()
            assert self._channel is not None
            self._channel.basic_publish(
                exchange=self._result_exchange,
                routing_key=self._result_routing_key,
                body=body,
                properties=pika.BasicProperties(
                    content_type="application/json",
                    delivery_mode=2,
                ),
            )
