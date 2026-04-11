from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class IncomingMessage:
    message_id: str
    article: str
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class TaskRecord:
    message_id: str
    task_id: str
    storage: str
    started_monotonic: float
    next_poll_monotonic: float


@dataclass
class OutgoingMessage:
    message_id: str
    task_id: str | None
    storage: str | None
    status: str
    error: str | None
