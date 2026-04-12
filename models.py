from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class IncomingMessage:
    message_id: str
    command_name: str
    params: dict[str, Any] = field(default_factory=dict)
    source: str | None = None
    destination: str | None = None


@dataclass
class TaskRecord:
    message_id: str
    task_id: str
    storage: str
    started_monotonic: float
    next_poll_monotonic: float
    source: str | None = None
    destination: str | None = None


@dataclass
class OutgoingMessage:
    message_id: str
    task_id: str | None
    storage: str | None
    status: str
    error: str | None
    source: str | None = None
    destination: str | None = None
