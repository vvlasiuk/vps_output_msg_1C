from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class IncomingMessage:
    message_id: str
    command_name: str
    params: dict[str, Any] = field(default_factory=dict)
    source: Any | None = None
    destination: Any | None = None


@dataclass
class TaskRecord:
    message_id: str
    task_id: str
    storage: str
    command_name: str
    started_monotonic: float
    next_poll_monotonic: float
    params: dict[str, Any] = field(default_factory=dict)
    source: Any | None = None
    destination: Any | None = None


@dataclass
class OutgoingMessage:
    command: dict[str, Any] | None = None
    DATA: Any | None = None
    error: str | None = None
    source: Any | None = None
    destination: Any | None = None
