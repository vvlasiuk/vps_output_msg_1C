from __future__ import annotations

import json
from typing import Any

import pythoncom
import win32com.client

from config import AppConfig


class OneCClient:
    def __init__(self, cfg: AppConfig):
        self._cfg = cfg
        self._session = None
        self._inited = False

    def connect(self) -> None:
        pythoncom.CoInitialize()
        self._inited = True

        connection_string = f'Srvr={self._cfg.onec_server};Ref={self._cfg.onec_ref};Usr={self._cfg.onec_user};Pwd={self._cfg.onec_password};'
        self._session = win32com.client.Dispatch(self._cfg.onec_connector_prog_id).Connect(connection_string)

    def close(self) -> None:
        self._session = None
        if self._inited:
            try:
                pythoncom.CoUninitialize()
            except Exception:
                pass
            self._inited = False

    def _new_structure(self):
        if self._session is None:
            raise RuntimeError("1C session is not connected")
        return self._session.NewObject("Structure")

    @staticmethod
    def _insert_to_structure(structure: Any, key: str, value: Any) -> None:
        try:
            structure.Insert(key, value)
        except Exception:
            structure.Вставить(key, value)

    def create_task(self, task_name: str, params: dict[str, Any]) -> tuple[str, str]:
        if self._session is None:
            raise RuntimeError("1C session is not connected")

        structure = self._new_structure()
        for key, value in params.items():
            self._insert_to_structure(structure, key, value)

        task = self._session.VPS.CreateTask(task_name, structure)

        raw_status = None
        raw_error = None

        if isinstance(task, dict):
            raw_status = task.get("status")
            raw_error = task.get("text_error") or task.get("error")
        else:
            for attr in ("status", "Status", "Статус"):
                try:
                    raw_status = getattr(task, attr)
                    break
                except Exception:
                    pass
            for attr in ("text_error", "TextError", "ТекстОшибки", "error", "Error"):
                try:
                    raw_error = getattr(task, attr)
                    break
                except Exception:
                    pass

        if raw_status is not None:
            status_value = str(raw_status).strip().upper()
            if status_value not in {"OK", "SUCCESS", "TRUE", "1"}:
                raise RuntimeError(str(raw_error or "1C CreateTask error"))

        task_id = str(task.TaskID)
        storage = str(task.Storage)
        return task_id, storage

    def get_task_state(
        self,
        task_id: str,
        storage: str,
    ) -> tuple[bool, str | None, dict[str, Any] | None, Any | None]:
        if self._session is None:
            raise RuntimeError("1C session is not connected")

        status_result = self._session.VPS.StatusTask(task_id, storage)

        # if isinstance(status_result, dict):
        #     status_value = str(status_result.get("status", "")).upper()
        #     error_value = status_result.get("error")
        #     if status_value == "OK":
        #         return True, "OK", None
        #     if status_value == "ERROR":
        #         return True, "ERROR", str(error_value or "1C task error")
        #     if status_value == "RUN":
        #         return False, "", None

        if isinstance(status_result, str):
            text = status_result.strip()
            try:
                parsed = json.loads(text)
            except Exception:
                parsed = None

            if isinstance(parsed, dict):
                status_value = str(parsed.get("status", "")).upper()
                error_value = parsed.get("error")
                if status_value == "OK":
                    command_value = parsed.get("command")
                    command_payload = command_value if isinstance(command_value, dict) else None
                    data_value = parsed.get("DATA", parsed.get("data"))
                    # if isinstance(data_value, list) and len(data_value) == 1 and isinstance(data_value[0], dict):
                    #     data_value = data_value[0]
                    # elif not isinstance(data_value, dict):
                    #     data_value = {}
                    return True, None, command_payload, data_value
                if status_value == "ERROR":
                    return True, str(error_value or "1C task error"), None, None
                if status_value == "RUN":
                    return False, None, None, None

        if isinstance(status_result, bool):
            return (True, None, None, None) if status_result else (False, None, None, None)

        raw_status = str(status_result)
        normalized = raw_status.upper()

        if normalized in {"TRUE", "1", "OK", "DONE", "FINISHED", "SUCCESS"}:
            return True, None, None, None
        if normalized in {"FALSE", "0", "PENDING", "INPROGRESS", "IN_PROGRESS", "RUNNING"}:
            return False, None, None, None
        if "ОШИБ" in normalized or "ERROR" in normalized or "FAIL" in normalized:
            return True, raw_status or "1C task error", None, None

        # Fallback for object-like statuses returned by 1C.
        for attr in ("Status", "Статус", "State", "Состояние"):
            try:
                attr_value = str(getattr(status_result, attr))
            except Exception:
                continue

            state = attr_value.upper()
            if "ОШИБ" in state or "ERROR" in state or "FAIL" in state:
                return True, attr_value or "1C task error", None, None
            if "OK" in state or "DONE" in state or "ЗАВЕРШ" in state:
                return True, None, None, None

        return False, None, None, None
