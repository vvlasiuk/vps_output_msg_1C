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

        #connector = win32com.client.Dispatch(self._cfg.onec_connector_prog_id)
        #conn_str = (
        #    f"Srvr={self._cfg.onec_server};"
        #    f"Ref={self._cfg.onec_ref};"
        #    f"Usr={self._cfg.onec_user};"
        #    f"Pwd={self._cfg.onec_password};"
        #)
        #self._session = connector.Connect(conn_str)

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
        task_id = str(task.TaskID)
        storage = str(task.Storage)
        return task_id, storage

    def get_task_state(self, task_id: str) -> tuple[bool, str, str | None]:
        if self._session is None:
            raise RuntimeError("1C session is not connected")

        status_result = self._session.VPS.StatusTask(task_id)

        if isinstance(status_result, dict):
            status_value = str(status_result.get("status", "")).upper()
            error_value = status_result.get("error")
            if status_value == "OK":
                return True, "OK", None
            if status_value == "ERROR":
                return True, "ERROR", str(error_value or "1C task error")
            if status_value == "RUN":
                return False, "", None

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
                    return True, "OK", None
                if status_value == "ERROR":
                    return True, "ERROR", str(error_value or "1C task error")
                if status_value == "RUN":
                    return False, "", None

        if isinstance(status_result, bool):
            return (True, "OK", None) if status_result else (False, "", None)

        raw_status = str(status_result)
        normalized = raw_status.upper()

        if normalized in {"TRUE", "1", "OK", "DONE", "FINISHED", "SUCCESS"}:
            return True, "OK", None
        if normalized in {"FALSE", "0", "PENDING", "INPROGRESS", "IN_PROGRESS", "RUNNING"}:
            return False, "", None
        if "ОШИБ" in normalized or "ERROR" in normalized or "FAIL" in normalized:
            return True, "ERROR", raw_status or "1C task error"

        # Fallback for object-like statuses returned by 1C.
        for attr in ("Status", "Статус", "State", "Состояние"):
            try:
                attr_value = str(getattr(status_result, attr))
            except Exception:
                continue

            state = attr_value.upper()
            if "ОШИБ" in state or "ERROR" in state or "FAIL" in state:
                return True, "ERROR", attr_value or "1C task error"
            if "OK" in state or "DONE" in state or "ЗАВЕРШ" in state:
                return True, "OK", None

        return False, "", None
