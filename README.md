# vps_output_messages_1C

Minimal Windows Python bridge between RabbitMQ and 1C COM.

## What it does

1. Reads JSON messages from `output_1c.queue`.
2. Acknowledges message immediately after read.
3. Creates 1C task using `VPS.CreateTask("ЗАЛИШКИТОВАРА", Структура)`.
4. Stores `TaskID` and `Storage` in process memory.
5. Polls task status through `ФоновыеЗадания.НайтиПоУникальномуИдентификатору(TaskID)`.
6. Sends final result with `OK` or `ERROR` to `input.queue`.

## Install

```powershell
pip install -r requirements.txt
```

## Configuration

Use external env file:

```powershell
python main.py --env D:\secrets\vps.env --log-path D:\logs\vps_bridge.log
```

If `--env` is not provided, service tries `.env` in project folder.
If `--log-path` is not provided, service writes to project folder using `LOG_FILE`.

See `.env.example` for all settings.

## Input message example

```json
{
  "message_id": "8e6da4f7-6c8b-4e51-86ac-395dc1fa0f5c",
  "article": "YT-47160",
  "params": {
    "Склад": "Основний"
  }
}
```

## Output message example

```json
{
  "message_id": "8e6da4f7-6c8b-4e51-86ac-395dc1fa0f5c",
  "task_id": "0f4aa6b6-9ebd-11ee-8f3b-00155d010300",
  "storage": "StorageA",
  "status": "OK",
  "error": null
}
```
