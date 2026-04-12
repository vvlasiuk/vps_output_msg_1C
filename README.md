# vps_output_messages_1C

Minimal Windows Python bridge between RabbitMQ and 1C COM.

## What it does

1. Reads JSON messages from `output_1c.queue`.
2. Acknowledges message immediately after read.
3. Creates 1C task using `VPS.CreateTask(command_name, Структура)`.
4. Stores `TaskID` and `Storage` in process memory.
5. Polls task status through `VPS.StatusTask(TaskID, Storage)`.
6. Sends final result to `input.queue` without `status` field.

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

For result publishing, configure exchange and routing key:

```env
RABBITMQ_RESULT_EXCHANGE=input.events
RABBITMQ_RESULT_ROUTING_KEY=input.queue
```

`RABBITMQ_RESULT_EXCHANGE` is required.

Required input fields:

1. `command_name` + `params` (flat format), or
2. `command.name` + `command.params` (nested format), and
3. `params` must contain at least one key-value pair that will be forwarded to 1C as-is

Optional input tags:

1. `source`
2. `destination` (or legacy `destanation`)

If present, these tags are propagated to the output message (string or object).

## Input message example

```json
{
  "message_id": "8e6da4f7-6c8b-4e51-86ac-395dc1fa0f5c",
  "command_name": "ЗАЛИШКИТОВАРА",
  "source": "crm",
  "destination": "1c",
  "params": {
    "article": "YT-47160"
  }
}
```

## Input message example (Telegram-style)

```json
{
  "source": {
    "system": "telegram",
    "source_id": "vlsv_bot",
    "chat_id": "1011471535",
    "user_id": "1011471535",
    "username": "vlsv00",
    "message_id": "1770",
    "timestamp": "2026-04-12T15:45:46+02:00",
    "group_id": null
  },
  "destination": {
    "system": "telegram",
    "chat_id": "1011471535"
  },
  "command": {
    "name": "ЗАЛИШКИТОВАРА",
    "params": {
      "article": "SBG100"
    }
  }
}
```

## Output message example (success)

```json
{
  "command": {
    "name": "ЗАЛИШКИТОВАРА",
    "params": {
      "article": "SBG100"
    }
  },
  "DATA": {
    "items": []
  },
  "source": "crm",
  "destination": "1c"
}
```

## Output message example (error)

```json
{
  "command": {
    "name": "ЗАЛИШКИТОВАРА",
    "params": {
      "article": "SBG100"
    }
  },
  "error": "1C task error",
  "source": "crm",
  "destination": "1c"
}
```
