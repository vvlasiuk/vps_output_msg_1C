from __future__ import annotations

import argparse
import sys

from config import load_config
from logger_setup import setup_logger
from processor import Processor


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="RabbitMQ to 1C COM bridge")
    parser.add_argument("--env", dest="env_path", default=None, help="Path to env file")
    parser.add_argument(
        "--log-path",
        dest="log_path",
        default=None,
        help="Path to rotating log file",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cfg = load_config(args.env_path, args.log_path)
    logger = setup_logger(cfg.log_level, cfg.log_file, cfg.log_max_bytes, cfg.log_backup_count)

    processor = Processor(cfg, logger)
    processor.run_forever()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(0)
    except Exception as exc:
        print(f"Fatal error: {exc}", file=sys.stderr)
        raise SystemExit(1)
