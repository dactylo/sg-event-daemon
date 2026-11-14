from __future__ import annotations

import os
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


def publish_text(path: Path, value: str) -> None:
    pending = path.with_name(f'.{path.name}.{os.getpid()}.{threading.get_ident()}.pending')
    try:
        pending.write_text(value, encoding='utf-8')
        pending.replace(path)
    finally:
        pending.unlink(missing_ok=True)


def read_complete_lines(path: Path) -> list[str]:
    try:
        return path.read_text(encoding='utf-8').split('\n')[:-1]
    except FileNotFoundError:
        return []
