from __future__ import annotations

import os
from pathlib import Path


def subprocess_env() -> dict[str, str]:
    environment = dict(os.environ)
    environment['PYTHONPATH'] = os.fspath(Path(__file__).parent.parent / 'src')
    return environment
