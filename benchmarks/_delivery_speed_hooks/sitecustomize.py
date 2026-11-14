"""Install offline delivery observations only in benchmark-launched children.

The benchmark alone adds this directory to its children's `PYTHONPATH`.
Installation failure terminates the child before provider code can execute.
"""

import logging
import os

_LOGGER = logging.getLogger(__name__)

try:
    from _instrumentation import install

    install()
except BaseException:
    _LOGGER.exception('offline benchmark hook installation failed')
    os._exit(70)
