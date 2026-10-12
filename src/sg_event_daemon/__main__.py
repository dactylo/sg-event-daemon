"""Run the shared command shell with `python -m sg_event_daemon`."""

import sys

from sg_event_daemon._cli import main

if __name__ == '__main__':
    sys.exit(main())
