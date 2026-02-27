# Initial code for running docker

"""Legacy launcher that forwards to the ``spark_app`` package.

This module remains for backwards compatibility; new code should import
from ``spark_app`` directly.
"""

from __future__ import annotations

import sys

from spark_app.main import main


if __name__ == "__main__":
    sys.exit(main())
