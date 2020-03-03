"""
Entrypoint module. This will hijack the script entrypoint to prevent circular
import errors if the top-level module is ran from the command line
(i.e. python -m prodstats)
"""
import sys

import loggers
from manage import main

loggers.config()

if __name__ == "__main__":
    sys.exit(main())
