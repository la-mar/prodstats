import os
import sys

# __file__ = os.getcwd() + "/test/__init__.py"
sys.path.append(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__)), os.pardir + "/src/prodstats"
    )
)
