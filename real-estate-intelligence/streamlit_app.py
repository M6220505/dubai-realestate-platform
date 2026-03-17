import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "real-estate-intelligence"))

import os
os.chdir(Path(__file__).parent / "real-estate-intelligence")

exec(open("dashboard.py").read())
