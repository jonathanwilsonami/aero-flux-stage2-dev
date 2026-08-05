"""Make scripts/ importable in tests.

scripts/*.py import each other by bare module name (e.g. build_dataset.py
does `from run_parser import ...`), matching how they're invoked directly
(`python scripts/build_dataset.py`), which puts the script's own dir on
sys.path. Pytest doesn't do that, so add it explicitly.
"""
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
