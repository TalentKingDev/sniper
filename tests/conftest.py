"""Pytest configuration: add project root to sys.path for imports."""
from __future__ import annotations

import sys
from pathlib import Path

# Add project root so 'from models import ...' etc. work
root = Path(__file__).resolve().parent.parent
if str(root) not in sys.path:
    sys.path.insert(0, str(root))
