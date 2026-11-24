"""Pytest 隔离：每个用例前把内存仓库恢复为初始种子。"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.store import store  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_in_memory_store():
    store.reset()
    yield
