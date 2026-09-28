"""A small disk-backed, latest-value-only research baseline."""

from .store import Store, StoreError

__all__ = ["Store", "StoreError"]
