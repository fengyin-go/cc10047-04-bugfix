"""批次归属协调服务的命名兼容模块。"""
from __future__ import annotations

from app.ingest_coordinator import IngestCoordinator, coordinator
from app.services.ingest_batch import IngestBatchService


class BatchOwnershipService(IngestBatchService):
    pass


batch_ownership_service = BatchOwnershipService()

__all__ = ["BatchOwnershipService", "IngestCoordinator", "coordinator", "batch_ownership_service"]
