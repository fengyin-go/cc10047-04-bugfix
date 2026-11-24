"""批次领取与归属服务的命名兼容模块。"""
from __future__ import annotations

from app.ingest_coordinator import IngestCoordinator, coordinator
from app.services.stream_ingest import StreamIngestService


class IngestBatchService(StreamIngestService):
    """StreamIngestService 的批次视角别名。"""


ingest_batch_service = IngestBatchService()

__all__ = ["IngestBatchService", "IngestCoordinator", "coordinator", "ingest_batch_service"]
