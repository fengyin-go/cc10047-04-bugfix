"""湖表版本归属服务的命名兼容模块。"""
from __future__ import annotations

from app.ingest_coordinator import coordinator
from app.services.delta_commit import DeltaCommitService


class LakeTableVersionService(DeltaCommitService):
    """提交版本服务同时承担湖表版本归属查询/校验。"""


lake_table_version_service = LakeTableVersionService()

__all__ = ["LakeTableVersionService", "coordinator", "lake_table_version_service"]
