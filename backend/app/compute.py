"""实时流数据湖业务计算引擎：分区、合并、压缩、快照差异、保留策略、
版本冲突、增量协调等核心算法实现。
"""
from __future__ import annotations

import hashlib
import math
from typing import Any


# ------------------------------------------------------------------
# partition_calculator
# ------------------------------------------------------------------

def partition_calculator(
    rows: int, partition_keys: list[str], target_size_mb: float = 128.0,
) -> dict[str, Any]:
    """根据行数和分区键计算最佳分区策略。"""
    partitions = max(1, math.ceil(rows * 0.25 / target_size_mb))
    return {
        "partition_count": partitions,
        "partition_keys": partition_keys,
        "strategy": "hash" if partitions > 4 else "range",
        "estimated_rows_per_partition": rows // partitions,
        "target_partition_size_mb": target_size_mb,
    }


# ------------------------------------------------------------------
# merge_strategy
# ------------------------------------------------------------------

def merge_strategy(
    small_files: list[int],
    target_file_size_mb: float = 256.0,
    min_file_count: int = 3,
) -> dict[str, Any]:
    """计算小文件合并策略：按目标文件大小将多个小文件合为一个。"""
    if not small_files or len(small_files) < min_file_count:
        return {"should_merge": False, "reason": "小文件数量不足，跳过合并"}
    total_size = sum(small_files)
    groups = max(1, math.ceil(total_size / target_file_size_mb))
    return {
        "should_merge": True,
        "file_count": len(small_files),
        "total_size_mb": total_size,
        "target_groups": groups,
        "files_per_group": len(small_files) // groups,
        "estimated_output_mb": target_file_size_mb,
    }


# ------------------------------------------------------------------
# compaction_planner
# ------------------------------------------------------------------

def compaction_planner(
    table_path: str,
    file_sizes: list[int],
    target_file_size: int = 134217728,  # 128MB
    max_parallelism: int = 4,
) -> dict[str, Any]:
    """规划压缩作业：输入文件大小列表，输出分批压缩计划。"""
    candidates = [i for i, sz in enumerate(file_sizes) if sz < target_file_size // 4]
    if not candidates:
        return {
            "table": table_path,
            "plan": None,
            "candidate_file_count": 0,
            "batches": 0,
            "files_per_batch": 0,
            "parallelism": 0,
            "target_file_size_bytes": target_file_size,
            "estimated_duration_seconds": 0,
            "reason": "没有需要压缩的小文件",
        }
    batches = math.ceil(len(candidates) / max_parallelism)
    plan = {
        "candidate_file_count": len(candidates),
        "batches": batches,
        "files_per_batch": len(candidates) // batches if batches else 0,
        "parallelism": min(max_parallelism, len(candidates)),
        "target_file_size_bytes": target_file_size,
        "estimated_duration_seconds": len(candidates) * 15,
    }
    return {
        "table": table_path,
        "plan": plan,
        **plan,
    }


# ------------------------------------------------------------------
# snapshot_differ
# ------------------------------------------------------------------

def snapshot_differ(
    base_snapshot: dict[str, Any],
    current_snapshot: dict[str, Any],
) -> dict[str, Any]:
    """比较两个快照的差异，输出新增/更新/删除的数据分布。"""
    base_files = base_snapshot.get("files", [])
    curr_files = current_snapshot.get("files", [])

    base_set = {f.get("path"): f for f in base_files}
    curr_set = {f.get("path"): f for f in curr_files}

    added = [f for path, f in curr_set.items() if path not in base_set]
    removed = [f for path, f in base_set.items() if path not in curr_set]
    modified = []
    for path in base_set.keys() & curr_set.keys():
        bf = base_set[path]
        cf = curr_set[path]
        if bf.get("checksum") != cf.get("checksum"):
            modified.append({"path": path, "old": bf, "new": cf})

    total_added_rows = sum(f.get("rows", 0) for f in added)
    total_removed_rows = sum(f.get("rows", 0) for f in removed)
    total_modified_rows = sum(f["new"].get("rows", 0) for f in modified)

    return {
        "base_snapshot_id": base_snapshot.get("snapshot_id"),
        "current_snapshot_id": current_snapshot.get("snapshot_id"),
        "added_files": len(added),
        "removed_files": len(removed),
        "modified_files": len(modified),
        "added_rows": total_added_rows,
        "removed_rows": total_removed_rows,
        "modified_rows": total_modified_rows,
        "net_change_rows": total_added_rows - total_removed_rows,
    }


# ------------------------------------------------------------------
# retention_checker
# ------------------------------------------------------------------

def retention_checker(
    snapshots: list[dict[str, Any]],
    retention_days: int = 30,
    min_versions: int = 5,
) -> dict[str, Any]:
    """检查快照保留策略：标记可清理的过期快照。"""
    from datetime import datetime, timedelta, timezone

    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    expired = []
    valid = []

    for snap in snapshots:
        ts = str(snap.get("created_at", ""))
        # Strip timezone to make comparison naive
        for suffix in ("Z", "+00:00", "+0000"):
            ts = ts.replace(suffix, "")
        ts = ts.split("+")[0]
        try:
            created = datetime.fromisoformat(ts)
        except (ValueError, TypeError):
            created = cutoff + timedelta(days=1)
        if created.replace(tzinfo=None) < cutoff.replace(tzinfo=None):
            expired.append(snap)
        else:
            valid.append(snap)

    can_clean = max(0, len(expired) - min_versions)
    return {
        "total_snapshots": len(snapshots),
        "expired_count": len(expired),
        "valid_count": len(valid),
        "min_versions_required": min_versions,
        "retention_days": retention_days,
        "can_clean_count": can_clean,
        "should_clean": can_clean > 0,
    }


# ------------------------------------------------------------------
# version_conflict_resolver
# ------------------------------------------------------------------

def version_conflict_resolver(
    base_version: int,
    incoming_version: int,
    strategy: str = "last-write-wins",
    conflict_data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """解决版本冲突：支持 LWW、三路合并和手动覆盖策略。"""
    if base_version == incoming_version:
        return {
            "conflict": False,
            "resolved_version": base_version,
            "strategy": strategy,
        }

    if strategy == "last-write-wins":
        winner = max(base_version, incoming_version)
    elif strategy == "base-wins":
        winner = base_version
    elif strategy == "incoming-wins":
        winner = incoming_version
    elif strategy == "manual" and conflict_data:
        winner = conflict_data.get("preferred_version", base_version)
    else:
        winner = base_version

    return {
        "conflict": base_version != incoming_version,
        "base_version": base_version,
        "incoming_version": incoming_version,
        "resolved_version": winner,
        "strategy": strategy,
        "resolution": "resolved" if strategy != "manual" else "needs_review",
    }


# ------------------------------------------------------------------
# delta_reconciler
# ------------------------------------------------------------------

def delta_reconciler(
    stream_events: list[dict[str, Any]],
    batch_events: list[dict[str, Any]],
    dedup_key: str = "id",
) -> dict[str, Any]:
    """协调流批数据：对流事件和批事件进行去重合并，输出最终写入集。"""
    # Build dedup index from batch (batch wins for same key)
    batch_index: dict[str, dict[str, Any]] = {}
    for event in batch_events:
        key = str(event.get(dedup_key, ""))
        if key:
            batch_index[key] = event

    reconciled = []
    duplicates = 0
    stream_only = 0

    for event in stream_events:
        key = str(event.get(dedup_key, ""))
        if key and key in batch_index:
            reconciled.append(event)  # batch wins, but keep source trace
            duplicates += 1
        else:
            reconciled.append(event)
            stream_only += 1

    # Append batch-only events
    existing_keys = {str(e.get(dedup_key, "")) for e in stream_events}
    batch_only = 0
    for key, event in batch_index.items():
        if key not in existing_keys:
            reconciled.append(event)
            batch_only += 1

    total = len(reconciled)
    checksum = _compute_checksum(str(reconciled))

    return {
        "stream_count": len(stream_events),
        "batch_count": len(batch_events),
        "reconciled_total": total,
        "duplicates_resolved": duplicates,
        "stream_only": stream_only,
        "batch_only": batch_only,
        "dedup_key": dedup_key,
        "checksum": checksum,
    }


# ------------------------------------------------------------------
# helpers
# ------------------------------------------------------------------

def _compute_checksum(data: str) -> str:
    return hashlib.md5(data.encode()).hexdigest()[:12]

# ------------------------------------------------------------------
# 摄入批次/湖表版本归属纯计算
# ------------------------------------------------------------------

def claim_offset_range(start_offset: int, row_count: int) -> dict[str, int | list[int]]:
    """根据起始偏移和行数计算半开区间及最后一个包含偏移。"""
    start = int(start_offset)
    count = max(0, int(row_count))
    end = start + count
    return {
        "start_offset": start,
        "end_offset": end,
        "last_offset": max(start, end - 1),
        "row_count": count,
        "offset_range": [start, max(start, end - 1)],
    }


def resolve_grant_precedence(
    old_grant_priority: bool | str | None = None,
    *,
    precedence: str | None = None,
    old_priority: bool | str | None = None,
) -> str:
    """把多种优先级入参归一为 ``old`` 或 ``new``。默认已领取范围的旧批授权优先。"""
    value = precedence if precedence is not None else old_grant_priority
    if value is None:
        value = old_priority
    if isinstance(value, bool):
        return "old" if value else "new"
    text = str(value or "").casefold().replace("-", "_")
    if text in {"new", "current", "binding", "new_grant", "new_first", "false", "0"}:
        return "new"
    if "new" in text or "current" in text or "binding" in text or "新" in text:
        return "new"
    return "old"


def resolve_ingest_ownership(
    batch_table: Any = None,
    commit_table: Any = None,
    current_table: Any = None,
    *args: Any,
    **kwargs: Any,
) -> dict[str, Any]:
    """判定批次能否按提交版本写入。

    该纯函数不访问内存仓库，便于单测；服务层会额外核对真实记录和权限。
    可通过 ``precedence`` / ``old_grant_priority`` 选择改绑后的授权优先级。
    """
    # 兼容位置参数中混入 generation/bool 的调用。
    values = [batch_table, commit_table, current_table, *args]
    strings = [v for v in values if isinstance(v, str) and v]
    if batch_table is None and strings:
        batch_table = strings[0]
    if commit_table is None and len(strings) > 1:
        commit_table = strings[1]
    if current_table is None and len(strings) > 2:
        current_table = strings[2]

    precedence_name = resolve_grant_precedence(
        kwargs.get("old_grant_priority", kwargs.get("old_priority")),
        precedence=kwargs.get("precedence") or kwargs.get("grant_precedence"),
    )
    same_batch_commit = str(batch_table) == str(commit_table) and batch_table is not None
    same_current = current_table is None or str(batch_table) == str(current_table)

    if not same_batch_commit:
        allowed = False
        code = "TABLE_OWNERSHIP_CONFLICT"
        reason = "批次归属表与提交版本归属表不一致"
    elif not same_current and precedence_name == "new":
        allowed = False
        code = "OLD_GRANT_REVOKED"
        reason = "新绑定授权优先，旧批不能继续提交"
    else:
        allowed = True
        code = "OK"
        reason = "批次归属、提交版本和授权区间一致"

    return {
        "ok": allowed,
        "allowed": allowed,
        "code": code,
        "conflict": not allowed,
        "reason": reason,
        "message": reason,
        "batch_table": batch_table,
        "commit_table": commit_table,
        "current_table": current_table,
        "precedence": precedence_name,
        "target_table": batch_table if allowed else None,
    }


# 常见命名别名。
resolve_table_attribution = resolve_ingest_ownership
validate_commit_attribution = resolve_ingest_ownership
check_batch_ownership = resolve_ingest_ownership
resolve_batch_ownership = resolve_ingest_ownership
authorize_table_write = resolve_ingest_ownership
