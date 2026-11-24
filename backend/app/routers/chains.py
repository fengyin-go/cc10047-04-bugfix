"""跨模块业务链路：把多个领域服务的 domain_xxx 方法串联为端到端业务流程。"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.services import service_for

router = APIRouter(prefix="/api/chains", tags=["业务链路"])

# ------------------------------------------------------------------
# 链路1: 实时流入
# stream_source→stream_ingest→commit_version→delta_ingest
# ------------------------------------------------------------------

@router.post("/stream-ingest")
def chain_stream_ingest(
    source_id: int,
    ingest_id: int,
    commit_id: int,
    batch_size: int = 1000,
    rows: int | None = None,
    claimed_rows: int | None = None,
    owner: str | None = None,
    writer: str | None = None,
) -> dict:
    """实时流入链路：探测源Schema → 批次摄入 → 提交版本 → 增量摄入。"""
    effective_batch_size = claimed_rows or rows or batch_size
    steps = []

    # Step 1: 探测源Schema
    svc_src = service_for("stream_source")
    if not svc_src:
        raise HTTPException(404, "stream_source 服务未注册")
    r1 = svc_src.domain_detect_schema(source_id)
    steps.append({"step": "detect_schema", "ok": r1["ok"], "details": r1})
    if not r1["ok"]:
        return {"chain": "stream-ingest", "ok": False, "steps": steps, "error": r1.get("message")}

    # Step 2: 批次摄入
    svc_ingest = service_for("stream_ingest")
    if not svc_ingest:
        raise HTTPException(404, "stream_ingest 服务未注册")
    r2 = svc_ingest.domain_ingest_batch(
        ingest_id,
        batch_size=effective_batch_size,
        source_id=source_id,
        owner=owner or writer,
        principal=writer or owner,
    )
    steps.append({"step": "ingest_batch", "ok": r2["ok"], "details": r2})
    if not r2["ok"]:
        return {"chain": "stream-ingest", "ok": False, "steps": steps, "error": r2.get("message")}

    # Step 3: 提交版本。必须把领取批次的来源/偏移/目标表交接给提交条目，
    # 由 delta_commit 按版本归属记录校验实际摄入对象，不能再独立确认成功。
    svc_commit = service_for("delta_commit")
    if not svc_commit:
        raise HTTPException(404, "delta_commit 服务未注册")
    handoff = r2.get("handoff") or {
        "batch_id": r2.get("batch_id"),
        "pipeline_id": ingest_id,
        "source_id": source_id,
        "target_table": r2.get("target_table"),
    }
    r3 = svc_commit.domain_commit(
        commit_id,
        batch=handoff,
        source_id=source_id,
        expected_table=r2.get("target_table"),
        owner=owner or writer,
        principal=writer or owner or "stream-ingest-chain",
    )
    steps.append({"step": "commit_version", "ok": r3["ok"], "details": r3})
    if not r3["ok"]:
        return {"chain": "stream-ingest", "ok": False, "steps": steps, "error": r3.get("message")}

    # Step 4: 增量摄入。继续携带同一批次和归属，避免增量写到另一张表。
    r4 = svc_commit.domain_ingest_delta(
        commit_id,
        delta_records=500,
        batch=r3.get("batch") or handoff,
        batch_id=r3.get("batch_id") or handoff.get("batch_id"),
        source_id=source_id,
        table=r3.get("target_table") or r2.get("target_table"),
    )
    steps.append({"step": "delta_ingest", "ok": r4["ok"], "details": r4})

    return {"chain": "stream-ingest", "ok": True, "steps": steps, "summary": {
        "ingested_rows": r2.get("ingested_rows"),
        "batch_id": r3.get("batch_id") or r2.get("batch_id"),
        "target_table": r3.get("target_table") or r2.get("target_table"),
        "source_id": source_id,
        "start_offset": r3.get("start_offset") or r2.get("start_offset"),
        "end_offset": r3.get("end_offset") or r2.get("end_offset"),
        "committed_version": r3.get("version"),
        "delta_records": r4.get("delta_records"),
    }}


# ------------------------------------------------------------------
# 链路2: 存储优化
# lake_table→partition_merge→compaction_job→table_compaction
# ------------------------------------------------------------------

@router.post("/storage-optimize")
def chain_storage_optimize(table_id: int, merge_id: int, compaction_id: int) -> dict:
    """存储优化链路：定义湖表 → 合并分区 → 规划压缩 → 压缩表。"""
    steps = []

    # Step 1: 定义湖表
    svc_table = service_for("lake_table")
    if not svc_table:
        raise HTTPException(404, "lake_table 服务未注册")
    r1 = svc_table.domain_define_table(table_id)
    steps.append({"step": "define_table", "ok": r1["ok"], "details": r1})
    if not r1["ok"]:
        return {"chain": "storage-optimize", "ok": False, "steps": steps, "error": r1.get("message")}

    # Step 2: 合并分区
    svc_merge = service_for("partition_merge")
    if not svc_merge:
        raise HTTPException(404, "partition_merge 服务未注册")
    r2 = svc_merge.domain_merge_partitions(merge_id)
    steps.append({"step": "merge_partitions", "ok": r2["ok"], "details": r2})
    if not r2["ok"]:
        return {"chain": "storage-optimize", "ok": False, "steps": steps, "error": r2.get("message")}

    # Step 3: 规划压缩
    svc_comp = service_for("compaction_task")
    if not svc_comp:
        raise HTTPException(404, "compaction_task 服务未注册")
    r3 = svc_comp.domain_plan_compaction(compaction_id)
    steps.append({"step": "plan_compaction", "ok": r3["ok"], "details": r3})
    if not r3["ok"]:
        return {"chain": "storage-optimize", "ok": False, "steps": steps, "error": r3.get("message")}

    # Step 4: 压缩表
    r4 = svc_comp.domain_compact_table(compaction_id)
    steps.append({"step": "compact_table", "ok": r4["ok"], "details": r4})

    return {"chain": "storage-optimize", "ok": True, "steps": steps, "summary": {
        "table_def": r1.get("ddl"),
        "merged_files": r2.get("merged_files"),
        "compaction_plan": r3.get("plan"),
        "compression_ratio": r4.get("compression_ratio"),
    }}


# ------------------------------------------------------------------
# 链路3: Schema演进
# schema_evolve→data_backfill→batch_merge→stream_checkpoint
# ------------------------------------------------------------------

@router.post("/schema-evolve")
def chain_schema_evolve(schema_id: int, backfill_id: int, merge_id: int, checkpoint_id: int) -> dict:
    """Schema演进链路：Schema演进 → 数据回填 → 批流合并 → 保存检查点。"""
    steps = []

    # Step 1: Schema演进
    svc_schema = service_for("stream_schema")
    if not svc_schema:
        raise HTTPException(404, "stream_schema 服务未注册")
    r1 = svc_schema.domain_evolve_schema(schema_id)
    steps.append({"step": "evolve_schema", "ok": r1["ok"], "details": r1})
    if not r1["ok"]:
        return {"chain": "schema-evolve", "ok": False, "steps": steps, "error": r1.get("message")}

    # Step 2: 数据回填
    svc_backfill = service_for("backfill_pipeline")
    if not svc_backfill:
        raise HTTPException(404, "backfill_pipeline 服务未注册")
    r2 = svc_backfill.domain_backfill(backfill_id)
    steps.append({"step": "data_backfill", "ok": r2["ok"], "details": r2})
    if not r2["ok"]:
        return {"chain": "schema-evolve", "ok": False, "steps": steps, "error": r2.get("message")}

    # Step 3: 批流合并
    svc_merge = service_for("batch_merge")
    if not svc_merge:
        raise HTTPException(404, "batch_merge 服务未注册")
    r3 = svc_merge.domain_merge_batch_stream(merge_id)
    steps.append({"step": "batch_stream_merge", "ok": r3["ok"], "details": r3})
    if not r3["ok"]:
        return {"chain": "schema-evolve", "ok": False, "steps": steps, "error": r3.get("message")}

    # Step 4: 保存检查点
    svc_ckpt = service_for("stream_checkpoint")
    if not svc_ckpt:
        raise HTTPException(404, "stream_checkpoint 服务未注册")
    r4 = svc_ckpt.domain_save_checkpoint(checkpoint_id)
    steps.append({"step": "save_checkpoint", "ok": r4["ok"], "details": r4})

    return {"chain": "schema-evolve", "ok": True, "steps": steps, "summary": {
        "schema_version": r1.get("evolved_version"),
        "backfill_rows": r2.get("completed_rows"),
        "reconciled_total": r3.get("reconciled_total"),
        "checkpoint": r4.get("checkpoint_id"),
    }}


# ------------------------------------------------------------------
# 链路4: 生命周期
# snapshot_mgr→history_clean→retention_policy
# ------------------------------------------------------------------

@router.post("/lifecycle")
def chain_lifecycle(snapshot_id: int, clean_id: int, retention_id: int) -> dict:
    """生命周期链路：创建快照 → 清理历史 → 检查保留策略。"""
    steps = []

    # Step 1: 创建快照
    svc_snap = service_for("snapshot_mgr")
    if not svc_snap:
        raise HTTPException(404, "snapshot_mgr 服务未注册")
    r1 = svc_snap.domain_create_snapshot(snapshot_id)
    steps.append({"step": "create_snapshot", "ok": r1["ok"], "details": r1})
    if not r1["ok"]:
        return {"chain": "lifecycle", "ok": False, "steps": steps, "error": r1.get("message")}

    # Step 2: 清理历史
    svc_vacuum = service_for("vacuum_job")
    if not svc_vacuum:
        raise HTTPException(404, "vacuum_job 服务未注册")
    r2 = svc_vacuum.domain_clean_history(clean_id)
    steps.append({"step": "clean_history", "ok": r2["ok"], "details": r2})
    if not r2["ok"]:
        return {"chain": "lifecycle", "ok": False, "steps": steps, "error": r2.get("message")}

    # Step 3: 检查保留策略
    svc_ret = service_for("retention_policy")
    if not svc_ret:
        raise HTTPException(404, "retention_policy 服务未注册")
    r3 = svc_ret.domain_check_retention(retention_id)
    steps.append({"step": "check_retention", "ok": r3["ok"], "details": r3})

    return {"chain": "lifecycle", "ok": True, "steps": steps, "summary": {
        "snapshot_rows": r1.get("row_count"),
        "files_cleaned": r2.get("files_cleaned"),
        "can_clean_snapshots": r3.get("can_clean_count"),
    }}


# ------------------------------------------------------------------
# 链路5: 元数据治理
# lake_lineage→lake_audit→catalog_sync
# ------------------------------------------------------------------

@router.post("/metadata-govern")
def chain_metadata_govern(lineage_id: int, audit_id: int, catalog_id: int) -> dict:
    """元数据治理链路：追踪血缘 → 运行审计 → 同步目录。"""
    steps = []

    # Step 1: 追踪血缘
    svc_lineage = service_for("lake_lineage")
    if not svc_lineage:
        raise HTTPException(404, "lake_lineage 服务未注册")
    r1 = svc_lineage.domain_trace_lineage(lineage_id)
    steps.append({"step": "trace_lineage", "ok": r1["ok"], "details": r1})
    if not r1["ok"]:
        return {"chain": "metadata-govern", "ok": False, "steps": steps, "error": r1.get("message")}

    # Step 2: 运行审计
    svc_audit = service_for("lake_audit")
    if not svc_audit:
        raise HTTPException(404, "lake_audit 服务未注册")
    r2 = svc_audit.domain_run_audit(audit_id)
    steps.append({"step": "run_audit", "ok": r2["ok"], "details": r2})
    if not r2["ok"]:
        return {"chain": "metadata-govern", "ok": False, "steps": steps, "error": r2.get("message")}

    # Step 3: 同步目录
    svc_catalog = service_for("table_catalog")
    if not svc_catalog:
        raise HTTPException(404, "table_catalog 服务未注册")
    r3 = svc_catalog.domain_sync_catalog(catalog_id)
    steps.append({"step": "sync_catalog", "ok": r3["ok"], "details": r3})

    return {"chain": "metadata-govern", "ok": True, "steps": steps, "summary": {
        "lineage_depth": r1.get("depth"),
        "audit_tables": r2.get("tables_checked"),
        "synced_tables": r3.get("synced_tables"),
    }}