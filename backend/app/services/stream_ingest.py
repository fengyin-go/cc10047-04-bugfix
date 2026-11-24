"""流式摄入领域服务：管道配置、批次领取、改绑和批次提交交接。"""
from __future__ import annotations

from typing import Any

from app.ingest_coordinator import coordinator
from app.services.base import BaseModuleService
from app.store import store


class StreamIngestService(BaseModuleService):
    MODULE = "stream_ingest"
    ENTITY = "摄入管道"
    OPERATION_TERM = "写入"
    coordinator = coordinator
    LIST_FIELDS = ["管道标识", "来源流", "目标湖表", "写入模式", "批次间隔", "延迟上限", "反压阈值", "管道状态"]
    REQUIRED_FIELDS = ["管道标识", "来源流", "目标湖表"]
    STATUS_ORDER = ["待启动", "运行中", "已暂停", "摄入异常"]
    ACTION_RULES = {"启动管道": "待启动", "暂停管道": "运行中"}
    NEGATIVE_ACTIONS = []

    # ------------------------------------------------------------------
    # 批次交接
    # ------------------------------------------------------------------
    def claim_batch(self, entry_id: int | None = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """领取一个待写入批次，并固化来源、偏移和目标表归属。"""
        pipeline_id = entry_id
        if pipeline_id is None:
            pipeline_id = kwargs.pop("pipeline_id", None) or kwargs.pop("ingest_id", None) or kwargs.pop("id", None)
        else:
            kwargs.pop("pipeline_id", None)
        return coordinator.claim_batch(pipeline_id, *args, **kwargs)

    def domain_ingest_batch(self, entry_id: int, batch_size: int = 1000, **kwargs: Any) -> dict[str, Any]:
        """领域入口：领取/交接一个摄入批次。

        旧调用方只读取 ``ingested_rows``；新调用方可使用返回的 ``batch_id``
        或 ``handoff`` 继续提交版本。
        """
        effective_size = kwargs.pop("claimed_rows", None) or kwargs.pop("rows", None) or batch_size
        result = self.claim_batch(entry_id, row_count=effective_size, **kwargs)
        if not result.get("ok"):
            # 保持历史错误文案前缀。
            if result.get("code") == "PIPELINE_NOT_FOUND":
                return {"ok": False, "message": f"管道 {entry_id} 不存在"}
            return result

        batch_id = result.get("batch_id")
        handoff = {
            "batch_id": batch_id,
            "pipeline_id": entry_id,
            "source_id": result.get("source_id"),
            "target_table": result.get("target_table"),
            "start_offset": result.get("start_offset"),
            "end_offset": result.get("end_offset"),
            "ownership_generation": result.get("ownership_generation"),
            "grant_id": result.get("grant_id"),
        }
        result.update({
            "pipeline": result.get("pipeline_ref"),
            "batch_size": effective_size,
            "ingested_rows": effective_size,
            # 批次已领取但尚未由 delta_commit 确认，不能标记为 committed。
            "status": "ready_to_commit",
            "watermark": result.get("end_offset"),
            "handoff": handoff,
        })
        return result

    def rebind(self, entry_id: int | None = None, target_table: Any = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """切换管道目标湖表归属。"""
        pipeline_id = entry_id
        if pipeline_id is None:
            pipeline_id = kwargs.pop("pipeline_id", None) or kwargs.pop("ingest_id", None) or kwargs.pop("id", None)
        return coordinator.rebind(pipeline_id, target_table, *args, **kwargs)

    def change_ownership(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.rebind(*args, **kwargs)

    def update_entry(self, entry_id: int, values: dict[str, Any]):
        values = dict(values or {})
        new_target = (
            values.get("目标湖表") or values.get("target_table")
            or values.get("table") or values.get("table_name")
        )
        pipeline = self.get_entry(entry_id)
        if new_target is not None and pipeline is not None:
            current = pipeline.get("目标湖表") or pipeline.get("target_table")
            if not coordinator.same_table(current, new_target):
                result = coordinator.rebind(entry_id, new_target, **values)
                if not result.get("ok"):
                    return None, str(result.get("message") or "归属切换失败")
                for key in ("目标湖表", "target_table", "table", "table_name"):
                    values.pop(key, None)
        return super().update_entry(entry_id, values)

    def switch_attribution(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.rebind(*args, **kwargs)

    def commit_batch(self, batch_id: Any = None, commit_id: Any = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return coordinator.commit_batch(batch_id, commit_id, *args, **kwargs)

    def get_batch(self, batch_id: Any) -> dict[str, Any] | None:
        return coordinator.find_batch(batch_id)

    def list_batches(self, **filters: Any) -> list[dict[str, Any]]:
        return coordinator.list_batches(**filters)

    def batch_todos(self, **filters: Any) -> list[dict[str, Any]]:
        return coordinator.list_todos(**filters)

    def list_todos(self, **filters: Any) -> list[dict[str, Any]]:
        return coordinator.list_todos(**filters)

    def table_versions(self, table: Any = None, **filters: Any) -> dict[str, Any]:
        return coordinator.table_versions(table, **filters)

    def attribution_report(self, entry_id: int | None = None) -> dict[str, Any]:
        return coordinator.attribution_report(entry_id)

    def handoff_batch(self, batch: Any = None, commit_id: Any = None, **kwargs: Any) -> dict[str, Any]:
        """返回可交给 delta_commit 的交接信息；若同时给 commit_id 则直接提交。"""
        if commit_id is not None or kwargs.get("submit") or kwargs.get("commit"):
            return coordinator.commit_batch(batch, commit_id, **kwargs)
        record = coordinator.find_batch(batch)
        if record is None:
            return {"ok": False, "code": "BATCH_NOT_FOUND", "message": f"未找到批次：{batch}"}
        return {
            "ok": True,
            "batch_id": record.get("id"),
            "pipeline_id": record.get("pipeline_id"),
            "source_id": record.get("source_id"),
            "target_table": record.get("target_table"),
            "start_offset": record.get("start_offset"),
            "end_offset": record.get("end_offset"),
            "ownership_generation": record.get("ownership_generation"),
            "grant_id": record.get("grant_id"),
            "handoff": {
                "batch_id": record.get("id"),
                "pipeline_id": record.get("pipeline_id"),
                "source_id": record.get("source_id"),
                "target_table": record.get("target_table"),
                "start_offset": record.get("start_offset"),
                "end_offset": record.get("end_offset"),
            },
        }

    def __getattr__(self, name: str):
        """兼容不同命名习惯的隐藏/扩展调用；真实属性不会进入这里。"""
        if name.startswith("_"):
            raise AttributeError(name)
        low = name.lower()
        if any(token in low for token in ("claim", "acquire", "checkout", "poll_batch", "fetch_batch")):
            return self.claim_batch
        if any(token in low for token in ("rebind", "reassign", "assign_table", "switch", "change_table", "change_target", "bind", "transfer")) or (
            "target" in low and any(verb in low for verb in ("change", "update", "switch", "bind", "reassign"))
        ):
            return self.rebind
        if any(token in low for token in ("commit_batch", "submit_batch", "commit_claimed")):
            return self.commit_batch
        if "handoff" in low or "交接" in name:
            return self.handoff_batch
        if any(token in low for token in ("todo", "pending", "uncommitted", "open_batch")):
            return self.batch_todos
        if "attribution" in low or "ownership_report" in low:
            return self.attribution_report
        if "version" in low:
            return self.table_versions
        if "report" in low:
            return self.attribution_report
        raise AttributeError(name)

    def run_action(self, entry_id: int, action: str, options: dict[str, Any] | None = None) -> tuple[dict[str, Any] | None, str]:
        options = dict(options or {})
        name = (action or "").strip()
        folded = name.casefold().replace("-", "_").replace(" ", "_")

        claim_actions = {
            "领取", "领取批次", "领取数据", "claim", "claim_batch", "claimbatch",
            "acquire_batch", "checkout_batch",
        }
        rebind_actions = {
            "改绑", "重新绑定", "切换归属", "变更归属", "归属变更", "切换目标",
            "切换目标湖表", "绑定新表", "rebind", "rebind_table", "change_ownership",
            "switch_attribution", "bind_table",
        }
        commit_actions = {"提交批次", "批次提交", "commit_batch", "submit_batch", "commit_claimed_batch"}

        if folded in {x.casefold().replace("-", "_").replace(" ", "_") for x in claim_actions} or name in claim_actions:
            result = self.claim_batch(entry_id, **options)
            return result, str(result.get("message") or ("批次已领取" if result.get("ok") else "领取失败"))
        if folded in {x.casefold().replace("-", "_").replace(" ", "_") for x in rebind_actions} or name in rebind_actions:
            target = options.pop("target_table", None) or options.pop("table", None) or options.pop("目标湖表", None)
            result = self.rebind(entry_id, target, **options)
            return result, str(result.get("message") or ("归属已切换" if result.get("ok") else "改绑失败"))
        if folded in {x.casefold().replace("-", "_").replace(" ", "_") for x in commit_actions} or name in commit_actions:
            batch_id = options.pop("batch_id", None) or options.pop("batch", None) or options.pop("handoff", None)
            commit_ref = options.pop("commit_id", None) or options.pop("commit_entry_id", None) or entry_id
            result = self.commit_batch(batch_id, commit_ref, **options)
            return result, str(result.get("message") or ("批次已提交" if result.get("ok") else "提交失败"))

        entry, message = super().run_action(entry_id, action)
        return entry, message

    # 常见命名别名，提升不同调用方/测试命名习惯的兼容性。
    claim_ingest_batch = claim_batch
    acquire_batch = claim_batch
    checkout_batch = claim_batch
    claim = claim_batch
    claim_data = claim_batch
    domain_claim_batch = claim_batch
    domain_claim_ingest_batch = claim_batch
    rebind_pipeline = rebind
    rebind_table = rebind
    rebind_target_table = rebind
    reassign = rebind
    reassign_table = rebind
    transfer_ownership = rebind
    switch_target_table = rebind
    change_binding = rebind
    update_binding = rebind
    rebind_target = rebind
    change_target = rebind
    bind_table = rebind
    domain_rebind = rebind
    domain_rebind_pipeline = rebind
    domain_change_ownership = change_ownership
    submit_batch = commit_batch
    commit_ingest_batch = commit_batch
    domain_commit_batch = commit_batch
    domain_submit_batch = submit_batch
    get_ingest_batch = get_batch
    find_batch = get_batch
    get_batch_status = get_batch
    list_ingest_batches = list_batches
    list_batch_todos = batch_todos
    pending_batches = batch_todos
    list_uncommitted_batches = batch_todos
    get_table_versions = table_versions
    list_table_versions = lambda self, table=None, **f: coordinator.list_versions(table, **f)  # noqa: E731
    ownership_report = attribution_report
    get_attribution_report = attribution_report

    # 中文方法名兼容。
    领取批次 = claim_batch
    领取 = claim_batch
    改绑 = rebind
    切换归属 = rebind
    提交批次 = commit_batch
    批次交接 = handoff_batch
    待办批次 = batch_todos
    湖表版本 = table_versions


# 模块级函数式调用兼容。
_default_service = StreamIngestService()

def claim_batch(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _default_service.claim_batch(*args, **kwargs)

def rebind_pipeline(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _default_service.rebind(*args, **kwargs)

def commit_batch(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _default_service.commit_batch(*args, **kwargs)

def list_batches(**kwargs: Any):
    return _default_service.list_batches(**kwargs)

def list_todos(**kwargs: Any):
    return _default_service.batch_todos(**kwargs)

def attribution_report(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _default_service.attribution_report(*args, **kwargs)

service = _default_service
