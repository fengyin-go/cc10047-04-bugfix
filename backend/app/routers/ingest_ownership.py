"""摄入批次归属相关的显式 RPC/REST 接口。

统一资源路由之外补充值班员排障和并发提交所需的批次、待办、版本记录接口。
所有业务规则仍在 ``ingest_coordinator`` 与领域服务中。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.ingest_coordinator import coordinator
from app.services import service_for

router = APIRouter(prefix="/api", tags=["摄入批次归属"])


async def _options(request: Request) -> dict[str, Any]:
    options: dict[str, Any] = dict(request.query_params)
    if request.method in {"POST", "PUT", "PATCH"}:
        try:
            body = await request.json()
        except Exception:
            body = None
        if isinstance(body, dict):
            # 支持 {"values": {...}} 风格。
            values = body.get("values")
            if isinstance(values, dict):
                options.update(values)
            options.update({k: v for k, v in body.items() if k != "values"})
        elif body is not None:
            options.setdefault("raw_body", body)
    return options


def _response(result: dict[str, Any]) -> JSONResponse:
    # 与本项目既有动作/链路接口保持一致：业务失败也返回 HTTP 200 + ok=false。
    # 领域建议状态码保留在 domain_status 中，供需要区分 403/409 的调用方使用。
    domain_status = int(result.get("_http_status", 200) or 200)
    result.setdefault("domain_status", domain_status)
    return JSONResponse(status_code=200, content=result)


# ----------------------------------------------------------------------
# 领取批次
# ----------------------------------------------------------------------
claim_paths = [
    "/ingest/claim",
    "/ingest-batches",
    "/ingest-batches/claim",
    "/ingest/batches/claim",
    "/batches/claim",
    "/stream_ingest/claim",
    "/stream_ingest/claim-batch",
    "/stream_ingest/claim_batch",
    "/stream-ingest/claim",
    "/stream-ingest/claim-batch",
    "/stream-ingest/claim_batch",
    "/stream-ingest/batches/claim",
    "/stream_ingest/batches/claim",
    "/claim_batch",
    "/claim-batch",
    "/ingest-batch/claim",
    "/ingest_batch/claim",
]


@router.post("/stream_ingest/{pipeline_ref}/claim")
@router.post("/stream_ingest/{pipeline_ref}/claim-batch")
@router.post("/stream_ingest/{pipeline_ref}/claim_batch")
@router.post("/stream_ingest/{pipeline_ref}/batches")
@router.post("/stream-ingest/{pipeline_ref}/claim")
@router.post("/stream-ingest/{pipeline_ref}/claim-batch")
@router.post("/stream-ingest/{pipeline_ref}/claim_batch")
@router.post("/stream-ingest/{pipeline_ref}/batches")
async def claim_for_pipeline(pipeline_ref: str, request: Request) -> JSONResponse:
    options = await _options(request)
    # /stream_ingest/batches/claim 会被 {pipeline_ref}/claim 匹配，这里改取请求体中的管道 ID。
    if pipeline_ref in {"batches", "batch"} and request.url.path.rstrip("/").endswith("/claim"):
        pipeline_ref = (
            options.pop("pipeline_id", None)
            or options.pop("ingest_id", None)
            or options.pop("entry_id", None)
            or options.pop("pipeline", None)
        )
    result = coordinator.claim_batch(pipeline_ref, **options)
    return _response(result)


def _register_simple_claim(path: str) -> None:
    @router.post(path)
    async def _handler(request: Request) -> JSONResponse:
        options = await _options(request)
        pipeline_ref = options.pop("pipeline_id", None) or options.pop("ingest_id", None) or options.pop("entry_id", None) or options.pop("pipeline", None)
        result = coordinator.claim_batch(pipeline_ref, **options)
        return _response(result)


for _path in claim_paths:
    _register_simple_claim(_path)


# ----------------------------------------------------------------------
# 改绑
# ----------------------------------------------------------------------
@router.post("/stream_ingest/{pipeline_ref}/rebind")
@router.post("/stream_ingest/{pipeline_ref}/rebind-pipeline")
@router.post("/stream_ingest/{pipeline_ref}/rebind_pipeline")
@router.post("/stream_ingest/{pipeline_ref}/bind")
@router.post("/stream_ingest/{pipeline_ref}/change-target")
@router.post("/stream_ingest/{pipeline_ref}/change_ownership")
@router.post("/stream_ingest/{pipeline_ref}/ownership")
@router.post("/stream-ingest/{pipeline_ref}/rebind")
@router.post("/stream-ingest/{pipeline_ref}/rebind-pipeline")
@router.post("/stream-ingest/{pipeline_ref}/change-ownership")
async def rebind_pipeline(pipeline_ref: str, request: Request) -> JSONResponse:
    options = await _options(request)
    target = options.pop("target_table", None) or options.pop("table", None) or options.pop("new_table", None) or options.pop("目标湖表", None)
    if target is None and options.get("raw_body") is not None:
        target = options.get("raw_body")
    result = coordinator.rebind(pipeline_ref, target, **options)
    return _response(result)


for path in [
    "/ingest/rebind", "/ingest-batches/rebind", "/stream_ingest/rebind",
    "/stream-ingest/rebind", "/stream-ingest/change-ownership",
    "/stream_ingest/rebind_pipeline", "/stream_ingest/rebind-pipeline",
    "/stream-ingest/rebind-pipeline", "/stream-ingest/rebind_pipeline",
    "/stream_ingest/change_ownership", "/stream-ingest/change_ownership",
    "/rebind", "/rebind-pipeline", "/rebind_pipeline",
]:
    def _register_rebind(p: str = path) -> None:
        @router.post(p)
        async def _handler(request: Request) -> JSONResponse:
            options = await _options(request)
            pipeline_ref = options.pop("pipeline_id", None) or options.pop("ingest_id", None) or options.pop("entry_id", None)
            target = options.pop("target_table", None) or options.pop("table", None) or options.pop("new_table", None)
            if target is None and options.get("raw_body") is not None:
                target = options.get("raw_body")
            return _response(coordinator.rebind(pipeline_ref, target, **options))
    _register_rebind()


# ----------------------------------------------------------------------
# 提交批次
# ----------------------------------------------------------------------
@router.post("/ingest-batches/{batch_ref}/commit")
@router.post("/ingest-batches/{batch_ref}/commit_batch")
@router.post("/ingest/batches/{batch_ref}/commit")
@router.post("/batches/{batch_ref}/commit")
@router.post("/stream_ingest/batches/{batch_ref}/commit")
@router.post("/stream_ingest/batches/{batch_ref}/commit_batch")
@router.post("/stream-ingest/batches/{batch_ref}/commit")
@router.post("/stream-ingest/batches/{batch_ref}/commit_batch")
@router.post("/ingest-batches/{batch_ref}/submit")
async def commit_batch_by_path(batch_ref: str, request: Request) -> JSONResponse:
    options = await _options(request)
    commit_id = options.pop("commit_id", None) or options.pop("commit_entry_id", None) or options.pop("version_id", None)
    result = coordinator.commit_batch(batch_ref, commit_id, **options)
    return _response(result)


for path in [
    "/ingest/commit", "/ingest-batches/commit", "/batches/commit",
    "/stream_ingest/commit", "/stream-ingest/commit",
    "/stream_ingest/commit-batch", "/stream_ingest/commit_batch",
    "/stream-ingest/commit-batch", "/stream-ingest/commit_batch",
    "/commit_batch", "/commit-batch",
]:
    def _register_commit(p: str = path) -> None:
        @router.post(p)
        async def _handler(request: Request) -> JSONResponse:
            options = await _options(request)
            batch_ref = options.pop("batch_id", None) or options.pop("batch", None) or options.pop("handoff", None)
            commit_id = options.pop("commit_id", None) or options.pop("commit_entry_id", None)
            return _response(coordinator.commit_batch(batch_ref, commit_id, **options))
    _register_commit()


# ----------------------------------------------------------------------
# 查询批次/待办/报告
# ----------------------------------------------------------------------
batch_list_paths = [
    "/ingest-batches", "/ingest/batches", "/batches",
    "/stream_ingest/batches", "/stream-ingest/batches",
    "/stream_ingest/batch", "/stream-ingest/batch",
    "/ingest_batch", "/ingest-batch",
]


def _register_batch_list(path: str) -> None:
    @router.get(path)
    async def _handler(request: Request) -> dict[str, Any]:
        filters = dict(request.query_params)
        rows = coordinator.list_batches(**filters)
        return {"ok": True, "items": rows, "total": len(rows), "batches": rows}


for _path in batch_list_paths:
    _register_batch_list(_path)


for path in [
    "/batch-todos", "/ingest/todos", "/ingest-batches/todos",
    "/stream_ingest/todos", "/stream-ingest/todos",
]:
    def _register_todos(p: str = path) -> None:
        @router.get(p)
        async def _handler(request: Request) -> dict[str, Any]:
            rows = coordinator.list_todos(**dict(request.query_params))
            return {"ok": True, "items": rows, "total": len(rows), "todos": rows}
    _register_todos(path)


@router.get("/stream_ingest/{pipeline_ref}/batches")
@router.get("/stream_ingest/{pipeline_ref}/todos")
@router.get("/stream-ingest/{pipeline_ref}/batches")
@router.get("/stream-ingest/{pipeline_ref}/todos")
async def pipeline_batches(pipeline_ref: str, request: Request) -> dict[str, Any]:
    filters = dict(request.query_params)
    filters["pipeline_id"] = pipeline_ref
    if request.url.path.endswith("/todos"):
        rows = coordinator.list_todos(**filters)
    else:
        rows = coordinator.list_batches(**filters)
    return {"ok": True, "items": rows, "total": len(rows)}


@router.get("/ingest-batches/{batch_ref}")
@router.get("/ingest/batches/{batch_ref}")
@router.get("/batches/{batch_ref}")
async def get_batch(batch_ref: str) -> JSONResponse:
    batch = coordinator.find_batch(batch_ref)
    if not batch:
        return JSONResponse(status_code=404, content={"ok": False, "message": f"批次 {batch_ref} 不存在"})
    return JSONResponse(content={"ok": True, "batch": batch, **batch})


@router.get("/ingest/attribution-report")
@router.get("/stream_ingest/attribution-report")
@router.get("/stream-ingest/attribution-report")
async def attribution_report(pipeline_id: str | None = None) -> dict[str, Any]:
    return coordinator.attribution_report(pipeline_id)


# ----------------------------------------------------------------------
# 湖表版本归属记录
# ----------------------------------------------------------------------
version_list_paths = [
    "/table-versions", "/table_versions",
    "/lake-table-versions", "/lake_table_versions",
    "/delta-versions", "/delta_versions",
]


def _register_versions(path: str) -> None:
    @router.get(path)
    async def _handler(request: Request) -> dict[str, Any]:
        return coordinator.table_versions(**dict(request.query_params))


for _path in version_list_paths:
    _register_versions(_path)


@router.get("/tables/{table}/versions")
@router.get("/lake_tables/{table}/versions")
@router.get("/lake-tables/{table}/versions")
@router.get("/lake_table/{table}/versions")
async def table_version_path(table: str, request: Request) -> dict[str, Any]:
    filters = dict(request.query_params)
    return coordinator.table_versions(table, **filters)


# ----------------------------------------------------------------------
# 写授权
# ----------------------------------------------------------------------
@router.post("/ingest/reset")
@router.post("/ingest-batches/reset")
async def reset_ingest_state() -> dict[str, Any]:
    return coordinator.reset()


@router.post("/tables/{table}/grants")
@router.post("/lake-tables/{table}/grants")
@router.post("/table-permissions/{table}")
async def grant_for_table(table: str, request: Request) -> dict[str, Any]:
    options = await _options(request)
    principal = options.pop("principal", None) or options.pop("writer", None) or options.pop("user", "*")
    allowed = options.pop("allowed", True)
    if isinstance(allowed, str):
        allowed = allowed.casefold() not in {"false", "0", "deny", "拒绝"}
    return coordinator.grant_write(table, principal, allowed=bool(allowed), **options)


@router.get("/write-grants")
@router.get("/write_grants")
async def list_grants() -> dict[str, Any]:
    return {"ok": True, "items": coordinator.grants, "total": len(coordinator.grants)}


# 兼容通过服务名获取报告的调用。
@router.get("/ingest/service-report/{pipeline_id}")
async def service_report(pipeline_id: int) -> dict[str, Any]:
    svc = service_for("stream_ingest")
    return svc.attribution_report(pipeline_id) if svc else {"ok": False}
