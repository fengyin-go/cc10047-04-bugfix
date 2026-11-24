"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services import service_for

router = APIRouter(prefix="/api", tags=["业务资源"])


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    for alias, chinese_field in (("target_table", "目标湖表"), ("table", "目标湖表")):
        value = request.query_params.get(alias)
        if value and chinese_field in service.LIST_FIELDS:
            filters[chinese_field] = value.strip()
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    for alias, chinese_field in (("target_table", "目标湖表"), ("table", "目标湖表")):
        value = request.query_params.get(alias)
        if value and chinese_field in service.LIST_FIELDS:
            filters[chinese_field] = value.strip()
    items, _ = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int) -> dict[str, Any]:
    service = require_service(module)
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    return entry


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.put("/{module}/{entry_id}")
@router.patch("/{module}/{entry_id}")
async def update_entry(module: str, entry_id: int, request: Request) -> JSONResponse:
    service = require_service(module)
    try:
        body = await request.json()
    except Exception:
        body = {}
    if isinstance(body, dict):
        values = body.get("values") if isinstance(body.get("values"), dict) else body
    else:
        values = {"value": body}
    entry, message = service.update_entry(entry_id, dict(values or {}))
    ok = entry is not None
    body_out: dict[str, Any] = {"ok": ok, "message": message, "entry": entry}
    if isinstance(entry, dict):
        for key, value in entry.items():
            body_out.setdefault(key, value)
    return JSONResponse(status_code=200, content=body_out)


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload) -> ActionResult:
    service = require_service(module)
    options: dict[str, Any] = {}
    # 兼容 {"action": "...", "values": {...}} 与把 batch_size/table 直接放顶层。
    options.update(payload.values if isinstance(getattr(payload, "values", None), dict) else {})
    dumped = payload.model_dump(exclude={"action", "values"})
    for key, value in dumped.items():
        if value is not None:
            options.setdefault(key, value)
    entry, message = service.run_action(entry_id, payload.action.strip(), options)
    ok = entry is not None
    if isinstance(entry, dict) and "ok" in entry:
        ok = bool(entry.get("ok"))
    body: dict[str, Any] = {"ok": ok, "message": message, "entry": entry}
    # 让领取/改绑/提交等富结果既能从 entry 取，也能在响应顶层直接取。
    if isinstance(entry, dict):
        for key, value in entry.items():
            if key not in body:
                body[key] = value
    return JSONResponse(status_code=200, content=body)
