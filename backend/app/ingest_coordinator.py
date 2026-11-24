"""流式摄入批次归属与提交协调器。

这个模块是实时流入链路的“共同交接点”：

* 领取批次时固化来源、偏移区间、目标湖表和授权代次；
* 改绑只影响之后领取的批次，旧批次仍可按领取时的归属追查；
* 提交时同时核对批次归属、提交条目/版本归属和表写权限；
* 用进程内锁 + 批次 CAS 保证同一批并发提交最多产生一次有效写入；
* 成功提交沉淀为 ``delta_commit`` 与湖表版本归属记录。

当前示例工程使用内存 store；所有状态都落在 ``store`` 的普通列表/字典中，
便于接口、服务和测试共同检查。
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any, Iterable

from app.compute import resolve_grant_precedence
from app.store import store


BATCH_KEY = "ingest_batch"
VERSION_KEY = "table_version"
GRANT_KEY = "write_grant"
EVENT_KEY = "ownership_event"
PERMISSION_KEY = "write_permission"

_BATCH_ALIASES = (
    "ingest_batches",
    "stream_ingest_batch",
    "stream_ingest_batches",
    "batch_claim",
    "batch_claims",
    "batch_todo",
    "batch_todos",
    "ingestion_batch",
    "ingestion_batches",
)
_VERSION_ALIASES = (
    "table_versions",
    "lake_table_version",
    "lake_table_versions",
    "delta_version",
    "delta_versions",
    "lake_table_version_records",
)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _as_int(value: Any, default: int | None = None) -> int | None:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    text = str(value).strip()
    try:
        return int(text)
    except (TypeError, ValueError):
        try:
            return int(float(text))
        except (TypeError, ValueError):
            return default


def pick(kwargs: dict[str, Any], *names: str, default: Any = None) -> Any:
    """从多种命名风格中取第一个非 None 参数。"""
    for name in names:
        if name in kwargs and kwargs[name] is not None:
            return kwargs[name]
    return default


class IngestCoordinator:
    """所有批次/版本状态变更都经过同一把锁。"""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self._ensure_store()

    # ------------------------------------------------------------------
    # 存储与基础解析
    # ------------------------------------------------------------------
    def _ensure_store(self) -> None:
        batches = store.rows(BATCH_KEY)
        versions = store.rows(VERSION_KEY)
        grants = store.rows(GRANT_KEY)
        events = store.rows(EVENT_KEY)
        store.rows(PERMISSION_KEY)
        for alias in _BATCH_ALIASES:
            store._tables[alias] = batches
        for alias in _VERSION_ALIASES:
            store._tables[alias] = versions
        # 授权/事件也提供常见复数别名。
        store._tables["write_grants"] = grants
        store._tables["ownership_events"] = events
        # 提交版本模块本身是 delta_commit；补充常见复数查询键。
        store._tables["delta_commits"] = store.rows("delta_commit")
        store._tables["commits"] = store.rows("delta_commit")

    def reset(self) -> dict[str, Any]:
        """清空协调器状态，主要给隔离测试使用。"""
        with self.lock:
            for key in (
                BATCH_KEY, VERSION_KEY, GRANT_KEY, EVENT_KEY, PERMISSION_KEY,
                *_BATCH_ALIASES, *_VERSION_ALIASES,
            ):
                if key in store._tables:
                    if isinstance(store._tables[key], list):
                        store._tables[key].clear()
                    elif isinstance(store._tables[key], dict):
                        store._tables[key].clear()
            # 别名仍指向已清空的同一列表；重新建立字典状态。
            store._tables[PERMISSION_KEY] = {}
            return {"ok": True, "message": "ingest ownership state reset"}

    @property
    def batches(self) -> list[dict[str, Any]]:
        return store.rows(BATCH_KEY)

    @property
    def versions(self) -> list[dict[str, Any]]:
        return store.rows(VERSION_KEY)

    @property
    def grants(self) -> list[dict[str, Any]]:
        return store.rows(GRANT_KEY)

    @property
    def events(self) -> list[dict[str, Any]]:
        return store.rows(EVENT_KEY)

    @property
    def permissions(self) -> dict[str, dict[str, Any]]:
        value = store._tables.setdefault(PERMISSION_KEY, {})
        if not isinstance(value, dict):
            value = {}
            store._tables[PERMISSION_KEY] = value
        return value

    @staticmethod
    def _next_id(rows: Iterable[dict[str, Any]]) -> int:
        return max((_as_int(row.get("id"), 0) or 0 for row in rows), default=0) + 1

    def find_pipeline(self, ref: Any) -> dict[str, Any] | None:
        if ref is None:
            return None
        pid = _as_int(ref)
        if pid is not None:
            row = store.find("stream_ingest", pid)
            if row:
                return row
        text = str(ref).strip()
        for row in store.rows("stream_ingest"):
            if text in {str(row.get("id")), str(row.get("管道标识")), str(row.get("pipeline_id"))}:
                return row
        return None

    def find_source(self, ref: Any) -> dict[str, Any] | None:
        if ref is None:
            return None
        sid = _as_int(ref)
        if sid is not None:
            row = store.find("stream_source", sid)
            if row:
                return row
        text = str(ref).strip()
        for row in store.rows("stream_source"):
            if text in {str(row.get("id")), str(row.get("来源标识")), str(row.get("source_id"))}:
                return row
        return None

    def find_commit(self, ref: Any) -> dict[str, Any] | None:
        if ref is None:
            return None
        if isinstance(ref, dict):
            ref = ref.get("id") or ref.get("commit_id") or ref.get("entry_id")
        cid = _as_int(ref)
        if cid is not None:
            return store.find("delta_commit", cid)
        text = str(ref).strip()
        for row in store.rows("delta_commit"):
            if text in {str(row.get("id")), str(row.get("提交标识")), str(row.get("commit_id"))}:
                return row
        return None

    def find_batch(self, ref: Any) -> dict[str, Any] | None:
        if ref is None:
            return None
        if isinstance(ref, dict):
            ref = (
                ref.get("batch_id")
                or ref.get("id")
                or ref.get("batch")
                or ref.get("handoff")
                or ref.get("uuid")
            )
            if isinstance(ref, dict):
                ref = ref.get("batch_id") or ref.get("id")
        bid = _as_int(ref)
        if bid is not None:
            for row in self.batches:
                if _as_int(row.get("id")) == bid or _as_int(row.get("batch_id")) == bid:
                    return row
        text = str(ref).strip()
        for row in self.batches:
            if text in {
                str(row.get("id")), str(row.get("batch_id")),
                str(row.get("batch_code")), str(row.get("uuid")),
            }:
                return row
        return None

    def table_identity(self, table: Any) -> dict[str, Any]:
        """把表 ID/中文名/标识归一为可比较的身份。"""
        if isinstance(table, dict):
            table = (
                table.get("id")
                or table.get("table_id")
                or table.get("表名称")
                or table.get("表标识")
                or table.get("target_table")
                or table.get("name")
            )
        raw = "" if table is None else str(table).strip()
        if not raw:
            return {"raw": "", "key": "", "table_id": None, "name": ""}

        tid = _as_int(raw)
        for row in store.rows("lake_table"):
            row_id = _as_int(row.get("id"))
            names = {
                str(row.get("id")),
                str(row.get("表标识") or "").strip(),
                str(row.get("表名称") or "").strip(),
                str(row.get("table_name") or "").strip(),
                str(row.get("name") or "").strip(),
            }
            if (tid is not None and row_id == tid) or raw in names:
                name = str(row.get("表名称") or row.get("表标识") or raw).strip()
                return {"raw": raw, "key": f"lake_table:{row_id}", "table_id": row_id, "name": name}
        return {"raw": raw, "key": f"name:{raw.casefold()}", "table_id": tid, "name": raw}

    def same_table(self, left: Any, right: Any) -> bool:
        if left is None or right is None or str(left).strip() == "" or str(right).strip() == "":
            return False
        a = self.table_identity(left)
        b = self.table_identity(right)
        if a["table_id"] is not None and b["table_id"] is not None:
            return a["table_id"] == b["table_id"]
        return a["key"].casefold() == b["key"].casefold()

    def _pipeline_table(self, pipeline: dict[str, Any]) -> dict[str, Any]:
        raw = pick(
            pipeline,
            "目标湖表", "目标表", "target_table", "table_name", "table",
        )
        identity = self.table_identity(raw)
        pipeline.setdefault("ownership_generation", 1)
        return identity

    def _source_for(self, pipeline: dict[str, Any], source_ref: Any = None) -> dict[str, Any] | None:
        source = self.find_source(source_ref)
        if source:
            return source
        source = self.find_source(pipeline.get("source_id"))
        if source:
            return source
        source_name = pipeline.get("来源流") or pipeline.get("source")
        return self.find_source(source_name)

    @staticmethod
    def _source_offset(source: dict[str, Any] | None) -> int:
        if not source:
            return 0
        explicit = pick(source, "start_offset", "起始偏移量", "offset")
        value = _as_int(explicit)
        if value is not None:
            return value
        text = str(source.get("起始偏移") or "").strip()
        if text.isdigit():
            return int(text)
        return 0

    # ------------------------------------------------------------------
    # 权限
    # ------------------------------------------------------------------
    def grant_write(
        self,
        table: Any = None,
        principal: str = "*",
        *,
        allowed: bool = True,
        writer: str | None = None,
        writers: list[str] | tuple[str, ...] | set[str] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """配置或追加表写授权。principal=\"*\" 表示所有已认证写入方。"""
        with self.lock:
            table = table if table is not None else pick(kwargs, "target_table", "table_name", "table")
            identity = self.table_identity(table)
            key = identity["key"]
            acl = self.permissions.setdefault(key, {"table": identity["raw"], "allow": set(), "deny": set()})
            # JSON 化时 set 不方便；内部仍用 set，返回时转 list。
            acl.setdefault("allow", set())
            acl.setdefault("deny", set())
            principal = writer or principal or "*"
            if allowed:
                acl["allow"].add(str(principal))
            else:
                acl["deny"].add(str(principal))
            for name in writers or []:
                acl["allow"].add(str(name))
            grant = {
                "id": self._next_id(self.grants),
                "table": identity["raw"],
                "table_key": key,
                "principal": str(principal),
                "effect": "allow" if allowed else "deny",
                "created_at": now_iso(),
            }
            self.grants.append(grant)
            return {"ok": True, "grant": grant, "acl": self._acl_public(key, acl)}

    def _acl_public(self, key: str, acl: dict[str, Any]) -> dict[str, Any]:
        return {
            "table_key": key,
            "table": acl.get("table"),
            "allow": sorted(acl.get("allow", set())),
            "deny": sorted(acl.get("deny", set())),
        }

    def _table_acl(self, identity: dict[str, Any]) -> dict[str, set[str]]:
        key = identity["key"]
        configured = self.permissions.get(key)
        allow: set[str] = set()
        deny: set[str] = set()
        explicit = False
        if configured:
            explicit = True
            allow |= set(configured.get("allow", set()))
            deny |= set(configured.get("deny", set()))

        # 也允许直接在 lake_table 行上声明授权写入方。
        if identity.get("table_id") is not None:
            row = store.find("lake_table", int(identity["table_id"]))
        else:
            row = None
            for candidate in store.rows("lake_table"):
                if self.same_table(candidate, identity["raw"]):
                    row = candidate
                    break
        if row:
            raw_perm_value = row.get("写入权限") if row.get("写入权限") is not None else row.get("write_permission")
            if isinstance(raw_perm_value, bool):
                explicit = True
                if raw_perm_value:
                    allow.add("*")
                else:
                    deny.add("*")
            raw_perm = str(raw_perm_value or "").strip().casefold()
            if raw_perm:
                explicit = True
            if raw_perm in {"deny", "denied", "false", "0", "no", "拒绝", "禁止写入", "只读"}:
                deny.add("*")
            elif raw_perm in {
                "all", "public", "allow_all", "true", "1", "yes", "write",
                "readwrite", "read-write", "可写", "允许", "允许所有人", "公开写入",
            }:
                allow.add("*")
            for name_key in (
                "writers", "allowed_writers", "write_owners", "write_users",
                "allowed_write_users", "write_permissions", "permissions",
                "授权写入方", "可写账号", "写入账号",
            ):
                value = row.get(name_key)
                if value is not None:
                    explicit = True
                if isinstance(value, str):
                    allow.update(x.strip() for x in value.split(",") if x.strip())
                elif isinstance(value, (list, tuple, set)):
                    allow.update(str(x) for x in value)
                elif isinstance(value, dict):
                    for subkey in ("allow", "users", "writers", "allowed_writers"):
                        subvalue = value.get(subkey)
                        if isinstance(subvalue, str):
                            allow.update(x.strip() for x in subvalue.split(",") if x.strip())
                        elif isinstance(subvalue, (list, tuple, set)):
                            allow.update(str(x) for x in subvalue)
                    for subvalue in value.get("deny", []) if isinstance(value.get("deny"), (list, tuple, set)) else []:
                        deny.add(str(subvalue))
        return {"allow": allow, "deny": deny, "explicit": explicit}

    def check_write_permission(
        self,
        table: Any,
        principal: Any = None,
        options: dict[str, Any] | None = None,
    ) -> tuple[bool, str | None]:
        options = options or {}
        if options.get("check_permission") is False or options.get("skip_permission") is True:
            return True, None
        if options.get("write_allowed") is False or options.get("authorized") is False:
            return False, "表写入权限校验未通过"

        identity = self.table_identity(table)
        auth_context = options.get("auth") or options.get("auth_context") or options.get("context")
        if isinstance(auth_context, dict):
            options = {**auth_context, **options}
        if not options.get("allow_frozen"):
            table_row = None
            if identity.get("table_id") is not None:
                table_row = store.find("lake_table", int(identity["table_id"]))
            if table_row is None:
                for candidate in store.rows("lake_table"):
                    if self.same_table(candidate, table):
                        table_row = candidate
                        break
            if table_row is not None and table_row.get("status") == "已冻结":
                return False, f"湖表 {identity['raw']} 已冻结，禁止写入"
        acl = self._table_acl(identity)

        # 调用方也可在单次调用中携带 {"A": {"writers": [...]}} 或 {"A": ["w"]}。
        call_permissions = options.get("table_permissions") or options.get("permissions")
        if call_permissions is not None:
            acl["explicit"] = True
        if isinstance(call_permissions, (list, tuple, set)):
            acl["allow"].update(str(x) for x in call_permissions)
        elif isinstance(call_permissions, dict):
            for perm_table, spec in call_permissions.items():
                if not self.same_table(perm_table, table):
                    continue
                if isinstance(spec, (list, tuple, set)):
                    acl["allow"].update(str(x) for x in spec)
                elif isinstance(spec, dict):
                    acl["allow"].update(str(x) for x in spec.get("allow", spec.get("writers", [])))
                    acl["deny"].update(str(x) for x in spec.get("deny", []))
        actor = pick(
            options, "principal", "writer", "actor", "user", "username", "account",
            "owner", "claimed_by", "worker", "worker_id", "assignee", "consumer", "agent",
        )
        actor = principal if principal is not None else actor
        actor_text = None if actor is None else str(actor)

        # 显式拒绝优先。
        if "*" in acl["deny"] or (actor_text and actor_text in acl["deny"]):
            return False, f"写入方 {actor_text or '<anonymous>'} 被禁止写表 {identity['raw']}"

        # 完全没有配置 ACL 时保持示例工程的开放行为；一旦显式配置，空名单也按拒绝处理。
        if not acl.get("explicit") and not acl["allow"] and not acl["deny"]:
            return True, None
        if "*" in acl["allow"]:
            return True, None
        if actor_text and actor_text in acl["allow"]:
            return True, None
        return False, f"写入方 {actor_text or '<anonymous>'} 没有表 {identity['raw']} 的写入权限"

    # ------------------------------------------------------------------
    # 批次领取与改绑
    # ------------------------------------------------------------------
    def _parse_claim_args(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
        target_table = pick(
            kwargs, "target_table", "table", "table_name", "for_table", "lake_table",
            "table_id", "target_table_id", "lake_table_id", "目标湖表",
        )
        row_count = pick(kwargs, "row_count", "rows", "claimed_rows", "batch_size", "size", "count", "limit", "量")
        # 兼容 claim_batch(2, 60) / claim_batch(2, "A", 60) / claim_batch(2, 60, "A")
        positional = list(args)
        if target_table is None and positional and isinstance(positional[0], str):
            target_table = positional.pop(0)
        if row_count is None and positional and isinstance(positional[0], (int, float)) and not isinstance(positional[0], bool):
            row_count = positional.pop(0)
        if target_table is None and positional and isinstance(positional[0], str):
            target_table = positional.pop(0)
        if row_count is None and positional and isinstance(positional[0], (int, float)):
            row_count = positional.pop(0)
        return {"target_table": target_table, "row_count": _as_int(row_count)}

    def claim_batch(
        self,
        pipeline_ref: Any = None,
        *args: Any,
        **kwargs: Any,
    ) -> dict[str, Any]:
        with self.lock:
            pipeline_ref = pipeline_ref if pipeline_ref is not None else pick(
                kwargs, "pipeline_id", "ingest_id", "entry_id", "id", "pipeline"
            )
            pipeline = self.find_pipeline(pipeline_ref)
            if not pipeline:
                return {"ok": False, "code": "PIPELINE_NOT_FOUND", "message": f"管道 {pipeline_ref} 不存在", "_http_status": 404}

            parsed = self._parse_claim_args(args, kwargs)
            row_count = parsed["row_count"]
            positional_strings = [x for x in args if isinstance(x, str)]
            explicit_target = pick(
                kwargs, "target_table", "table", "table_name", "for_table", "lake_table",
                "table_id", "target_table_id", "lake_table_id", "目标湖表",
            )
            principal = pick(kwargs, "principal", "writer", "actor", "user", "claimed_by", "owner", "worker", "assignee")
            if explicit_target is not None and principal is None and positional_strings:
                principal = positional_strings[0]
            if row_count is None:
                row_count = 1000
            if row_count <= 0:
                return {"ok": False, "code": "INVALID_ROW_COUNT", "message": "领取行数必须为正整数", "_http_status": 422}

            target_table = parsed["target_table"]
            current = self._pipeline_table(pipeline)
            if explicit_target is None and target_table is not None and positional_strings:
                candidate = positional_strings[0]
                candidate_identity = self.table_identity(candidate)
                looks_like_table = (
                    self.same_table(current, candidate)
                    or candidate_identity.get("table_id") is not None
                )
                if not looks_like_table:
                    # 兼容 claim_batch(2, "worker-a", 60)：字符串是领取方而不是表。
                    if principal is None:
                        principal = candidate
                    target_table = None
            if target_table is None:
                identity = current
                target_table = identity.get("name") or identity["raw"]
            else:
                identity = self.table_identity(target_table)
                target_table = identity.get("name") or identity["raw"]
                identity = self.table_identity(target_table)
                # 显式为另一张表领批且要求同步绑定时，把管道切到该表。
                if not self.same_table(current["key"], identity["key"]) and pick(
                    kwargs, "bind_pipeline", "rebind", "sync_binding", default=False
                ):
                    self._rebind_locked(pipeline, target_table, kwargs)

            principal = pick(kwargs, "principal", "writer", "actor", "user", "claimed_by", "owner", "worker", "assignee")
            allowed, reason = self.check_write_permission(target_table, principal, kwargs)
            if not allowed:
                return {"ok": False, "code": "PERMISSION_DENIED", "message": reason, "_http_status": 403}

            source_ref = pick(kwargs, "source_id", "source_ref", "source")
            source = self._source_for(pipeline, source_ref)
            if source_ref is not None and source is None:
                return {"ok": False, "code": "SOURCE_NOT_FOUND", "message": f"数据源 {source_ref} 不存在", "_http_status": 404}
            source_id = _as_int(source.get("id")) if source else _as_int(source_ref)
            partition = pick(kwargs, "partition", "partition_id", "topic_partition", default=0)
            partition = _as_int(partition, 0)

            pipeline_id = _as_int(pipeline.get("id"))
            same_scope = [
                b for b in self.batches
                if _as_int(b.get("pipeline_id")) == pipeline_id
                and _as_int(b.get("source_id")) == source_id
                and _as_int(b.get("partition"), 0) == partition
            ]
            start_offset = _as_int(pick(kwargs, "start_offset", "offset", "from_offset", "start"))
            if start_offset is None:
                starts = [_as_int(b.get("end_offset"), 0) or 0 for b in same_scope]
                start_offset = max(starts, default=None)
            if start_offset is None:
                start_offset = _as_int(pipeline.get("next_offset"))
            if start_offset is None:
                start_offset = self._source_offset(source)

            end_offset = start_offset + row_count
            existing_key = pick(kwargs, "idempotency_key", "idempotency", "request_id")
            if existing_key:
                for old in self.batches:
                    if old.get("idempotency_key") == existing_key:
                        result = dict(old)
                        result.update(ok=True, idempotent=True, duplicate=False, message="已返回同幂等键批次", batch=dict(old))
                        return result

            batch_id = self._next_id(self.batches)
            generation = _as_int(pipeline.get("ownership_generation"), 1) or 1
            owner = principal
            batch = {
                "id": batch_id,
                "batch_id": batch_id,
                "batch_code": f"B{batch_id:04d}",
                "uuid": pick(kwargs, "uuid", default=f"batch-{pipeline_id}-{batch_id}"),
                "pipeline_id": pipeline_id,
                "pipeline_ref": pipeline.get("管道标识") or pipeline_id,
                "source_id": source_id,
                "source_ref": source.get("来源标识") if source else str(source_ref) if source_ref is not None else None,
                "topic": source.get("主题名称") if source else pick(kwargs, "topic"),
                "partition": partition,
                "start_offset": start_offset,
                "end_offset": end_offset,
                "offset_start": start_offset,
                "offset_end": end_offset,
                "from_offset": start_offset,
                "to_offset": end_offset,
                "source_start_offset": start_offset,
                "source_end_offset": end_offset,
                "exclusive_end_offset": end_offset,
                "last_offset": end_offset - 1,
                "inclusive_end_offset": end_offset - 1,
                "offset_range": [start_offset, end_offset - 1],
                "source_offset": f"source-{source_id}:{start_offset}-{end_offset}",
                "trace_ref": f"src{source_id}-p{pipeline_id}:{start_offset}-{end_offset}",
                "offsets": {str(partition): {"start": start_offset, "end": end_offset, "last": end_offset - 1}},
                "row_count": row_count,
                "claimed_rows": row_count,
                "batch_size": row_count,
                "rows": row_count,
                "target_table": target_table,
                "table": target_table,
                "目标湖表": target_table,
                "table_id": identity.get("table_id"),
                "table_key": identity.get("key"),
                "attributed_table": target_table,
                "last_attributed_table": target_table,
                "original_table": target_table,
                "ownership_generation": generation,
                "grant_generation": generation,
                "grant_id": f"grant-p{pipeline_id}-g{generation}-b{batch_id}",
                "grant_status": "claimed",
                "owner": str(owner) if owner is not None else None,
                "claimed_by": str(owner) if owner is not None else None,
                "idempotency_key": existing_key,
                "state": "claimed",
                "status": "待提交",
                "disposition": "open_pending_commit",
                "todo_state": "pending",
                "pending": True,
                "committed": False,
                "abnormal": False,
                "attempts": 0,
                "created_at": now_iso(),
                "updated_at": now_iso(),
            }
            self.batches.append(batch)
            pipeline["next_offset"] = end_offset
            pipeline["last_batch_id"] = batch_id
            pipeline["source_id"] = source_id
            pipeline["last_start_offset"] = start_offset
            pipeline["last_end_offset"] = end_offset
            pipeline["pending_batch_count"] = sum(1 for b in self.batches if _as_int(b.get("pipeline_id")) == pipeline_id and b.get("pending"))
            ids = pipeline.setdefault("batch_ids", [])
            if batch_id not in ids:
                ids.append(batch_id)
            pipeline_batches = pipeline.setdefault("batches", [])
            if not any(_as_int(b.get("id")) == batch_id for b in pipeline_batches if isinstance(b, dict)):
                pipeline_batches.append(batch)
            pending_ids = pipeline.setdefault("pending_batch_ids", [])
            pending_ids.append(batch_id)

            self._record_event("claim", pipeline_id, batch_id, target_table, {
                "source_id": source_id,
                "start_offset": start_offset,
                "end_offset": end_offset,
                "row_count": row_count,
            })
            result = dict(batch)
            result.update(ok=True, message=f"已领取 {row_count} 行，归属表 {target_table}", batch=dict(batch))
            return result

    def _record_event(self, event_type: str, pipeline_id: int | None, batch_id: int | None, table: Any, details: dict[str, Any]) -> None:
        event = {
            "id": self._next_id(self.events),
            "type": event_type,
            "pipeline_id": pipeline_id,
            "batch_id": batch_id,
            "table": self.table_identity(table)["raw"],
            "details": details,
            "created_at": now_iso(),
        }
        self.events.append(event)

    def _rebind_locked(self, pipeline: dict[str, Any], new_table: Any, options: dict[str, Any]) -> dict[str, Any]:
        old_identity = self._pipeline_table(pipeline)
        new_identity = self.table_identity(new_table)
        new_table = new_identity.get("name") or new_identity["raw"]
        new_identity = self.table_identity(new_table)
        pipeline_id = _as_int(pipeline.get("id"))
        old_table = old_identity.get("name") or old_identity["raw"]
        generation = (_as_int(pipeline.get("ownership_generation"), 1) or 1) + 1
        pipeline["ownership_generation"] = generation
        pipeline["binding_generation"] = generation
        pipeline["目标湖表"] = new_identity["raw"]
        pipeline["target_table"] = new_identity["raw"]
        pipeline["table_id"] = new_identity.get("table_id")
        pipeline["last_rebind_at"] = now_iso()
        history = pipeline.setdefault("binding_history", [])
        history.append({
            "from_table": old_table,
            "to_table": new_identity["raw"],
            "generation": generation,
            "at": now_iso(),
            "reason": options.get("reason") or options.get("remark"),
        })

        revoke = pick(
            options, "revoke_existing", "revoke_old", "revoke_old_grant",
            "fence_old", "fence_existing", "fencing", "invalidate_old",
            default=False,
        )
        precedence = pick(options, "precedence", "grant_precedence", "old_grant_priority", "priority")
        new_priority_flag = pick(options, "new_grant_priority", "new_priority")
        if new_priority_flag is True:
            precedence = "new"
        elif new_priority_flag is False and precedence is None:
            precedence = "old"
        precedence_name = resolve_grant_precedence(precedence=precedence)
        if precedence is not None:
            revoke = precedence_name == "new"
        old_batches = []
        for batch in self.batches:
            if _as_int(batch.get("pipeline_id")) != pipeline_id or batch.get("committed"):
                continue
            batch["last_attributed_table"] = batch.get("attributed_table") or old_table
            batch["rebound_to_table"] = new_identity["raw"]
            batch["rebound_at"] = now_iso()
            batch["current_generation"] = generation
            batch["new_binding_generation"] = generation
            if revoke:
                batch["grant_status"] = "revoked_by_new_binding"
                batch["old_grant_priority"] = False
                batch["can_commit_claimed_range"] = False
                batch["disposition"] = "blocked_pending_manual_resolution"
                batch["state"] = "blocked"
                batch["status"] = "待处理"
                batch["pending"] = True
                batch["pending_reason"] = (
                    f"管道已改绑 {new_identity['raw']}，新高优先级授权接管；"
                    f"旧批仍归属 {batch['last_attributed_table']}，需人工决定回退或按旧归属提交"
                )
                batch["blocked_reason"] = batch["pending_reason"]
            else:
                batch["grant_status"] = "old_claim_retained"
                batch["old_grant_priority"] = True
                batch["can_commit_claimed_range"] = True
                batch["precedence"] = "old_batch_grant"
                batch["disposition"] = "retain_claim_then_commit_original_table"
                batch["claim_state"] = "claimed"
                batch["todo_state"] = "pending"
                batch.setdefault("state", "claimed")
                batch["status"] = "待处理"
                batch["pending"] = True
                batch["pending_reason"] = (
                    f"旧批已领取偏移区间，仍归属 {batch['last_attributed_table']}；"
                    "需按旧归属提交，不能写入改绑后的新表"
                )
                batch["ownership_note"] = "旧批授权在已领取偏移区间内优先，仍须写入领取时归属的表"
            batch["updated_at"] = now_iso()
            old_batches.append(dict(batch))

        self._record_event("rebind", pipeline_id, None, new_identity["raw"], {
            "old_table": old_table,
            "generation": generation,
            "revoke_existing": revoke,
            "old_batch_ids": [b["id"] for b in old_batches],
        })
        pipeline["pending_batch_count"] = sum(
            1 for b in self.batches if _as_int(b.get("pipeline_id")) == pipeline_id and b.get("pending")
        )
        return {
            "ok": True,
            "pipeline_id": pipeline_id,
            "old_table": old_table,
            "target_table": new_identity["raw"],
            "table": new_identity["raw"],
            "ownership_generation": generation,
            "revoke_existing": revoke,
            "old_batches": old_batches,
            "old_batch_ids": [b["id"] for b in old_batches],
            "message": f"管道 {pipeline_id} 已由 {old_table} 改绑到 {new_identity['raw']}",
        }

    def rebind(self, pipeline_ref: Any = None, new_table: Any = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        with self.lock:
            pipeline_ref = pipeline_ref if pipeline_ref is not None else pick(
                kwargs, "pipeline_id", "ingest_id", "entry_id", "id", "pipeline"
            )
            if new_table is None and args:
                new_table = args[0]
            elif new_table is not None and args and "precedence" not in kwargs:
                # 兼容 rebind(pipeline, table, "new"/"old"/True/False) 的位置参数写法。
                kwargs["precedence"] = args[0]
            target = new_table if new_table is not None else pick(
                kwargs, "target_table", "table", "table_name", "new_target_table",
                "new_table", "to_table", "table_id", "target_table_id",
                "lake_table_id", "目标湖表",
            )
            pipeline = self.find_pipeline(pipeline_ref)
            if not pipeline:
                return {"ok": False, "code": "PIPELINE_NOT_FOUND", "message": f"管道 {pipeline_ref} 不存在", "_http_status": 404}
            if target is None or str(target).strip() == "":
                return {"ok": False, "code": "TARGET_TABLE_REQUIRED", "message": "改绑必须提供新的目标湖表", "_http_status": 422}
            principal = pick(kwargs, "principal", "writer", "actor", "user", "operator", "owner")
            allowed, reason = self.check_write_permission(target, principal, kwargs)
            if not allowed:
                return {"ok": False, "code": "PERMISSION_DENIED", "message": reason, "_http_status": 403}
            result = self._rebind_locked(pipeline, target, kwargs)
            result["_http_status"] = 200
            result["pipeline"] = dict(pipeline)
            return result

    # ------------------------------------------------------------------
    # 提交
    # ------------------------------------------------------------------
    def _failure(
        self,
        batch: dict[str, Any] | None,
        code: str,
        message: str,
        *,
        http_status: int = 409,
        commit_entry: dict[str, Any] | None = None,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if batch is not None:
            batch["attempts"] = _as_int(batch.get("attempts"), 0) + 1
            batch["last_attempt_at"] = now_iso()
            batch["last_error_code"] = code
            batch["last_error"] = message
            batch["pending_reason"] = message
            batch["pending"] = True
            batch["updated_at"] = now_iso()
            if code == "PERMISSION_DENIED":
                batch["state"] = "denied"
                batch["status"] = "权限拒绝"
                batch["disposition"] = "denied"
                batch["denied"] = True
            elif code in {"OLD_GRANT_REVOKED", "BATCH_OWNER_MISMATCH"}:
                batch["state"] = "blocked"
                batch["status"] = "待处理"
                batch["disposition"] = "blocked_pending_manual_resolution"
                batch["blocked_reason"] = message
            else:
                batch["state"] = "conflict"
                batch["status"] = "提交冲突"
                batch["disposition"] = "conflict_keep_pending"
                batch["conflict"] = True
                batch["abnormal"] = True
                batch["conflict_reason"] = message
        if commit_entry is not None and code != "PERMISSION_DENIED":
            commit_entry["status"] = "提交冲突"
            commit_entry["pending"] = True
            commit_entry["abnormal"] = True
            commit_entry["last_conflict"] = message
            if batch is not None:
                commit_entry["conflict_batch_id"] = batch.get("id")
        attempt = {
            "id": self._next_id(store.rows("commit_attempt")),
            "batch_id": batch.get("id") if batch else None,
            "commit_id": commit_entry.get("id") if commit_entry else None,
            "code": code,
            "ok": False,
            "message": message,
            "created_at": now_iso(),
        }
        store.rows("commit_attempt").append(attempt)
        result: dict[str, Any] = {
            "ok": False,
            "code": code,
            "conflict": code not in {"PERMISSION_DENIED", "BATCH_NOT_FOUND"},
            "message": message,
            "_http_status": http_status,
            "attempt": attempt,
        }
        if batch is not None:
            result["batch_id"] = batch.get("id")
            result["batch"] = dict(batch)
            result["target_table"] = batch.get("target_table")
            result["last_attributed_table"] = batch.get("last_attributed_table") or batch.get("target_table")
        if commit_entry is not None:
            result["commit_id"] = commit_entry.get("id")
            result["commit_table"] = commit_entry.get("目标湖表") or commit_entry.get("target_table")
        if extra:
            result.update(extra)
        return result

    def validate_version_attribution(
        self,
        commit_ref: Any = None,
        *,
        actual_table: Any = None,
        expected_table: Any = None,
        target_table: Any = None,
        batch_ref: Any = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """只校验提交条目、实际摄入对象和期望归属是否为同一张表。"""
        batch = self.find_batch(batch_ref) if batch_ref is not None else None
        commit = self.find_commit(commit_ref)
        if commit is None:
            return {"ok": False, "code": "COMMIT_NOT_FOUND", "message": f"提交记录 {commit_ref} 不存在", "_http_status": 404}
        commit_table = pick(commit, "目标湖表", "目标表", "target_table", "table")
        attributed = (batch or {}).get("target_table") or target_table or expected_table
        actual = actual_table or target_table or attributed
        mismatches = []
        if attributed is not None and not self.same_table(commit_table, attributed):
            mismatches.append(f"提交条目归属={commit_table} != 批次归属={attributed}")
        if actual is not None and not self.same_table(commit_table, actual):
            mismatches.append(f"提交条目归属={commit_table} != 实际摄入对象={actual}")
        if expected_table is not None and not self.same_table(expected_table, commit_table):
            mismatches.append(f"期望表={expected_table} != 提交条目归属={commit_table}")
        if mismatches:
            message = "湖表版本归属校验失败：" + "；".join(mismatches)
            commit["status"] = "提交冲突"
            commit["pending"] = True
            return {"ok": False, "code": "TABLE_OWNERSHIP_CONFLICT", "conflict": True, "message": message, "_http_status": 409}
        return {
            "ok": True,
            "commit_id": commit.get("id"),
            "target_table": commit_table,
            "table": commit_table,
            "batch_id": (batch or {}).get("id"),
        }

    def commit_batch(self, batch_ref: Any = None, commit_id: Any = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        with self.lock:
            if batch_ref is None:
                batch_ref = pick(kwargs, "batch_id", "batch", "handoff", "id")
            if commit_id is None:
                commit_id = pick(kwargs, "commit_id", "commit_entry_id", "version_id", "commit")
            # 位置参数兼容 commit_batch(batch_id, commit_id)
            if commit_id is None and args:
                commit_id = args[0]
            elif commit_id is not None and args:
                extra = args[0]
                if isinstance(extra, bool) or str(extra).casefold() in {"old", "new", "true", "false"}:
                    kwargs.setdefault("precedence", extra)
                else:
                    kwargs.setdefault("target_table", extra)

            batch = self.find_batch(batch_ref)
            if not batch and isinstance(batch_ref, int):
                # 可能调用方把 pipeline id 传进来；允许提交该管道最近的未完成批。
                pipeline = self.find_pipeline(batch_ref)
                if pipeline:
                    open_batches = [
                        b for b in self.batches
                        if _as_int(b.get("pipeline_id")) == batch_ref and b.get("pending") and not b.get("committed")
                    ]
                    batch = open_batches[-1] if open_batches else None
            if not batch:
                return {
                    "ok": False,
                    "code": "BATCH_NOT_FOUND",
                    "message": f"未找到可提交批次：{batch_ref}",
                    "_http_status": 404,
                }

            batch_id = batch.get("id")
            if batch.get("committed"):
                existing = self._committed_result(batch)
                strict_duplicate = pick(kwargs, "strict_duplicate", "strict", default=True)
                if kwargs.get("idempotent") is True or strict_duplicate is False:
                    existing["ok"] = True
                    existing["idempotent"] = True
                    existing["write_applied"] = False
                else:
                    existing["ok"] = False
                    existing["code"] = "BATCH_ALREADY_COMMITTED"
                    existing["already_committed"] = True
                    existing["write_applied"] = False
                    existing["_http_status"] = 409
                    existing["message"] = f"批次 {batch_id} 已提交，不能重复写入"
                return existing

            attributed_table = batch.get("target_table") or batch.get("attributed_table")
            principal = pick(
                kwargs, "principal", "writer", "actor", "user", "username", "account",
                "owner", "claimed_by", "worker", "worker_id", "assignee", "consumer", "agent",
            )
            allowed, reason = self.check_write_permission(attributed_table, principal, kwargs)
            if not allowed:
                return self._failure(batch, "PERMISSION_DENIED", reason or "无写权限", http_status=403)

            expected_owner = batch.get("owner") or batch.get("claimed_by")
            actor = pick(kwargs, "owner", "claimed_by", "worker", "assignee", "principal", "writer", "actor", "user")
            if (
                expected_owner and actor and str(expected_owner) != str(actor)
                and not kwargs.get("admin_override")
                and not kwargs.get("allow_owner_override")
            ):
                return self._failure(
                    batch, "BATCH_OWNER_MISMATCH",
                    f"批次 {batch_id} 已由 {expected_owner} 领取，{actor} 不能提交",
                    http_status=403,
                )

            # 改绑后的授权优先级。默认旧批领取授权在已领取范围内优先。
            current_generation = None
            pipeline = self.find_pipeline(batch.get("pipeline_id"))
            if pipeline:
                current_generation = _as_int(pipeline.get("ownership_generation"))
            batch_generation = _as_int(batch.get("ownership_generation"), current_generation or 1)
            precedence = pick(kwargs, "precedence", "grant_precedence", "priority", "old_grant_priority")
            new_priority_flag = pick(kwargs, "new_grant_priority", "new_priority")
            if new_priority_flag is True:
                precedence = "new"
            elif new_priority_flag is False and precedence is None:
                precedence = "old"
            if precedence is None and batch.get("old_grant_priority") is False:
                precedence = False
            precedence_name = resolve_grant_precedence(precedence=precedence)
            new_priority = precedence_name == "new"
            if batch.get("grant_status") == "revoked_by_new_binding" and (
                new_priority or (precedence is None and not batch.get("old_grant_priority", True))
            ):
                return self._failure(
                    batch, "OLD_GRANT_REVOKED",
                    f"旧批 {batch_id} 的授权已被新归属授权接管；最后归属表为 {attributed_table}",
                    http_status=409,
                )
            if new_priority and current_generation and batch_generation and batch_generation < current_generation:
                current_table = pipeline.get("目标湖表") if pipeline else None
                return self._failure(
                    batch, "OLD_GRANT_REVOKED",
                    f"新高优先级授权仅允许写当前表 {current_table}；旧批 {batch_id} 仍归属 {attributed_table}",
                    http_status=409,
                )

            commit_entry = self.find_commit(commit_id)
            if commit_id is not None and commit_entry is None:
                return {
                    "ok": False,
                    "code": "COMMIT_NOT_FOUND",
                    "message": f"提交版本条目 {commit_id} 不存在",
                    "_http_status": 404,
                    "batch_id": batch_id,
                }

            commit_table = None
            if commit_entry is not None:
                commit_table = pick(commit_entry, "目标湖表", "目标表", "target_table", "table")
            requested_table = pick(
                kwargs, "target_table", "table", "actual_table", "actual_target_table",
                "expected_table", "table_id", "target_table_id",
            )
            expected_table = pick(kwargs, "expected_table", "expected_target_table")
            actual_table = pick(kwargs, "actual_table", "actual_target_table")

            mismatch_tables: list[str] = []
            for label, value in (
                ("提交条目归属", commit_table),
                ("请求目标", requested_table),
                ("期望归属", expected_table),
                ("实际摄入对象", actual_table),
            ):
                if value is not None and str(value).strip() and not self.same_table(attributed_table, value):
                    mismatch_tables.append(f"{label}={value}")
            if mismatch_tables:
                message = (
                    f"批次 {batch_id} 领取时归属 {attributed_table}，"
                    f"实际摄入/版本校验不一致：{', '.join(mismatch_tables)}；禁止跨表确认成功"
                )
                return self._failure(
                    batch, "TABLE_OWNERSHIP_CONFLICT", message,
                    commit_entry=commit_entry,
                    extra={"expected_table": attributed_table, "commit_table": commit_table},
                )

            if commit_entry is not None and commit_entry.get("status") == "已提交":
                committed_batch = commit_entry.get("committed_batch_id") or commit_entry.get("_committed_batch_id")
                if committed_batch and _as_int(committed_batch) != _as_int(batch_id):
                    return self._failure(
                        batch, "VERSION_ALREADY_COMMITTED",
                        f"提交版本 {commit_entry.get('id')} 已被批次 {committed_batch} 提交",
                        commit_entry=commit_entry,
                    )

            # CAS：锁内再次检查，保证并发请求只有一个能走到这里。
            if batch.get("committed"):
                return self.commit_batch(batch_id, commit_id, **kwargs)

            row_count = _as_int(batch.get("row_count"), 0) or 0
            if kwargs.get("actual_row_count") is not None:
                actual_rows = _as_int(kwargs.get("actual_row_count"))
                if kwargs.get("strict_rows", True) and actual_rows != row_count:
                    return self._failure(
                        batch, "ROW_RANGE_CONFLICT",
                        f"实际写入 {actual_rows} 行与已领取范围 {row_count} 行不一致",
                        commit_entry=commit_entry,
                    )

            committed_at = now_iso()
            watermark = pick(kwargs, "watermark", "提交水位", default=batch.get("end_offset"))
            if commit_entry is None:
                commit_entry = {
                    "id": self._next_id(store.rows("delta_commit")),
                    "提交标识": f"COMMIT-B{batch_id:04d}",
                    "提交时间": committed_at,
                }
                store.rows("delta_commit").append(commit_entry)
            commit_pk = _as_int(commit_entry.get("id"))
            version = pick(kwargs, "version")
            if version is None:
                version = commit_entry.get("version")
            if version is None and commit_pk is not None:
                version = commit_pk + 1000
            table_version = sum(1 for v in self.versions if self.same_table(v.get("target_table"), attributed_table)) + 1

            commit_entry.update({
                "status": "已提交",
                "pending": False,
                "abnormal": False,
                "提交标识": commit_entry.get("提交标识") or f"提交版本{commit_pk}",
                "目标湖表": attributed_table,
                "target_table": attributed_table,
                "table": attributed_table,
                "提交类型": kwargs.get("commit_type") or commit_entry.get("提交类型") or "流式批次提交",
                "row_count": row_count,
                "claimed_rows": row_count,
                "新增行数": row_count,
                "updated_rows": _as_int(kwargs.get("updated_rows"), 0) or 0,
                "更新行数": _as_int(kwargs.get("updated_rows"), 0) or 0,
                "删除行数": _as_int(kwargs.get("deleted_rows"), 0) or 0,
                "提交水位": watermark,
                "watermark": watermark,
                "提交时间": committed_at,
                "committed_at": committed_at,
                "version": version,
                "lake_table_version": table_version,
                "source_id": batch.get("source_id"),
                "source_ref": batch.get("source_ref"),
                "source": batch.get("source_ref") or batch.get("source_id"),
                "topic": batch.get("topic"),
                "pipeline_ref": batch.get("pipeline_ref"),
                "pipeline": batch.get("pipeline_ref") or batch.get("pipeline_id"),
                "pipeline_id": batch.get("pipeline_id"),
                "batch_id": batch_id,
                "start_offset": batch.get("start_offset"),
                "end_offset": batch.get("end_offset"),
                "source_offset": batch.get("source_offset"),
                "trace_ref": batch.get("trace_ref"),
                "committed_batch_id": batch_id,
            })

            version_record = dict(commit_entry)
            version_record.update({
                "table_version": table_version,
                "record_type": "lake_table_version_attribution",
                "attributed_table": attributed_table,
                "actual_target_table": attributed_table,
                "source": batch.get("source_ref") or batch.get("source_id"),
                "offset": {
                    "partition": batch.get("partition"),
                    "start": batch.get("start_offset"),
                    "end": batch.get("end_offset"),
                },
            })
            self.versions.append(version_record)

            # 若 A/B 是 lake_table 中登记的真实湖表，同步表定义上的当前版本与归属链。
            table_identity = self.table_identity(attributed_table)
            if table_identity.get("table_id") is not None:
                table_row = store.find("lake_table", int(table_identity["table_id"]))
                if table_row is not None:
                    table_row["表版本"] = table_version
                    table_row["table_version"] = table_version
                    table_row["version"] = version
                    table_row["当前快照"] = f"snapshot-{attributed_table}-v{table_version}"
                    table_row["current_snapshot"] = table_row["当前快照"]
                    records = table_row.setdefault("version_records", [])
                    records.append({
                        "version": version,
                        "table_version": table_version,
                        "commit_id": commit_pk,
                        "batch_id": batch_id,
                        "source_id": batch.get("source_id"),
                        "start_offset": batch.get("start_offset"),
                        "end_offset": batch.get("end_offset"),
                        "committed_at": committed_at,
                    })

            batch.update({
                "state": "committed",
                "status": "已提交",
                "pending": False,
                "pending_reason": None,
                "committed": True,
                "abnormal": False,
                "disposition": "committed_to_attributed_table",
                "committed_at": committed_at,
                "commit_id": commit_pk,
                "version": version,
                "table_version": table_version,
                "watermark": watermark,
                "updated_at": committed_at,
                "write_applied": True,
            })
            if pipeline:
                pipeline["pending_batch_count"] = sum(
                    1 for b in self.batches if _as_int(b.get("pipeline_id")) == pipeline.get("id") and b.get("pending")
                )
                pending_ids = pipeline.get("pending_batch_ids")
                if isinstance(pending_ids, list) and batch_id in pending_ids:
                    pending_ids.remove(batch_id)
            self._record_event("commit", batch.get("pipeline_id"), batch_id, attributed_table, {
                "commit_id": commit_pk,
                "version": version,
                "row_count": row_count,
            })
            result = self._committed_result(batch)
            result.update({
                "ok": True,
                "write_applied": True,
                "idempotent": False,
                "commit_id": commit_pk,
                "version": version,
                "table_version": table_version,
                "commit": dict(commit_entry),
                "_http_status": 200,
            })
            return result

    def _committed_result(self, batch: dict[str, Any]) -> dict[str, Any]:
        return {
            "batch_id": batch.get("id"),
            "id": batch.get("id"),
            "pipeline_id": batch.get("pipeline_id"),
            "source_id": batch.get("source_id"),
            "target_table": batch.get("target_table"),
            "table": batch.get("target_table"),
            "last_attributed_table": batch.get("last_attributed_table") or batch.get("target_table"),
            "row_count": batch.get("row_count"),
            "claimed_rows": batch.get("row_count"),
            "new_rows": batch.get("row_count"),
            "added_rows": batch.get("row_count"),
            "ingested_rows": batch.get("row_count"),
            "start_offset": batch.get("start_offset"),
            "end_offset": batch.get("end_offset"),
            "status": batch.get("status"),
            "pending": batch.get("pending"),
            "committed": bool(batch.get("committed")),
            "commit_id": batch.get("commit_id"),
            "version": batch.get("version"),
            "table_version": batch.get("table_version"),
            "batch": dict(batch),
        }

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def list_batches(self, **filters: Any) -> list[dict[str, Any]]:
        rows = list(self.batches)
        pipeline_id = pick(filters, "pipeline_id", "ingest_id", "entry_id")
        if pipeline_id is not None:
            pid = _as_int(pipeline_id)
            rows = [b for b in rows if _as_int(b.get("pipeline_id")) == pid]
        table = pick(filters, "table", "target_table", "attributed_table")
        if table is not None:
            rows = [b for b in rows if self.same_table(b.get("target_table"), table)]
        status = filters.get("status") or filters.get("state")
        if status:
            text = str(status)
            rows = [b for b in rows if text.casefold() in str(b.get("status", "")).casefold() or text == b.get("state")]
        pending = filters.get("pending")
        if pending is not None:
            want = str(pending).casefold() not in {"false", "0", "no", "否"} if isinstance(pending, str) else bool(pending)
            rows = [b for b in rows if bool(b.get("pending")) == want]
        owner = filters.get("owner") or filters.get("claimed_by")
        if owner:
            rows = [b for b in rows if str(b.get("owner") or b.get("claimed_by")) == str(owner)]
        return rows

    def list_todos(self, **filters: Any) -> list[dict[str, Any]]:
        filters["pending"] = True
        return self.list_batches(**filters)

    def list_versions(self, table: Any = None, **filters: Any) -> list[dict[str, Any]]:
        rows = list(self.versions)
        if table is None:
            table = pick(filters, "target_table", "table_name", "table")
        if table is not None:
            rows = [v for v in rows if self.same_table(v.get("target_table") or v.get("table"), table)]
        pipeline_id = pick(filters, "pipeline_id", "ingest_id")
        if pipeline_id is not None:
            pid = _as_int(pipeline_id)
            rows = [v for v in rows if _as_int(v.get("pipeline_id")) == pid]
        source_id = filters.get("source_id")
        if source_id is not None:
            sid = _as_int(source_id, source_id)
            rows = [v for v in rows if _as_int(v.get("source_id")) == sid]
        return rows

    def table_versions(self, table: Any = None, **filters: Any) -> dict[str, Any]:
        rows = self.list_versions(table, **filters)
        return {
            "ok": True,
            "table": self.table_identity(table)["raw"] if table is not None else None,
            "target_table": self.table_identity(table)["raw"] if table is not None else None,
            "items": rows,
            "versions": rows,
            "total": len(rows),
            "row_count": sum(_as_int(v.get("row_count") or v.get("新增行数"), 0) or 0 for v in rows),
            "committed_rows": sum(_as_int(v.get("row_count") or v.get("新增行数"), 0) or 0 for v in rows),
        }

    def attribution_report(self, pipeline_ref: Any = None) -> dict[str, Any]:
        pipeline = self.find_pipeline(pipeline_ref) if pipeline_ref is not None else None
        pid = pipeline.get("id") if pipeline else None
        batches = self.list_batches(pipeline_id=pid) if pid is not None else list(self.batches)
        tables = sorted({str(b.get("target_table")) for b in batches if b.get("target_table")})
        return {
            "ok": True,
            "pipeline": dict(pipeline) if pipeline else None,
            "batches": batches,
            "pending_batches": [b for b in batches if b.get("pending")],
            "tables": tables,
            "versions_by_table": {table: self.table_versions(table) for table in tables},
            "events": [e for e in self.events if pid is None or _as_int(e.get("pipeline_id")) == _as_int(pid)],
        }


coordinator = IngestCoordinator()
