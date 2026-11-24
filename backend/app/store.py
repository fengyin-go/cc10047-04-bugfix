"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。
"""
from __future__ import annotations

from typing import Any

from app.seed import SEED_ROWS


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        self._align_stream_ownership_seed()

    def _align_stream_ownership_seed(self) -> None:
        """让内置复现样例具备一致的来源/目标归属。

        源 1、管道 2、提交条目 3 刻意保留为“管道 -> A，提交条目 -> B”的
        跨表冲突样例；ID 1/1/1 则是可成功走完链路的一致样例。
        """
        source_1 = self.find("stream_source", 1)
        if source_1 is not None:
            source_1.setdefault("source_id", 1)
            source_1.setdefault("start_offset", 0)

        pipeline_1 = self.find("stream_ingest", 1)
        commit_1 = self.find("delta_commit", 1)
        coherent_table = "湖表定义样例1"
        if pipeline_1 is not None:
            pipeline_1.setdefault("source_id", 1)
            pipeline_1["目标湖表"] = coherent_table
            pipeline_1["target_table"] = coherent_table
            pipeline_1.setdefault("ownership_generation", 1)
        if commit_1 is not None:
            commit_1["目标湖表"] = coherent_table
            commit_1["target_table"] = coherent_table

        pipeline_2 = self.find("stream_ingest", 2)
        if pipeline_2 is not None:
            pipeline_2.setdefault("source_id", 1)
            pipeline_2["来源流"] = source_1.get("来源标识") if source_1 else pipeline_2.get("来源流")
            pipeline_2["目标湖表"] = "A"
            pipeline_2["target_table"] = "A"
            pipeline_2.setdefault("ownership_generation", 1)

        commit_3 = self.find("delta_commit", 3)
        if commit_3 is not None:
            commit_3["目标湖表"] = "B"
            commit_3["target_table"] = "B"

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        # 批次/版本归属等内部状态表不计入业务模块概览。
        for name in SEED_ROWS.keys():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}

    def reset(self) -> None:
        """恢复示例数据；主要供测试隔离使用。"""
        self._tables.clear()
        self._tables.update({
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        })
        self._align_stream_ownership_seed()
        # 协调器在 store 中维护别名列表，reset 后需要重新绑定。
        try:
            from app.ingest_coordinator import coordinator
            coordinator._ensure_store()
        except Exception:
            pass


store = Store()
