"""路由汇总：批次归属专用接口先注册，随后是通用资源与业务链路路由。"""
from app.routers import ingest_ownership, resource, chains

ROUTERS = [ingest_ownership, resource, chains]
