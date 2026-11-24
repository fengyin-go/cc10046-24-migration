"""网络数据采集与RPA自动化平台 后端服务入口。

启动：uvicorn app.main:app --host 127.0.0.1 --port 8010
健康检查：GET /api/health
"""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.routers import ROUTERS
from app.routers.chains import run_data_process, run_data_process_v2
from app.routers.product_dedup import (
    batch_detail as product_batch_detail,
    claim as product_claim,
    core_router as product_dedup_router,
    data_processing_get as product_data_processing_get,
    lookup_key as product_lookup_key,
    migrate as product_migrate,
    short_router as product_dedup_short_router,
)
from app.store import store

app = FastAPI(title="网络数据采集与RPA自动化平台", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/health")
def health() -> dict[str, object]:
    """健康检查：确认服务已经监听、示例数据已经就绪。"""
    return {"ok": True, "app": settings.app_name, "modules": len(store.module_names())}


@app.get("/api/overview")
def overview() -> dict[str, object]:
    """运营概览：把各业务模块的待处理量汇总成看板卡片。"""
    return store.overview()


# 商品去重 V2 迁移是带字符串批次号的专用接口，必须先于 /api/{module}/{entry_id}
# 这类动态资源路由注册，避免被 entry_id:int 匹配拦截。
PRODUCT_DEDUP_PREFIXES = (
    "/api/product-dedup",
    "/api/product_dedup",
    "/api/dedup-migration",
    "/api/dedup_migration",
    "/api/dedup-engine-migration",
    "/api/dedup_engine_migration",
    "/api/product-dedup-migration",
    "/api/product_dedup_migration",
    "/api/product-batches",
    "/api/product_batches",
    "/api/batches",
    "/api/batch",
    "/api/product-dedup-batches",
    "/api/product_dedup_batches",
    "/api/dedup",
    "/api/dedup-engine",
    "/api/migration/product-dedup",
)
for prefix in PRODUCT_DEDUP_PREFIXES:
    app.include_router(product_dedup_router, prefix=prefix)
    app.include_router(product_dedup_short_router, prefix=prefix)

# 顶层直观别名：/api/batches、/api/receipts、/api/claim、/api/keys/lookup。
# 只挂不含 /{batch_id} 根动态路由的 core_router，避免抢占 /api/{module}。
app.include_router(product_dedup_router, prefix="/api")
app.include_router(product_dedup_short_router, prefix="/api/batches")
app.include_router(product_dedup_short_router, prefix="/api/batch")
app.post("/api/migrate")(product_migrate)
app.post("/api/claim")(product_claim)

# 数据处理接口的无 chain 前缀别名；主契约仍保留在 /api/chains/data-process*.
app.post("/api/data-process")(run_data_process)
app.post("/api/data-processing")(run_data_process)
app.post("/api/data-process-v2")(run_data_process_v2)
app.post("/api/data-processing-v2")(run_data_process_v2)
app.post("/api/data-processing/v2")(run_data_process_v2)
app.post("/api/v2/data-process")(run_data_process_v2)

# 给已有 dedup_engine 模块命名空间补的场景别名；必须先于通用 /api/{module} 路由。
app.add_api_route("/api/dedup_engine/keys/lookup", product_lookup_key, methods=["GET", "POST"])
app.add_api_route("/api/dedup_engine/batches/{batch_id}", product_batch_detail, methods=["GET"])
app.add_api_route("/api/dedup_engine/batches/{batch_id}/data-processing", product_data_processing_get, methods=["GET", "POST"])
app.add_api_route("/api/dedup-engine/keys/lookup", product_lookup_key, methods=["GET", "POST"])
app.post("/api/dedup_engine/data-process-v2")(run_data_process_v2)
app.post("/api/dedup-engine/data-process-v2")(run_data_process_v2)
app.post("/api/dedup_engine/migrate")(product_migrate)
app.post("/api/dedup-engine/migrate")(product_migrate)
app.post("/api/dedup_engine/claim")(product_claim)
app.post("/api/dedup-engine/claim")(product_claim)


# 资源路由含 /api/{module}，必须放在 health / overview 之后注册，
# 否则固定路径会被动态路径抢先匹配。
for module in ROUTERS:
    app.include_router(module.router)
