"""商品去重 V2 迁移接口。

同一组无参数路由会在 main 中挂载到多个常见路径别名下，避免调用方对
product-dedup / dedup-migration 等命名偏好不一致。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, HTTPException, Query, Request

from app.services.product_dedup import (
    EvidenceMissingError,
    product_dedup_service as service,
)

core_router = APIRouter(tags=["商品去重迁移"])


def _not_found(exc: KeyError) -> HTTPException:
    return HTTPException(status_code=404, detail=str(exc.args[0] if exc.args else exc))


def _evidence_error(exc: EvidenceMissingError) -> HTTPException:
    return HTTPException(
        status_code=409,
        detail={
            "ok": False,
            "error": "evidence_missing",
            "batch_id": exc.batch_id,
            "reason": exc.reason,
            "pending_reason": exc.reason,
            "missing_evidence": exc.missing_evidence,
            "new_counts_available": False,
        },
    )


# ---------- 清单、领取、样本键查询（固定路径必须在动态批次路径前注册） ----------

@core_router.get("/manifest")
def manifest_only() -> dict[str, Any]:
    return {
        "name": "product-dedup-migration",
        "version": 2,
        "endpoints": [
            "GET /batches",
            "POST /batches",
            "GET /batches/{batch_id}",
            "POST /batches/{batch_id}/backfill",
            "POST /batches/{batch_id}/continue",
            "POST /batches/{batch_id}/legacy-dedup",
            "POST /batches/{batch_id}/publish",
            "GET|POST /batches/{batch_id}/data-processing",
            "POST /claim",
            "GET /pending-backfill",
            "GET /keys/lookup",
            "POST /keys/lookup",
            "POST /receipts/{receipt_id}/reopen",
        ],
    }


@core_router.get("")
@core_router.get("/")
def manifest() -> dict[str, Any]:
    result = service.list_batches()
    result.update({
        "name": "product-dedup-migration",
        "version": 2,
        "manifest": "/manifest",
    })
    return result


@core_router.get("/batches")
def list_batches(status: str | None = Query(default=None), scope: str | None = Query(default=None)) -> dict[str, Any]:
    return service.list_batches(status=status, scope=scope)


@core_router.post("/batches")
def create_batch(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    return service.create_batch(payload)


@core_router.get("/pending-backfill")
def pending_backfill(scope: str | None = Query(default=None)) -> dict[str, Any]:
    result = service.list_batches(status="待回填", scope=scope)
    result["missing_evidence_only"] = True
    return result


@core_router.post("/claim")
def claim(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    return service.claim_new_version(payload)


@core_router.post("/migrate")
def migrate(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    batch_ref = payload.get("batch_id") or payload.get("batch") or payload.get("批次标识")
    if batch_ref is not None:
        try:
            return service.migrate_batch(batch_ref, payload)
        except KeyError as exc:
            raise _not_found(exc)
    readiness = service.claim_new_version(payload)
    readiness["migration_action"] = "listed_scope"
    return readiness


@core_router.post("/batches/{batch_id}/migrate")
def migrate_batch(batch_id: str, payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    try:
        return service.migrate_batch(batch_id, payload)
    except KeyError as exc:
        raise _not_found(exc)
    except EvidenceMissingError as exc:
        raise _evidence_error(exc)


@core_router.get("/readiness")
def readiness_all(scope: str | None = Query(default=None)) -> dict[str, Any]:
    return service.claim_new_version({"delivery_scope": scope} if scope else {})


@core_router.api_route("/keys/lookup", methods=["GET", "POST"])
async def lookup_key(request: Request) -> dict[str, Any]:
    if request.method == "POST":
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        query = dict(request.query_params)
        if isinstance(payload, dict):
            query.update(payload)
    else:
        payload = {}
        query = dict(request.query_params)
    return service.lookup_products(payload if isinstance(payload, dict) else {}, query)


@core_router.get("/products/by-key")
def lookup_by_key_get(request: Request) -> dict[str, Any]:
    return service.lookup_products({}, dict(request.query_params))


@core_router.get("/receipts")
def list_receipts() -> dict[str, Any]:
    return {"items": list(service.receipts.values()), "total": len(service.receipts)}


@core_router.get("/receipts/{receipt_id}")
def get_receipt(receipt_id: str) -> dict[str, Any]:
    try:
        return service.get_receipt(receipt_id)
    except KeyError as exc:
        raise _not_found(exc)


@core_router.post("/receipts/{receipt_id}/reopen")
def reopen_receipt(receipt_id: str, payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    try:
        return service.reopen_receipt(receipt_id, payload)
    except KeyError as exc:
        raise _not_found(exc)


@core_router.post("/testing/reset")
def reset_scenario(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    return service.reset(payload)


# ---------- 批次详情和操作 ----------

@core_router.get("/batches/{batch_id}")
def batch_detail(batch_id: str) -> dict[str, Any]:
    try:
        return service.batch_detail(batch_id)
    except KeyError as exc:
        raise _not_found(exc)


@core_router.get("/batches/{batch_id}/detail")
def batch_detail_alias(batch_id: str) -> dict[str, Any]:
    try:
        return service.batch_detail(batch_id)
    except KeyError as exc:
        raise _not_found(exc)


@core_router.put("/batches/{batch_id}")
@core_router.patch("/batches/{batch_id}")
def update_batch(batch_id: str, payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    try:
        return service.update_batch(batch_id, payload)
    except KeyError as exc:
        raise _not_found(exc)


@core_router.post("/batches/{batch_id}/raw-rows")
@core_router.post("/batches/{batch_id}/evidence/raw-rows")
@core_router.post("/batches/{batch_id}/backfill")
def backfill(batch_id: str, payload: Any = Body(default_factory=dict)) -> dict[str, Any]:
    try:
        return service.backfill_raw_rows(batch_id, payload)
    except KeyError as exc:
        raise _not_found(exc)
    except EvidenceMissingError as exc:
        raise _evidence_error(exc)


@core_router.post("/batches/{batch_id}/continue")
@core_router.post("/batches/{batch_id}/resume")
@core_router.post("/batches/{batch_id}/continue-migration")
def continue_migration(batch_id: str, payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    try:
        return service.continue_migration(batch_id, payload)
    except KeyError as exc:
        raise _not_found(exc)
    except EvidenceMissingError as exc:
        raise _evidence_error(exc)


@core_router.post("/batches/{batch_id}/legacy-dedup")
@core_router.post("/batches/{batch_id}/run-legacy")
def legacy_dedup(batch_id: str, payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    try:
        return service.run_legacy_dedup(batch_id, payload)
    except KeyError as exc:
        raise _not_found(exc)
    except EvidenceMissingError as exc:
        raise _evidence_error(exc)


@core_router.post("/batches/{batch_id}/publish")
@core_router.post("/batches/{batch_id}/publish-v2")
def publish_v2(batch_id: str, payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    try:
        return service.publish_v2(batch_id, payload)
    except KeyError as exc:
        raise _not_found(exc)
    except EvidenceMissingError as exc:
        raise _evidence_error(exc)


@core_router.get("/batches/{batch_id}/data-processing")
def data_processing_get(
    batch_id: str,
    version: str | int = Query(default=2),
    publication_id: str | None = Query(default=None),
) -> dict[str, Any]:
    try:
        return service.data_processing(batch_id, version, publication_id=publication_id)
    except KeyError as exc:
        raise _not_found(exc)


@core_router.post("/batches/{batch_id}/data-processing")
def data_processing_post(
    batch_id: str,
    payload: dict[str, Any] = Body(default_factory=dict),
) -> dict[str, Any]:
    version = payload.get("version", payload.get("dedup_version", 2))
    publication_id = payload.get("publication_id") or payload.get("dedup_publication_id")
    try:
        return service.data_processing(batch_id, version, publication_id=publication_id)
    except KeyError as exc:
        raise _not_found(exc)


@core_router.post("/batches/{batch_id}/actions")
def batch_action(batch_id: str, payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    """兼容现有资源接口的 action 风格。"""
    action = str(payload.get("action", "")).strip()
    try:
        if action in {"补交原始行", "回填", "backfill"}:
            return service.backfill_raw_rows(batch_id, payload.get("values") or payload)
        if action in {"续做", "继续迁移", "continue", "resume"}:
            return service.continue_migration(batch_id, payload.get("values") or payload)
        if action in {"迁移", "迁移去重键", "首次迁移", "migrate"}:
            return service.migrate_batch(batch_id, payload.get("values") or payload)
        if action in {"发布", "发布V2", "发布版本2", "publish"}:
            return service.publish_v2(batch_id, payload)
        if action in {"旧版去重", "运行旧版去重", "legacy-dedup"}:
            return service.run_legacy_dedup(batch_id, payload)
        if action in {"重开回执", "reopen"}:
            receipt_id = payload.get("receipt_id") or service.get_batch(batch_id).get("old_receipt_id")
            return service.reopen_receipt(receipt_id, payload)
        if action in {"领取新版", "claim"}:
            return service.readiness(batch_id)
        raise HTTPException(status_code=400, detail=f"不支持的商品去重迁移动作：{action}")
    except KeyError as exc:
        raise _not_found(exc)
    except EvidenceMissingError as exc:
        raise _evidence_error(exc)


# ---------- 短路径别名：/{batch_id} 与主路由分开挂载 ----------

short_router = APIRouter(tags=["商品去重迁移短路径"])


@short_router.get("/{batch_id}")
def short_batch_detail(batch_id: str) -> dict[str, Any]:
    # 避免把 known top-level words 当成批次。
    if batch_id in {"claim", "keys", "receipts", "pending-backfill", "readiness", "products", "testing", "migrate", "batches", "manifest"}:
        raise HTTPException(status_code=404)
    try:
        return service.batch_detail(batch_id)
    except KeyError as exc:
        raise _not_found(exc)


@short_router.post("/{batch_id}/backfill")
def short_backfill(batch_id: str, payload: Any = Body(default_factory=dict)) -> dict[str, Any]:
    try:
        return service.backfill_raw_rows(batch_id, payload)
    except KeyError as exc:
        raise _not_found(exc)
    except EvidenceMissingError as exc:
        raise _evidence_error(exc)


@short_router.post("/{batch_id}/continue")
def short_continue(batch_id: str, payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    try:
        return service.continue_migration(batch_id, payload)
    except KeyError as exc:
        raise _not_found(exc)
    except EvidenceMissingError as exc:
        raise _evidence_error(exc)
