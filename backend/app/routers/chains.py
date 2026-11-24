"""跨模块业务链路：多条业务链把多个领域服务串成完整流程。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from app.services.crawler_config import CrawlerConfigService
from app.services.crawl_schedule import CrawlScheduleService
from app.services.batch_tracker import BatchTrackerService
from app.services.result_store import ResultStoreService
from app.services.parser_rule import ParserRuleService
from app.services.field_mapper import FieldMapperService
from app.services.html_cleaner import HtmlCleanerService
from app.services.dedup_engine import DedupEngineService
from app.services.proxy_pool import ProxyPoolService
from app.services.rate_limiter import RateLimiterService
from app.services.error_retry import ErrorRetryService
from app.services.error_logger import ErrorLoggerService
from app.services.cookie_mgr import CookieMgrService
from app.services.source_auth import SourceAuthService
from app.services.session_manager import SessionManagerService
from app.services.export_channel import ExportChannelService
from app.services.crawl_audit import CrawlAuditService
from app.services.data_cleanup import DataCleanupService
from app.services.product_dedup import product_dedup_service

router = APIRouter(prefix="/api/chains", tags=["业务链路"])

_crawler = CrawlerConfigService()
_schedule = CrawlScheduleService()
_batch = BatchTrackerService()
_store = ResultStoreService()
_parse = ParserRuleService()
_mapper = FieldMapperService()
_cleaner = HtmlCleanerService()
_dedup = DedupEngineService()
_proxy = ProxyPoolService()
_limiter = RateLimiterService()
_retry = ErrorRetryService()
_logger = ErrorLoggerService()
_cookie = CookieMgrService()
_auth = SourceAuthService()
_session = SessionManagerService()
_export = ExportChannelService()
_audit = CrawlAuditService()
_cleanup = DataCleanupService()


@router.post("/crawl-pipeline")
def run_crawl_pipeline(payload: dict[str, Any]) -> dict[str, Any]:
    """采集流水线：配置校验 → 生成调度 → 批次追踪 → 结果存储。"""
    config_id = payload.get("config_id", 1)
    config_name = payload.get("config_name", "default")
    period = payload.get("period", "daily")
    batch_label = payload.get("batch_label", "BATCH-1")
    row_count = payload.get("row_count", 100)
    valid_count = payload.get("valid_count", 95)

    vresult = _crawler.domain_validate_config(config_id)
    sresult = _schedule.domain_create_schedule(config_name, period)
    bresult = _batch.domain_track_batch(batch_label, sresult["task_id"])
    storeresult = _store.domain_store_result(batch_label, config_name, row_count, valid_count)

    return {
        "chain": "crawl-pipeline",
        "steps": {
            "validate_config": vresult,
            "create_schedule": sresult,
            "track_batch": bresult,
            "store_result": storeresult,
        },
    }


def _batch_ref(payload: dict[str, Any]) -> Any:
    return (
        payload.get("batch_id")
        or payload.get("dedup_batch_id")
        or payload.get("batch")
        or payload.get("批次标识")
        or payload.get("批次ID")
    )


def _wants_v2(payload: dict[str, Any]) -> bool:
    version = payload.get("version", payload.get("dedup_version", payload.get("key_version")))
    return str(version).lower() in {"2", "v2"} or payload.get("interface") in {"new_v2", "v2"}


@router.post("/data-process")
@router.post("/data-processing")
def run_data_process(payload: dict[str, Any]) -> dict[str, Any]:
    """数据处理：HTML解析 → 字段映射 → HTML清洗 → 去重。

    不带显式 V2 标记时，本接口严格保留旧响应字段和 V1 拼接键口径。
    若传入商品迁移批次，则去重输入取该批次真实原始行；乙批次只有旧汇总时，
    返回最早留档的旧三计数。
    """
    if _wants_v2(payload):
        result = run_data_process_v2(payload)
        # 同一旧路径显式请求 V2 时，保留原 chain 名称，版本由 read_version 标出。
        result["chain"] = "data-process"
        return result

    html = payload.get("html", "<html><body><p>测试内容</p></body></html>")
    source_data = payload.get("source_data", {"name": "test", "count": "42"})
    mapping_rules = payload.get("mapping_rules", [
        {"source": "name", "target": "title", "default": "", "transform": ""},
        {"source": "count", "target": "total", "default": "0", "transform": "int"},
    ])
    key_fields = payload.get("key_fields", ["title"])
    batch_id = _batch_ref(payload)

    parse_result = _parse.domain_parse_html(html)
    map_result = _mapper.domain_map_fields(source_data, mapping_rules)
    clean_result = _cleaner.domain_clean_html(html)

    supplied_items = None
    for row_key in ("items", "rows", "dedup_items", "source_rows", "product_rows", "raw_rows"):
        if payload.get(row_key) is not None:
            supplied_items = payload[row_key]
            break

    batch_context: dict[str, Any] | None = None
    if batch_id is not None:
        batch = product_dedup_service.get_batch(batch_id)
        batch_context = {"batch_id": batch["batch_id"], "batch": batch}
        if payload.get("key_fields") is None:
            key_fields = batch["key_fields"]
        if supplied_items is None and batch.get("raw_rows_available"):
            supplied_items = batch["raw_rows"]
        if supplied_items is None and batch.get("old_counts"):
            old = batch["old_counts"]
            dedup_step = {
                "original_count": old["original_count"],
                "deduped_count": old["deduped_count"],
                "removed_count": old["removed_count"],
            }
            response = {
                "chain": "data-process",
                "steps": {
                    "parse_html": parse_result,
                    "map_fields": map_result,
                    "clean_html": {"original_length": clean_result["original_length"], "cleaned_length": clean_result["cleaned_length"]},
                    "dedup": dedup_step,
                },
                "batch_id": batch["batch_id"],
                "source": "archived_old_summary",
                "archived_old_counts": old,
                "matches_archived_old_counts": True,
            }
            return response

    if supplied_items is None:
        items = [map_result, {"title": "extra", "other": "value"}]
    else:
        items = supplied_items

    # 默认即是 V1/旧拼接键口径；不传额外参数，兼容旧测试中的两参替身。
    dedup_result = _dedup.domain_dedup(items, key_fields)
    dedup_step = {
        "original_count": dedup_result["original_count"],
        "deduped_count": dedup_result["deduped_count"],
        "removed_count": dedup_result["removed_count"],
    }
    response = {
        "chain": "data-process",
        "steps": {
            "parse_html": parse_result,
            "map_fields": map_result,
            "clean_html": {"original_length": clean_result["original_length"], "cleaned_length": clean_result["cleaned_length"]},
            "dedup": dedup_step,
        },
    }
    if batch_context:
        batch = batch_context["batch"]
        response["batch_id"] = batch_context["batch_id"]
        archived = batch.get("old_counts")
        response["archived_old_counts"] = archived
        if archived:
            response["matches_archived_old_counts"] = all(
                dedup_step[key] == archived.get(key)
                for key in ("original_count", "deduped_count", "removed_count")
            )
    return response


@router.post("/data-process-v2")
@router.post("/data-process/v2")
@router.post("/data-processing-v2")
@router.post("/data-processing/v2")
@router.post("/v2/data-process")
@router.post("/v2/data-processing")
def run_data_process_v2(payload: dict[str, Any]) -> dict[str, Any]:
    """新版数据处理：只读取该批次实际发布的 V2 商品去重数据。"""
    batch_id = _batch_ref(payload) or payload.get("id")
    publication_id = (
        payload.get("publication_id")
        or payload.get("dedup_publication_id")
        or payload.get("v2_publication_id")
    )
    if batch_id is None and publication_id:
        publication = product_dedup_service.publications.get(str(publication_id))
        if publication:
            batch_id = publication.get("batch_id")
    if batch_id is None:
        return {
            "chain": "data-process-v2",
            "ok": False,
            "available": False,
            "reason": "缺少 batch_id；新版计数必须对应一批实际发布的版本 2 商品去重数据。",
            "pending_reason": "缺少 batch_id；新版计数必须对应一批实际发布的版本 2 商品去重数据。",
            "steps": {"dedup": None},
        }
    result = product_dedup_service.data_processing(batch_id, version=2, publication_id=publication_id)
    result.setdefault("chain", "data-process-v2")
    return result


@router.post("/reliability")
def run_reliability(payload: dict[str, Any]) -> dict[str, Any]:
    """可靠性保障：代理分配 → 限速检查 → 重试计算 → 错误记录。"""
    site = payload.get("site", "example.com")
    protocol = payload.get("protocol", "http")
    request_count = payload.get("request_count", 100)
    error_type = payload.get("error_type", "timeout")
    attempt = payload.get("attempt", 2)

    proxy_result = _proxy.domain_allocate_proxy(protocol)
    rate_result = _limiter.domain_check_rate(site, request_count)
    retry_result = _retry.domain_retry_calc(error_type, attempt)
    log_result = _logger.domain_log_error(
        "ERR-CHECK", f"可靠性检查 {site}", site)

    return {
        "chain": "reliability",
        "steps": {
            "allocate_proxy": proxy_result,
            "check_rate": rate_result,
            "retry_calc": retry_result,
            "log_error": {"error_code": log_result["错误码"], "error_message": log_result["错误信息"]},
        },
    }


@router.post("/auth")
def run_auth(payload: dict[str, Any]) -> dict[str, Any]:
    """认证链路：Cookie获取 → 来源认证 → 会话创建。"""
    site = payload.get("site", "example.com")
    auth_type = payload.get("auth_type", "token")
    session_type = payload.get("session_type", "http")

    cookie_result = _cookie.domain_get_cookies(site)
    auth_result = _auth.domain_authenticate(site, auth_type)
    session_id = f"{site}-session-1"
    session_result = _session.domain_create_session(session_id, site, session_type)

    return {
        "chain": "auth",
        "steps": {
            "get_cookies": cookie_result,
            "authenticate": auth_result,
            "create_session": session_result,
        },
    }


@router.post("/delivery")
def run_delivery(payload: dict[str, Any]) -> dict[str, Any]:
    """交付链路：结果存储 → 导出 → 审计 → 数据清理。"""
    batch_id = payload.get("batch_id", "BATCH-DELIVERY-1")
    config_name = payload.get("config_name", "delivery")
    export_format = payload.get("export_format", "csv")
    fields = payload.get("fields", ["id", "title", "count"])
    cleanup_module = payload.get("cleanup_module", "result_store")

    store_result = _store.domain_store_result(batch_id, config_name, 200, 180)
    export_result = _export.domain_export(batch_id, export_format, fields)
    audit_result = _audit.domain_audit("export", batch_id, "system")
    cleanup_result = _cleanup.domain_cleanup(f"CLEANUP-{batch_id}", cleanup_module, 30)

    return {
        "chain": "delivery",
        "steps": {
            "store_result": store_result,
            "export": export_result,
            "audit": audit_result,
            "cleanup": cleanup_result,
        },
    }