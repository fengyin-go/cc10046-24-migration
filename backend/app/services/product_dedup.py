"""商品批次去重键 V1 -> V2 迁移的状态服务。

该服务不依赖通用 ``store`` 中的演示数据：迁移批次必须持有真实原始行或
明确的旧汇总证据。所有计数都由去重引擎从真实行重算，旧汇总只作为不可变
留档，不能反推出商品记录。
"""
from __future__ import annotations

import copy
import threading
from datetime import datetime, timezone
from typing import Any

from app.services.dedup_engine import DedupEngineService

PENDING_BACKFILL = "待回填"
PENDING_PUBLISH = "待发布"
PUBLISHED = "已发布"
RECEIPT_ARCHIVED = "已留档"
RECEIPT_REOPENED = "已重开"

DEFAULT_KEY_FIELDS = ["商品组", "地区"]
V2_SOURCE = "real_raw_rows"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _first(payload: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in payload and payload[key] is not None:
            return payload[key]
    return default


def normalize_counts(data: Any) -> dict[str, int] | None:
    """把各种命名的三计数统一为 original/deduped/removed。"""
    if data is None:
        return None
    if isinstance(data, list | tuple) and len(data) >= 3:
        return {
            "original_count": int(data[0]),
            "deduped_count": int(data[1]),
            "removed_count": int(data[2]),
        }
    if not isinstance(data, dict):
        return None

    source = data
    nested = data.get("counts")
    if isinstance(nested, dict):
        # 外层可能只有版本信息；计数优先取显式别名，否则读 nested。
        merged = {**nested, **{k: v for k, v in data.items() if k != "counts"}}
        source = merged

    original = _first(
        source,
        "original_count", "total_count", "input_count", "before_count",
        "original", "total", "before", "去重前", "去重前计数", "原始行数", "总数",
    )
    retained = _first(
        source,
        "deduped_count", "retained_count", "kept_count", "keep_count",
        "deduped", "retained", "kept", "保留", "保留数", "保留计数", "保留行数",
        "有效行数",
    )
    removed = _first(
        source,
        "removed_count", "duplicate_count", "deleted_count",
        "removed", "duplicates", "去除", "去除数", "去除计数", "删除数", "重复数",
    )
    if original is None or retained is None or removed is None:
        return None
    return {
        "original_count": int(original),
        "deduped_count": int(retained),
        "removed_count": int(removed),
    }


def counts_view(counts: dict[str, int] | None) -> dict[str, Any] | None:
    if counts is None:
        return None
    return {
        **counts,
        "total_count": counts["original_count"],
        "before_count": counts["original_count"],
        "input_count": counts["original_count"],
        "retained_count": counts["deduped_count"],
        "kept_count": counts["deduped_count"],
        "after_count": counts["deduped_count"],
        "total": counts["original_count"],
        "before": counts["original_count"],
        "original": counts["original_count"],
        "retained": counts["deduped_count"],
        "kept": counts["deduped_count"],
        "after": counts["deduped_count"],
        "removed": counts["removed_count"],
        "去重前计数": counts["original_count"],
        "原始行数": counts["original_count"],
        "保留计数": counts["deduped_count"],
        "保留行数": counts["deduped_count"],
        "去除计数": counts["removed_count"],
        "去除行数": counts["removed_count"],
    }


def extract_rows(payload: Any) -> list[dict[str, Any]] | None:
    """从常见请求字段中提取真实原始行。"""
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        rows = _first(
            payload,
            "raw_rows", "rows", "items", "records", "products",
            "product_rows", "source_rows", "原始行", "原始数据", "商品行", "商品列表",
        )
        if rows is None and isinstance(payload.get("values"), list):
            rows = payload["values"]
    else:
        rows = None

    if rows is None:
        return None
    if not isinstance(rows, list):
        raise ValueError("原始行必须是数组")
    normalized: list[dict[str, Any]] = []
    for index, row in enumerate(rows, start=1):
        if isinstance(row, dict):
            normalized.append(copy.deepcopy(row))
        else:
            normalized.append({"row_index": index, "value": row})
    return normalized


def normalize_key_fields(payload: dict[str, Any] | None = None) -> list[str]:
    if not payload:
        return list(DEFAULT_KEY_FIELDS)
    fields = _first(
        payload,
        "key_fields", "business_keys", "key_columns", "keyFields",
        "businessKeyFields", "业务键列", "业务键字段", "键字段",
        default=DEFAULT_KEY_FIELDS,
    )
    if isinstance(fields, str):
        fields = [part.strip() for part in fields.replace("，", ",").replace("、", ",").split(",") if part.strip()]
    if not isinstance(fields, list) or not fields:
        return list(DEFAULT_KEY_FIELDS)
    return [str(field) for field in fields]


class EvidenceMissingError(ValueError):
    """缺少 V2 重算所需的真实原始行。"""

    def __init__(self, batch_id: str, reason: str, missing: list[str] | None = None):
        super().__init__(reason)
        self.batch_id = batch_id
        self.reason = reason
        self.missing_evidence = missing or ["raw_rows", "真实原始行"]


class ProductDedupMigrationService:
    MODULE = "product_dedup"
    ENTITY = "商品去重批次"
    LIST_FIELDS = ["batch_id", "name", "party", "status", "delivery_scope"]

    def __init__(self, *, seed: bool = False) -> None:
        self._lock = threading.RLock()
        self.dedup = DedupEngineService()
        self.batches: dict[str, dict[str, Any]] = {}
        self.receipts: dict[str, dict[str, Any]] = {}
        self.publications: dict[str, dict[str, Any]] = {}
        self.aliases: dict[str, str] = {}
        self._seq = 0
        if seed:
            self.seed_scenario()

    # ---------- 基础工具 ----------

    def _next_id(self, prefix: str) -> str:
        self._seq += 1
        return f"{prefix}-{self._seq:04d}"

    @staticmethod
    def _alias_key(value: Any) -> str:
        return str(value).strip().casefold()

    def _add_alias(self, batch_id: str, *aliases: Any) -> None:
        for alias in aliases:
            if alias is None or str(alias).strip() == "":
                continue
            self.aliases[self._alias_key(alias)] = batch_id

    def resolve_batch_id(self, batch_id: Any) -> str | None:
        if batch_id is None:
            return None
        bid = str(batch_id)
        if bid in self.batches:
            return bid
        mapped = self.aliases.get(self._alias_key(bid))
        if mapped and mapped in self.batches:
            return mapped
        try:
            numeric_id = int(batch_id)
        except (TypeError, ValueError):
            numeric_id = None
        if numeric_id is not None:
            for actual_id, batch in self.batches.items():
                if int(batch.get("id", -1)) == numeric_id:
                    return actual_id
        return None

    def get_batch(self, batch_id: Any) -> dict[str, Any]:
        bid = self.resolve_batch_id(batch_id)
        if bid is None:
            raise KeyError(f"商品去重批次 {batch_id} 不存在")
        return self.batches[bid]

    def _publication_counts(self, rows: list[dict[str, Any]], key_fields: list[str], version: int) -> dict[str, Any]:
        if version == 2:
            result = self.dedup.domain_dedup_v2(rows, key_fields)
        else:
            result = self.dedup.domain_legacy_dedup(rows, key_fields)
        return result

    def _build_v2_publication(
        self,
        batch: dict[str, Any],
        rows: list[dict[str, Any]],
        *,
        status: str = PENDING_PUBLISH,
        published: bool = False,
        passed: bool = False,
    ) -> dict[str, Any]:
        from app.compute import product_dedup_key

        key_fields = batch["key_fields"]
        result = self.dedup.domain_dedup_v2(rows, key_fields)
        keys = result.get("keys") or [product_dedup_key(row, key_fields, 2) for row in rows]
        import json
        json_keys = [
            "v2:" + json.dumps([str(row.get(field, "")) for field in key_fields], ensure_ascii=False, separators=(",", ":"))
            for row in rows
        ]
        retained = result["retained_items"]
        removed = result.get("removed_items", [])

        key_index: dict[str, list[dict[str, Any]]] = {}
        retained_refs: list[dict[str, Any]] = []
        for row, key, json_key in zip(rows, keys, json_keys):
            # hash_dedup 保留首次出现；这里按同一规则构造索引。
            if key not in key_index:
                ref = {
                    "key": key,
                    "json_key": json_key,
                    "version": 2,
                    "row_id": row.get("row_id") or row.get("商品ID") or row.get("id"),
                    "product_id": row.get("商品ID") or row.get("product_id") or row.get("id"),
                    "product": row,
                    "row": row,
                }
                key_index[key] = [ref]
                key_index[json_key] = [ref]
                retained_refs.append(ref)
            else:
                duplicate_ref = {
                    "key": key,
                    "json_key": json_key,
                    "version": 2,
                    "row_id": row.get("row_id") or row.get("商品ID") or row.get("id"),
                    "product_id": row.get("商品ID") or row.get("product_id") or row.get("id"),
                    "product": row,
                    "row": row,
                    "duplicate": True,
                }
                key_index[key].append(duplicate_ref)
                key_index.setdefault(json_key, []).append(duplicate_ref)

        pid = batch.get("publication_id") or f"PUB-{batch['batch_id']}-V2"
        if pid in self.publications:
            pid = self._next_id(pid)
        counts = normalize_counts(result)
        publication = {
            "publication_id": pid,
            "id": pid,
            "batch_id": batch["batch_id"],
            "version": 2,
            "dedup_version": 2,
            "key_version": "v2",
            "schema": "product-dedup-v2",
            "source": V2_SOURCE,
            "source_is_real_raw_rows": True,
            "status": PUBLISHED if published else status,
            "published": published,
            "passed": passed or published,
            "raw_row_count": len(rows),
            "counts": counts_view(counts),
            **(counts or {}),
            "retained_count": (counts or {}).get("deduped_count"),
            "retained_rows": retained,
            "retained_products": retained,
            "products": retained,
            "items": retained,
            "deduped_items": retained,
            "removed_rows": removed,
            "key_index": key_index,
            "created_at": _now(),
            "published_at": _now() if published else None,
        }
        self.publications[pid] = publication
        batch["draft_publication_id"] = pid
        batch["v2_counts"] = None if published is False else counts_view(counts)
        return publication

    # ---------- 批次和旧回执 ----------

    def create_batch(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        if isinstance(payload.get("values"), dict):
            wrapped = payload.pop("values")
            payload = {**wrapped, **payload}
        with self._lock:
            requested_id = _first(
                payload, "batch_id", "batchId", "id", "code",
                "批次标识", "批次ID", "批次编号",
            )
            bid = self.resolve_batch_id(requested_id)
            if bid is not None:
                return self.update_batch(bid, payload)

            if requested_id is None:
                bid = self._next_id("BATCH-PD")
            else:
                bid = str(requested_id)
                if bid in self.batches:
                    return self.update_batch(bid, payload)

            self._seq += 1
            key_fields = normalize_key_fields(payload)
            name = _first(payload, "name", "batch_name", "批次名称", default=bid)
            party = _first(payload, "party", "owner", "source_party", "holder", "持有方", "来源方")
            aliases = _first(payload, "aliases", "alias", "别名", default=[]) or []
            if isinstance(aliases, str):
                aliases = [aliases]

            scope = _first(payload, "delivery_scope", "scope", "交付范围", default="SCOPE-DEFAULT")
            batch = {
                "batch_id": bid,
                "id": self._seq,
                "code": bid,
                "name": name,
                "party": party,
                "delivery_scope": scope,
                "key_fields": key_fields,
                "status": PENDING_BACKFILL,
                "raw_rows": None,
                "raw_rows_available": False,
                "raw_row_count": 0,
                "old_summary": None,
                "old_counts": None,
                "old_receipt_id": None,
                "receipt_ids": [],
                "publication_id": None,
                "draft_publication_id": None,
                "published_version": None,
                "adopted_version": None,
                "read_version": None,
                "new_counts_available": False,
                "new_counts": None,
                "v2_counts": None,
                "pending_reason": None,
                "missing_evidence": ["raw_rows", "真实原始行"],
                "events": [],
                "created_at": _now(),
                "updated_at": _now(),
            }
            self.batches[bid] = batch
            self._add_alias(bid, bid, name, party, *aliases)
            batch["events"].append({"at": _now(), "event": "batch_registered"})

            old_summary = _first(payload, "old_summary", "legacy_summary", "旧汇总", "旧版汇总")
            old_counts = normalize_counts(
                _first(payload, "old_counts", "legacy_counts", "counts", "三计数", "旧计数")
                or old_summary
            )
            if old_summary is not None:
                batch["old_summary"] = copy.deepcopy(old_summary) if isinstance(old_summary, dict) else {"text": old_summary}
            if old_counts is not None:
                self.archive_old_counts(
                    bid,
                    old_counts,
                    receipt_id=_first(payload, "receipt_id", "old_receipt_id", "回执编号"),
                    source="old_summary",
                    summary=batch["old_summary"],
                )

            rows = extract_rows(payload)
            if rows is not None:
                self.attach_raw_rows(bid, rows, reopen_receipt=False)

            requested_status = _first(payload, "status", "initial_status", "状态")
            should_publish = bool(
                payload.get("publish") or payload.get("auto_publish")
                or payload.get("published") or requested_status == PUBLISHED
            )
            if should_publish and rows is not None:
                self.publish_v2(bid, payload)
            elif rows is not None and requested_status:
                batch["status"] = str(requested_status)
                if str(requested_status) == PENDING_PUBLISH:
                    batch["pending_reason"] = _first(
                        payload, "pending_reason", "reason", "待发布原因",
                        default="版本 2 商品去重结果尚待发布/校验，数据处理不能读取草稿。",
                    )
            else:
                self._mark_backfill_required(batch)

            return self.batch_detail(bid)

    def update_batch(self, batch_id: Any, payload: dict[str, Any]) -> dict[str, Any]:
        batch = self.get_batch(batch_id)
        with self._lock:
            for source, target in [
                ("name", "name"), ("batch_name", "name"), ("批次名称", "name"),
                ("party", "party"), ("owner", "party"), ("持有方", "party"),
                ("delivery_scope", "delivery_scope"), ("scope", "delivery_scope"),
            ]:
                if source in payload:
                    batch[target] = payload[source]
            if "key_fields" in payload or "业务键列" in payload:
                batch["key_fields"] = normalize_key_fields(payload)
            rows = extract_rows(payload)
            if rows is not None:
                self.attach_raw_rows(batch["batch_id"], rows, reopen_receipt=bool(payload.get("reopen", False)))
            if payload.get("publish") or payload.get("auto_publish"):
                self.publish_v2(batch["batch_id"], payload)
            return self.batch_detail(batch["batch_id"])

    def _mark_backfill_required(self, batch: dict[str, Any]) -> None:
        batch["status"] = PENDING_BACKFILL
        batch["new_counts_available"] = False
        batch["new_counts"] = None
        batch["v2_counts"] = None
        batch["publication_id"] = None
        batch["published_version"] = None
        batch["adopted_version"] = None
        batch["missing_evidence"] = ["raw_rows", "真实原始行"]
        batch["pending_reason"] = "仅有旧汇总，缺少可用于版本 2 重算的真实原始行；不能用旧行数猜测或反推商品。"

    def archive_old_counts(
        self,
        batch_id: Any,
        counts: dict[str, int],
        *,
        receipt_id: str | None = None,
        source: str = "legacy_recalculation",
        summary: Any = None,
    ) -> dict[str, Any]:
        batch = self.get_batch(batch_id)
        bid = batch["batch_id"]
        with self._lock:
            # 最早回执和计数一旦写入即冻结，后续续做不得覆盖。
            if batch.get("old_receipt_id") and batch["old_receipt_id"] in self.receipts:
                return self.receipts[batch["old_receipt_id"]]

            rid = str(receipt_id or f"RCPT-{bid}-OLD")
            if rid in self.receipts:
                rid = f"{rid}-{self._next_id('RCPT')}"
            frozen = dict(counts)
            frozen_view = counts_view(frozen)
            receipt = {
                "receipt_id": rid,
                "id": rid,
                "batch_id": bid,
                "type": "legacy_dedup_receipt",
                "version": 1,
                "dedup_version": 1,
                "key_version": "v1",
                "key_format": "legacy_concat_pipe",
                "counts": frozen_view,
                **frozen_view,
                "archived_counts": frozen_view,
                "old_counts": frozen_view,
                "source": source,
                "old_summary": copy.deepcopy(summary) if isinstance(summary, dict) else summary,
                "status": RECEIPT_ARCHIVED,
                "status_code": "archived",
                "archived": True,
                "immutable": True,
                "reopened": False,
                "reopen_count": 0,
                "created_at": _now(),
                "reopened_at": None,
            }
            self.receipts[rid] = receipt
            batch["old_receipt_id"] = rid
            batch["old_counts"] = counts_view(frozen)
            batch["receipt_ids"].append(rid)
            batch["events"].append({"at": _now(), "event": "old_counts_archived", "receipt_id": rid})
            return receipt

    def reopen_receipt(self, receipt_id: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = payload or {}
        with self._lock:
            receipt = self.receipts.get(str(receipt_id))
            if receipt is None:
                # 允许传批次 ID，重开该批次最早的旧回执。
                batch = self.get_batch(receipt_id)
                receipt = self.receipts.get(batch["old_receipt_id"] or "")
                if receipt is None:
                    raise KeyError(f"旧回执 {receipt_id} 不存在")

            if not receipt.get("reopened"):
                receipt["original_status"] = receipt.get("status", RECEIPT_ARCHIVED)
                receipt["status"] = RECEIPT_REOPENED
                receipt["status_code"] = "reopened"
                receipt["reopened"] = True
                receipt["reopen_count"] = int(receipt.get("reopen_count", 0)) + 1
                receipt["reopened_at"] = _now()
                receipt["reopen_operator"] = payload.get("operator") or payload.get("operator_name")
                batch = self.batches[receipt["batch_id"]]
                batch["events"].append({
                    "at": _now(), "event": "old_receipt_reopened",
                    "receipt_id": receipt["receipt_id"],
                })
            return self.get_receipt(receipt["receipt_id"])

    def get_receipt(self, receipt_id: str) -> dict[str, Any]:
        receipt = self.receipts.get(str(receipt_id))
        if receipt is None:
            raise KeyError(f"旧回执 {receipt_id} 不存在")
        result = copy.deepcopy(receipt)
        batch = self.batches.get(receipt["batch_id"])
        if batch and batch.get("raw_rows_available"):
            legacy = self.run_legacy_dedup(batch["batch_id"], save_receipt=False)
            result["full_result_available"] = True
            result["full_result"] = {
                "legacy_dedup": legacy,
                "v2": self.data_processing(batch["batch_id"], version=2),
                "batch_detail_href": f"/api/product-dedup/batches/{batch['batch_id']}",
            }
            archived = normalize_counts(receipt.get("archived_counts") or receipt)
            current = normalize_counts(legacy)
            result["matches_archived_counts"] = archived == current
        else:
            result["full_result_available"] = False
            result["missing_evidence"] = ["raw_rows", "真实原始行"]
        return result

    # ---------- 原始行、旧版计算、V2 发布 ----------

    def attach_raw_rows(
        self,
        batch_id: Any,
        rows: list[dict[str, Any]],
        *,
        reopen_receipt: bool = False,
        mode: str | None = None,
    ) -> dict[str, Any]:
        batch = self.get_batch(batch_id)
        bid = batch["batch_id"]
        with self._lock:
            if batch.get("raw_rows_available") and batch.get("publication_id"):
                # 已通过/已发布批次不得用补交动作改写已发布数据。
                return self.batch_detail(bid)

            if mode == "append":
                batch.setdefault("raw_rows", []).extend(copy.deepcopy(rows))
            else:
                # 默认按“补交缺失证据”处理；重复调用不会把同一批行追加两次。
                batch["raw_rows"] = copy.deepcopy(rows)

            batch["raw_rows_available"] = True
            batch["raw_row_count"] = len(batch["raw_rows"])
            batch["missing_evidence"] = []
            batch["status"] = PENDING_PUBLISH
            batch["pending_reason"] = "真实原始行已补齐；版本 2 结果已重算，等待续做/发布通过。"
            batch["updated_at"] = _now()
            batch["events"].append({"at": _now(), "event": "raw_rows_attached", "count": len(rows)})

            publication = self._build_v2_publication(batch, batch["raw_rows"])
            batch["draft_publication_id"] = publication["publication_id"]
            batch["new_counts_available"] = False
            batch["new_counts"] = None
            batch["publication_id"] = None
            batch["published_version"] = None
            batch["adopted_version"] = None

            if reopen_receipt and batch.get("old_receipt_id"):
                self.reopen_receipt(batch["old_receipt_id"], {})
            return self.batch_detail(bid)

    def backfill_raw_rows(self, batch_id: Any, payload: Any) -> dict[str, Any]:
        data = payload if isinstance(payload, dict) else {"rows": payload}
        rows = extract_rows(payload)
        if rows is None and isinstance(data.get("values"), dict):
            rows = extract_rows(data["values"])
        if rows is None and isinstance(data.get("data"), dict):
            rows = extract_rows(data["data"])
        if rows is None:
            raise EvidenceMissingError(
                str(batch_id),
                "补交内容中没有 raw_rows/rows/原始行，不能用旧汇总行数代替真实商品行。",
            )
        option_source = data
        if isinstance(data.get("values"), dict):
            option_source = {**data["values"], **data}
        mode = _first(option_source, "mode", "write_mode", default=None)
        self.attach_raw_rows(
            batch_id,
            rows,
            # 补交证据本身只负责回填；“续做”时再按流程重开旧回执。
            reopen_receipt=bool(_first(option_source, "reopen", "reopen_receipt", default=False)),
            mode=mode,
        )
        if _first(option_source, "publish", "auto_publish", "continue_migration", default=False):
            self.publish_v2(batch_id, option_source)
        return self.batch_detail(batch_id)

    def run_legacy_dedup(
        self,
        batch_id: Any = None,
        payload: dict[str, Any] | None = None,
        *,
        rows: list[dict[str, Any]] | None = None,
        save_receipt: bool | None = None,
    ) -> dict[str, Any]:
        payload = payload or {}
        if rows is None:
            rows = extract_rows(payload)

        batch = None
        if batch_id is not None or payload.get("batch_id") is not None:
            batch = self.get_batch(batch_id or payload.get("batch_id"))
            key_fields = normalize_key_fields({**payload, **({"key_fields": batch["key_fields"]} if batch else {})})
            if rows is None and batch.get("raw_rows_available"):
                rows = batch["raw_rows"]
        else:
            key_fields = normalize_key_fields(payload)

        if rows is None:
            if batch and batch.get("old_counts"):
                archived = copy.deepcopy(batch["old_counts"])
                return {
                    "ok": True,
                    "available": True,
                    "recalculable": False,
                    "source": "archived_old_summary",
                    "version": 1,
                    "key_version": "v1",
                    "batch_id": batch["batch_id"],
                    "receipt_id": batch.get("old_receipt_id"),
                    "steps": {"dedup": {
                        "original_count": archived["original_count"],
                        "deduped_count": archived["deduped_count"],
                        "removed_count": archived["removed_count"],
                    }},
                    **archived,
                }
            raise EvidenceMissingError(str(batch_id), "没有可运行旧版去重的原始行，也没有旧汇总留档。")

        result = self.dedup.domain_legacy_dedup(rows, key_fields)
        three = {
            "original_count": result["original_count"],
            "deduped_count": result["deduped_count"],
            "removed_count": result["removed_count"],
        }
        receipt = None
        should_save = save_receipt
        if should_save is None:
            should_save = bool(batch is not None and _first(payload, "save_receipt", "persist", default=True))
        if batch is not None and should_save:
            if not batch.get("old_receipt_id"):
                receipt = self.archive_old_counts(
                    batch["batch_id"], three,
                    receipt_id=_first(payload, "receipt_id", "old_receipt_id"),
                    source="legacy_recalculation",
                )
            else:
                receipt = self.receipts[batch["old_receipt_id"]]
        response = {
            "ok": True,
            "available": True,
            "recalculable": True,
            "source": "legacy_engine_recalculation",
            "version": 1,
            "dedup_version": 1,
            "key_version": "v1",
            "key_fields": key_fields,
            "batch_id": batch["batch_id"] if batch else _first(payload, "batch_id"),
            "receipt_id": (receipt or {}).get("receipt_id"),
            "archived_receipt_unchanged": bool(receipt and receipt.get("archived")),
            "steps": {"dedup": dict(three)},
            **three,
            "deduped_items": result.get("deduped_items", []),
        }
        if receipt:
            archived = normalize_counts(receipt.get("archived_counts") or receipt)
            response["archived_counts"] = counts_view(archived)
            response["matches_archived"] = archived == three
        elif batch is not None and batch.get("old_receipt_id"):
            archived_receipt = self.receipts.get(batch["old_receipt_id"])
            if archived_receipt:
                archived = normalize_counts(archived_receipt.get("archived_counts") or archived_receipt)
                response["receipt_id"] = archived_receipt.get("receipt_id")
                response["archived_receipt_unchanged"] = True
                response["archived_counts"] = counts_view(archived)
                response["matches_archived"] = archived == three
        return response

    def publish_v2(self, batch_id: Any, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = payload or {}
        batch = self.get_batch(batch_id)
        bid = batch["batch_id"]
        with self._lock:
            existing_id = batch.get("publication_id")
            if existing_id and existing_id in self.publications:
                existing = copy.deepcopy(self.publications[existing_id])
                existing.update({
                    "already_published": True,
                    "idempotent": True,
                    "republished": False,
                    "message": "该批版本 2 数据已经发布/通过，未重复发布。",
                })
                return existing

            if not batch.get("raw_rows_available") or batch.get("raw_rows") is None:
                batch["status"] = PENDING_BACKFILL
                self._mark_backfill_required(batch)
                raise EvidenceMissingError(
                    bid,
                    "无法发布版本 2：缺少真实原始行；旧汇总不能用于重算商品去重数据。",
                )

            should_pass = not (payload.get("pass") is False or payload.get("passed") is False or payload.get("approve") is False)
            publication = self._build_v2_publication(
                batch, batch["raw_rows"],
                status=PUBLISHED if should_pass else PENDING_PUBLISH,
                published=should_pass,
                passed=should_pass,
            )
            if not should_pass:
                batch["status"] = PENDING_PUBLISH
                batch["pending_reason"] = _first(
                    payload, "pending_reason", "reason", "待发布原因",
                    default="版本 2 重算已完成，但发布校验/审批尚未通过。",
                )
                return publication

            self.publications[publication["publication_id"]] = publication
            batch["publication_id"] = publication["publication_id"]
            batch["published_version"] = 2
            batch["adopted_version"] = 2
            batch["status"] = PUBLISHED
            batch["pending_reason"] = None
            batch["new_counts_available"] = True
            batch["new_counts"] = publication["counts"]
            batch["v2_counts"] = publication["counts"]
            batch["updated_at"] = _now()
            batch["events"].append({
                "at": _now(), "event": "v2_published",
                "publication_id": publication["publication_id"],
            })
            publication["already_published"] = False
            publication["idempotent"] = False
            return copy.deepcopy(publication)

    def migrate_batch(self, batch_id: Any, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        """执行一次迁移检查/重算，但不把待发布草稿冒充为已发布数据。"""
        payload = payload or {}
        batch = self.get_batch(batch_id)
        bid = batch["batch_id"]
        with self._lock:
            if batch.get("publication_id") and batch["publication_id"] in self.publications:
                publication = self.publications[batch["publication_id"]]
                if publication.get("published"):
                    result = self.data_processing(bid, 2)
                    result["migration_action"] = "skipped_already_published"
                    result["idempotent"] = True
                    return result

            if not batch.get("raw_rows_available"):
                self._mark_backfill_required(batch)
                return self.data_processing(bid, 2)

            self._build_v2_publication(batch, batch["raw_rows"])
            batch["status"] = PENDING_PUBLISH
            batch["pending_reason"] = _first(
                payload, "pending_reason", "reason", "待发布原因",
                default="版本 2 已仅用真实原始行重算，发布/校验尚未通过。",
            )
            if payload.get("publish") or payload.get("auto_publish"):
                return self.publish_v2(bid, payload)
            result = self.data_processing(bid, 2)
            result["migration_action"] = "recomputed_waiting_for_publish"
            return result

    def continue_migration(self, batch_id: Any, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        """补交后续做：重算 V2、发布通过并重开旧回执。"""
        payload = payload or {}
        if isinstance(payload.get("values"), dict):
            payload = {**payload["values"], **{k: v for k, v in payload.items() if k != "values"}}
        batch = self.get_batch(batch_id)
        rows = extract_rows(payload)
        if rows is not None:
            self.backfill_raw_rows(batch_id, {**payload, "reopen": False})
        receipt = None
        if batch.get("old_receipt_id") and payload.get("reopen", True):
            receipt = self.reopen_receipt(batch["old_receipt_id"], payload)
        publication = self.publish_v2(batch_id, payload)
        detail = self.batch_detail(batch_id)
        detail["continued"] = True
        detail["publication"] = publication
        if receipt:
            detail["reopened_receipt"] = self.get_receipt(receipt["receipt_id"])
        return detail

    # ---------- 查询/数据处理 ----------

    def data_processing(
        self,
        batch_id: Any,
        version: int | str = 2,
        publication_id: str | None = None,
    ) -> dict[str, Any]:
        batch = self.get_batch(batch_id)
        bid = batch["batch_id"]
        if str(version).lower() in {"1", "old", "legacy", "v1"}:
            return self.run_legacy_dedup(bid, save_receipt=False)

        pub_id = publication_id or batch.get("publication_id")
        publication = self.publications.get(pub_id or "") if pub_id else None
        if publication_id and publication is not None and publication.get("batch_id") != bid:
            publication = None
        if publication is None or not publication.get("published"):
            reason = batch.get("pending_reason")
            if not batch.get("raw_rows_available"):
                self._mark_backfill_required(batch)
                reason = batch["pending_reason"]
            else:
                batch["status"] = PENDING_PUBLISH
                reason = reason or "版本 2 商品去重数据尚未发布/通过，数据处理不能读取草稿。"
            return {
                "ok": False,
                "available": False,
                "chain": "data-process-v2",
                "interface": "new_v2",
                "requested_version": 2,
                "version": 2,
                "read_version": None,
                "read_version_label": "unavailable",
                "本次读到的版本": None,
                "adopted_version": batch.get("adopted_version"),
                "batch_id": bid,
                "status": batch.get("status"),
                "pending": True,
                "pending_reason": reason,
                "reason": reason,
                "missing_evidence": batch.get("missing_evidence", []),
                "draft_publication_id": batch.get("draft_publication_id"),
                "steps": {"dedup": None},
                "counts": None,
            }

        counts = normalize_counts(publication)
        counts_alias = counts_view(counts)
        retained = publication.get("retained_rows", [])
        detail_matches = counts is not None and counts["deduped_count"] == len(retained)
        result = {
            "ok": True,
            "available": True,
            "chain": "data-process-v2",
            "interface": "new_v2",
            "requested_version": 2,
            "version": 2,
            "dedup_version": 2,
            "key_version": "v2",
            "read_version": 2,
            "read_version_label": "v2",
            "本次读到的版本": 2,
            "adopted_version": 2,
            "batch_id": bid,
            "publication_id": publication["publication_id"],
            "source": publication.get("source"),
            "read_source": "actually_published_v2_product_dedup_data",
            "same_published_batch": True,
            "retained_detail_matches_count": detail_matches,
            "steps": {
                "dedup": {
                    "original_count": counts["original_count"],
                    "deduped_count": counts["deduped_count"],
                    "removed_count": counts["removed_count"],
                    "before_count": counts["original_count"],
                    "retained_count": counts["deduped_count"],
                    "kept_count": counts["deduped_count"],
                    "去重前计数": counts["original_count"],
                    "保留计数": counts["deduped_count"],
                    "去除计数": counts["removed_count"],
                    "version": 2,
                    "dedup_version": 2,
                    "read_version": 2,
                    "publication_id": publication["publication_id"],
                }
            },
            "counts": counts_alias,
            **(counts_alias or {}),
        }
        batch["read_version"] = 2
        batch["events"].append({"at": _now(), "event": "data_processing_read_v2", "publication_id": pub_id})
        return result

    def batch_detail(self, batch_id: Any) -> dict[str, Any]:
        batch = self.get_batch(batch_id)
        with self._lock:
            result = copy.deepcopy(batch)
            result["backfill_status"] = "已回填" if batch.get("raw_rows_available") else PENDING_BACKFILL
            result["回填状态"] = result["backfill_status"]
            result["migration_status"] = batch.get("status")
            result["待发布原因"] = batch.get("pending_reason")
            result["reason"] = batch.get("pending_reason")
            result["缺少证据"] = batch.get("missing_evidence", [])
            pub_id = batch.get("publication_id")
            publication = self.publications.get(pub_id or "")
            if publication:
                result["publication"] = copy.deepcopy(publication)
                result["active_publication"] = copy.deepcopy(publication)
                result["new_counts"] = copy.deepcopy(publication.get("counts"))
                result["v2_counts"] = copy.deepcopy(publication.get("counts"))
                result["new_counts_available"] = True
                result["retained_rows"] = copy.deepcopy(publication.get("retained_rows", []))
                result["retained_products"] = copy.deepcopy(publication.get("retained_products", []))
                result["products"] = copy.deepcopy(publication.get("products", []))
                result["items"] = copy.deepcopy(publication.get("items", []))
                result["deduped_items"] = copy.deepcopy(publication.get("deduped_items", []))
                result["actual_retained_products"] = copy.deepcopy(publication.get("retained_products", []))
                result["retained_count_matches_detail"] = (
                    publication.get("counts", {}).get("deduped_count") == len(publication.get("retained_rows", []))
                )
            else:
                draft_id = batch.get("draft_publication_id")
                draft = self.publications.get(draft_id or "")
                result["publication"] = None
                result["active_publication"] = None
                result["new_counts"] = None
                result["v2_counts"] = None
                result["new_counts_available"] = False
                result["retained_rows"] = []
                result["retained_products"] = []
                result["products"] = []
                result["items"] = []
                result["deduped_items"] = []
                result["actual_retained_products"] = []
                result["retained_count_matches_detail"] = False
                if draft:
                    result["draft_publication"] = {
                        "publication_id": draft["publication_id"],
                        "status": draft["status"],
                        "published": False,
                        "counts": draft.get("counts"),
                        "pending_reason": batch.get("pending_reason"),
                    }

            receipt = self.receipts.get(batch.get("old_receipt_id") or "")
            result["old_receipt"] = self.get_receipt(receipt["receipt_id"]) if receipt else None
            result["old_counts_archive"] = copy.deepcopy(batch.get("old_counts"))
            result["readiness"] = self.readiness(batch["batch_id"])
            # 同时提供常见的 batch/entry/data 包装名，顶层字段保持不变。
            wrapper = copy.deepcopy(result)
            result["batch"] = wrapper
            result["entry"] = wrapper
            result["data"] = wrapper
            return result

    def readiness(self, batch_id: Any) -> dict[str, Any]:
        batch = self.get_batch(batch_id)
        raw_ready = bool(batch.get("raw_rows_available"))
        pub_id = batch.get("publication_id")
        publication = self.publications.get(pub_id or "") if pub_id else None
        published = bool(publication and publication.get("published"))
        return {
            "batch_id": batch["batch_id"],
            "party": batch.get("party"),
            "status": batch.get("status"),
            "new_version_ready": published,
            "usable": published,
            "adopted_version": 2 if published else None,
            "raw_rows_available": raw_ready,
            "old_summary_available": bool(batch.get("old_summary") or batch.get("old_counts")),
            "v2_published": published,
            "already_passed": published and bool(publication and publication.get("passed")),
            "missing_evidence": [] if raw_ready else batch.get("missing_evidence", ["raw_rows"]),
            "缺少证据": [] if raw_ready else batch.get("missing_evidence", ["raw_rows"]),
            "pending_reason": None if published else batch.get("pending_reason"),
            "reason": None if published else batch.get("pending_reason"),
            "待发布原因": None if published else batch.get("pending_reason"),
            "checks": {
                "has_real_raw_rows": raw_ready,
                "v2_recomputed_from_raw": bool(batch.get("draft_publication_id") or published),
                "v2_actually_published": published,
                "old_counts_archived": bool(batch.get("old_receipt_id")),
            },
        }

    def claim_new_version(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = payload or {}
        ids = _first(payload, "batch_ids", "ids", "batches", default=None)
        scope = _first(payload, "delivery_scope", "scope", "交付范围", default=None)
        with self._lock:
            if ids:
                target_ids = [self.get_batch(x)["batch_id"] for x in ids]
            else:
                target_ids = [
                    bid for bid, batch in self.batches.items()
                    if scope is None or batch.get("delivery_scope") == scope
                ]
            items = [self.readiness(bid) for bid in target_ids]
            return {
                "ok": True,
                "claimed": True,
                "operator": payload.get("operator") or payload.get("operator_name"),
                "delivery_scope": scope or "ALL",
                "claimed_at": _now(),
                "total": len(items),
                "ready_count": sum(1 for x in items if x["new_version_ready"]),
                "blocked_count": sum(1 for x in items if not x["new_version_ready"]),
                "items": items,
                "batches": items,
            }

    def lookup_products(self, payload: dict[str, Any] | None = None, request_query: Any = None) -> dict[str, Any]:
        payload = dict(payload or {})
        query: dict[str, Any] = {}
        if request_query is not None:
            query.update(dict(request_query))
        query.update(payload)

        batch_ref = _first(query, "batch_id", "batch", "批次", "批次标识")
        batch = None
        if batch_ref is not None:
            batch = self.get_batch(batch_ref)
            fields = normalize_key_fields({**query, "key_fields": batch["key_fields"]})
        else:
            fields = normalize_key_fields(query)

        values = query.get("values")
        if values is None and isinstance(query.get("business_key"), dict):
            bk = query["business_key"]
            values = [bk.get(field, bk.get(self._camel_alias(field), "")) for field in fields]
        elif values is None and isinstance(query.get("business_key"), list | tuple):
            values = query["business_key"]
        if isinstance(values, str):
            import json
            try:
                values = json.loads(values)
            except Exception:
                values = [values]
        if not isinstance(values, list | tuple):
            values = []
            for field in fields:
                if field == "商品组":
                    aliases = [field, "product_group", "productGroup", "group", "group_name", "商品组名称"]
                elif field == "地区":
                    aliases = [field, "region", "area", "region_name", "地区名称"]
                else:
                    aliases = [field, str(field).casefold(), str(field).replace(" ", "_")]
                value = _first(query, *aliases)
                values.append("" if value is None else value)

        from app.compute import canonical_dedup_key
        import json
        version = _first(query, "version", "dedup_version", "key_version", default=2)
        version = 2 if str(version).lower() in {"2", "v2"} else 1
        key = canonical_dedup_key(values, version)
        json_key = "v2:" + json.dumps([str(v) for v in values], ensure_ascii=False, separators=(",", ":"))

        include_unpublished = bool(_first(query, "include_unpublished", "include_draft", default=False))
        provided_key = _first(query, "key", "dedup_key", "business_key_string")
        candidate_keys = [key, json_key]
        if isinstance(provided_key, str) and provided_key not in candidate_keys:
            candidate_keys.append(provided_key)

        matches: list[dict[str, Any]] = []
        seen_refs: set[int] = set()
        batch_ids = [batch["batch_id"]] if batch else list(self.batches.keys())
        for bid in batch_ids:
            b = self.batches[bid]
            pub = self.publications.get(b.get("publication_id") or "")
            if pub is None and include_unpublished and b.get("draft_publication_id"):
                pub = self.publications.get(b["draft_publication_id"])
            if pub is None:
                continue
            key_index = pub.get("key_index", {})
            for candidate in candidate_keys:
                for ref in key_index.get(candidate, []):
                    if version == 2 and not pub.get("published") and not include_unpublished:
                        continue
                    marker = id(ref)
                    if marker in seen_refs:
                        continue
                    seen_refs.add(marker)
                    matches.append({
                        "matched": True,
                        "batch_id": bid,
                        "publication_id": pub["publication_id"],
                        "version": version,
                        "key": key,
                        "json_key": json_key,
                        "business_key": dict(zip(fields, values)),
                        **ref,
                    })

        return {
            "matched": bool(matches),
            "count": len(matches),
            "version": version,
            "key": key,
            "json_key": json_key if version == 2 else None,
            "key_v2": json_key if version == 2 else None,
            "key_fields": fields,
            "business_key": dict(zip(fields, values)),
            "items": matches,
            "results": matches,
            "records": matches,
            "products": [m.get("product") for m in matches],
            "product": matches[0]["product"] if len(matches) == 1 else (matches[0]["product"] if matches else None),
        }

    @staticmethod
    def _camel_alias(field: str) -> str | None:
        mapping = {"商品组": "product_group", "地区": "region"}
        return mapping.get(field)

    def list_batches(self, *, status: str | None = None, scope: str | None = None) -> dict[str, Any]:
        items = []
        for bid in sorted(self.batches):
            batch = self.batches[bid]
            if status and batch.get("status") != status:
                continue
            if scope and batch.get("delivery_scope") != scope:
                continue
            readiness = self.readiness(bid)
            items.append({
                "batch_id": bid,
                "id": batch.get("id"),
                "name": batch.get("name"),
                "party": batch.get("party"),
                "status": batch.get("status"),
                "migration_status": batch.get("status"),
                "backfill_status": "已回填" if batch.get("raw_rows_available") else PENDING_BACKFILL,
                "回填状态": "已回填" if batch.get("raw_rows_available") else PENDING_BACKFILL,
                "delivery_scope": batch.get("delivery_scope"),
                "raw_rows_available": batch.get("raw_rows_available"),
                "raw_row_count": batch.get("raw_row_count"),
                "new_counts_available": batch.get("new_counts_available"),
                "new_counts": batch.get("new_counts"),
                "old_counts": batch.get("old_counts"),
                "old_receipt_id": batch.get("old_receipt_id"),
                "publication_id": batch.get("publication_id"),
                "pending_reason": batch.get("pending_reason"),
                "missing_evidence": batch.get("missing_evidence"),
                "readiness": readiness,
            })
        return {"items": items, "total": len(items), "page": 1, "size": max(20, len(items))}

    # ---------- 通用资源服务适配 ----------

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        result = self.list_batches(status=status)
        rows = result["items"]
        for key, value in (filters or {}).items():
            rows = [row for row in rows if value.casefold() in str(row.get(key, "")).casefold()]
        if keyword:
            needle = keyword.casefold()
            rows = [
                row for row in rows
                if needle in str(row.get("batch_id", "")).casefold()
                or needle in str(row.get("name", "")).casefold()
                or needle in str(row.get("party", "")).casefold()
            ]
        total = len(rows)
        start = (max(page, 1) - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int | str) -> dict[str, Any] | None:
        try:
            return self.batch_detail(entry_id)
        except KeyError:
            return None

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        if not isinstance(values, dict):
            return None, ["values"]
        return self.create_batch(values), []

    def run_action(self, entry_id: int | str, action: str) -> tuple[dict[str, Any] | None, str]:
        try:
            if action in {"续做", "继续迁移", "continue"}:
                return self.continue_migration(entry_id), "已续做迁移"
            if action in {"迁移", "迁移去重键", "migrate"}:
                return self.migrate_batch(entry_id), "已执行迁移检查"
            if action in {"发布", "发布V2", "publish"}:
                return self.publish_v2(entry_id), "版本 2 已发布"
            if action in {"重开回执", "reopen"}:
                batch = self.get_batch(entry_id)
                receipt = self.reopen_receipt(batch["old_receipt_id"])
                return receipt, "旧回执已重开"
            return None, f"动作「{action}」不属于 product_dedup 可执行范围"
        except (KeyError, EvidenceMissingError) as exc:
            return None, str(exc)

    def reset(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        with self._lock:
            self.batches.clear()
            self.receipts.clear()
            self.publications.clear()
            self.aliases.clear()
            self._seq = 0
            payload = payload or {}
            if payload.get("seed", True):
                self.seed_scenario(pending_jia=bool(payload.get("pending_jia") or payload.get("jia_pending")))
            return {"ok": True, "reset": True, "seeded": bool(payload.get("seed", True)), "batches": list(self.batches)}

    # ---------- 内置甲/乙场景 ----------

    def seed_scenario(self, *, pending_jia: bool = False) -> None:
        scope = "SCOPE-PRODUCT-DEDUP-MIXED"
        jia_id = "BATCH-JIA"
        self.create_batch({
            "batch_id": jia_id,
            "id": "JIA",
            "name": "甲批次（可取回原始行）",
            "party": "甲",
            "aliases": ["JIA", "A", "BATCH-A", "甲批次", "批次甲"],
            "delivery_scope": scope,
            "key_fields": DEFAULT_KEY_FIELDS,
            "raw_rows": [
                {
                    "商品ID": "GOODS-A1",
                    "商品名称": "联名钥匙扣",
                    "商品组": "A|B",
                    "地区": "C",
                    "供应方": "甲",
                },
                {
                    "商品ID": "GOODS-A2",
                    "商品名称": "限定徽章",
                    "商品组": "A",
                    "地区": "B|C",
                    "供应方": "甲",
                },
            ],
        })
        self._add_alias(jia_id, "JIA", "A", "BATCH-A", "甲", "甲批次", "批次甲")
        self.run_legacy_dedup(jia_id, save_receipt=True, payload={"receipt_id": "RCPT-JIA-OLD"})
        if pending_jia:
            batch = self.batches[jia_id]
            batch["status"] = PENDING_PUBLISH
            batch["pending_reason"] = "甲的版本 2 重算已完成，但发布校验单尚未通过。"
        else:
            self.publish_v2(jia_id, {"publication_id": "PUB-JIA-V2"})
            # publish_v2 不接受指定 ID；若自动生成不同，补一个易读别名发布对象。
            pub_id = self.batches[jia_id]["publication_id"]
            if pub_id != "PUB-JIA-V2":
                pub = self.publications[pub_id]
                pub["publication_id"] = "PUB-JIA-V2"
                pub["id"] = "PUB-JIA-V2"
                self.publications["PUB-JIA-V2"] = pub
                del self.publications[pub_id]
                self.batches[jia_id]["publication_id"] = "PUB-JIA-V2"

        yi_id = "BATCH-YI"
        old_summary = {
            "source_system": "乙旧汇总",
            "original_count": 3,
            "deduped_count": 2,
            "removed_count": 1,
            "note": "只有旧汇总三计数，未保留原始商品行，也不知道旧拼接键合并了哪一行。",
        }
        self.create_batch({
            "batch_id": yi_id,
            "id": "YI",
            "name": "乙批次（仅有旧汇总）",
            "party": "乙",
            "aliases": ["YI", "B", "BATCH-B", "乙批次", "批次乙"],
            "delivery_scope": scope,
            "key_fields": DEFAULT_KEY_FIELDS,
            "old_summary": old_summary,
            "old_counts": old_summary,
            "receipt_id": "RCPT-YI-OLD",
            "status": PENDING_BACKFILL,
        })
        self._add_alias(yi_id, "YI", "B", "BATCH-B", "乙", "乙批次", "批次乙")


product_dedup_service = ProductDedupMigrationService(seed=True)
service = product_dedup_service

# 常见命名别名。
ProductDedupService = ProductDedupMigrationService
DedupMigrationService = ProductDedupMigrationService
