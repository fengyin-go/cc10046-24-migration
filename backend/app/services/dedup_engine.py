"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class DedupEngineService(BaseModuleService):
    MODULE = "dedup_engine"
    ENTITY = "去重策略"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["策略标识", "业务键列", "排序字段", "保留规则", "时间窗口", "相似阈值", "哈希算法", "输出目标"]
    REQUIRED_FIELDS = ["策略标识", "业务键列", "排序字段"]
    STATUS_ORDER = ["草稿", "待校验", "已启用", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "启用策略": "已启用", "停用策略": "已停用"}
    NEGATIVE_ACTIONS = ["停用策略"]

    def domain_dedup(
        self,
        items: list[dict],
        key_fields: list[str],
        version: int | str = 1,
        *,
        include_removed: bool = False,
        include_keys: bool = False,
    ) -> dict:
        """对数据行执行去重，并始终给出去重前/保留/去除三个计数。"""
        from app.compute import hash_dedup, product_dedup_key

        key_version = 2 if str(version).lower() in {"2", "v2"} else 1
        deduped, removed_count = hash_dedup(items, key_fields, key_version=key_version)
        result: dict = {
            "original_count": len(items),
            "deduped_count": len(deduped),
            "removed_count": removed_count,
            # 语义别名，方便新版商品去重数据直接对照。
            "total_count": len(items),
            "retained_count": len(deduped),
            "kept_count": len(deduped),
            "version": key_version,
            "dedup_version": key_version,
            "key_version": f"v{key_version}",
            "deduped_items": deduped,
            "retained_items": deduped,
        }
        if include_removed:
            retained_ids = {id(row) for row in deduped}
            result["removed_items"] = [row for row in items if id(row) not in retained_ids]
        if include_keys:
            result["keys"] = [product_dedup_key(row, key_fields, key_version) for row in items]
        return result

    def domain_dedup_v2(self, items: list[dict], key_fields: list[str]) -> dict:
        """版本 2 商品去重：只能对调用方提供的真实原始行重算。"""
        return self.domain_dedup(items, key_fields, version=2, include_removed=True, include_keys=True)

    def domain_legacy_dedup(self, items: list[dict], key_fields: list[str]) -> dict:
        """旧拼接键口径，字段和计数名保持历史接口不变。"""
        result = self.domain_dedup(items, key_fields, version=1)
        return {
            "original_count": result["original_count"],
            "deduped_count": result["deduped_count"],
            "removed_count": result["removed_count"],
            "deduped_items": result["deduped_items"],
        }

    def build_key_v2(self, item: dict, key_fields: list[str]) -> str:
        """从真实原始行构造 V2 无歧义业务键。"""
        from app.compute import product_dedup_key
        return product_dedup_key(item, key_fields, 2)

    build_product_dedup_key_v2 = build_key_v2
    migrate_key_v2 = build_key_v2
