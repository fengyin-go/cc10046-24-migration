"""通用计算引擎：跨模块复用的纯计算函数，不依赖任何外部框架。"""
from __future__ import annotations

import hashlib
import math
import time
from difflib import SequenceMatcher
from typing import Any


def content_hash(content: str | bytes, algorithm: str = "sha256") -> str:
    """对内容计算哈希摘要。"""
    if isinstance(content, str):
        content = content.encode("utf-8")
    h = hashlib.new(algorithm)
    h.update(content)
    return h.hexdigest()


def canonical_dedup_key(parts: list[Any], version: int | str = 1) -> str:
    """构造业务键的规范字符串。

    v1 直接用 ``|`` 拼接，保留历史口径；这种方式会让
    ``("A|B", "C")`` 与 ``("A", "B|C")`` 撞键。
    v2 使用长度前缀编码，字段值中的分隔符不再能改变字段边界。
    """
    values = ["" if value is None else str(value) for value in parts]
    if str(version).lower() in {"2", "v2", "product-dedup-v2"}:
        return "v2:" + "".join(f"{len(value)}:{value}" for value in values)
    return "|".join(values)


def product_dedup_key(
    item: dict[str, Any],
    key_fields: list[str],
    version: int | str = 1,
) -> str:
    """计算单条商品行的去重业务键（未哈希的规范形式）。"""
    return canonical_dedup_key([item.get(field, "") for field in key_fields], version)


def build_product_dedup_key_v2(
    item_or_values: dict[str, Any] | list[Any] | tuple[Any, ...],
    key_fields: list[str] | None = None,
) -> str:
    """构造版本 2 商品去重键。

    只能从真实原始行或显式字段值构造；不要尝试从旧拼接字符串反解析，
    因为字段值本身可能包含旧分隔符。
    """
    if isinstance(item_or_values, dict):
        if not key_fields:
            raise ValueError("从原始行构造 V2 键时必须提供 key_fields")
        values = [item_or_values.get(field, "") for field in key_fields]
    else:
        values = list(item_or_values)
    return canonical_dedup_key(values, 2)


# 常见命名别名。
build_dedup_key_v2 = build_product_dedup_key_v2
migrate_dedup_key_v2 = build_product_dedup_key_v2


def hash_dedup(
    items: list[dict[str, Any]],
    key_fields: list[str],
    algorithm: str = "sha256",
    key_version: int | str = 1,
    version: int | str | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """基于内容哈希对数据行去重，返回去重结果和去重数。

    ``key_version`` 默认为 1，严格保留旧拼接键口径；迁移到版本 2 时
    显式传 ``key_version=2`` 或 ``version=2``，使用无歧义的长度前缀键。
    """
    if version is not None:
        key_version = version
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    for item in items:
        canonical = product_dedup_key(item, key_fields, key_version)
        h = content_hash(canonical, algorithm)
        if h not in seen:
            seen.add(h)
            result.append(item)
    return result, len(items) - len(result)


def dedup_counts(
    items: list[dict[str, Any]],
    key_fields: list[str],
    version: int | str = 1,
) -> dict[str, int]:
    """返回去重前、保留、去除三个标准计数。"""
    deduped, removed = hash_dedup(items, key_fields, version=version)
    return {
        "original_count": len(items),
        "deduped_count": len(deduped),
        "retained_count": len(deduped),
        "removed_count": removed,
    }


def text_similarity(a: str, b: str) -> float:
    """计算两段文本的相似度，返回值 0.0~1.0。"""
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def rate_limit_check(
    request_count: int,
    window_seconds: float,
    max_per_second: float,
    now: float | None = None,
) -> tuple[bool, float]:
    """限速检查：返回 (是否通过, 建议等待秒数)。"""
    if now is None:
        now = time.time()
    if window_seconds <= 0 or max_per_second <= 0:
        return True, 0.0
    max_in_window = max_per_second * window_seconds
    if request_count <= max_in_window:
        return True, 0.0
    wait = (request_count - max_in_window) / max_per_second
    return False, max(wait, 0.0)


def batch_partition(
    items: list[Any],
    batch_size: int,
    min_batches: int = 0,
) -> list[list[Any]]:
    """将数据行按指定大小切片，并保证最小切片数。"""
    if not items:
        return []
    if len(items) <= min_batches * batch_size:
        actual_size = max(1, math.ceil(len(items) / max(min_batches, 1)))
    else:
        actual_size = max(1, batch_size)
    partitions: list[list[Any]] = []
    for i in range(0, len(items), actual_size):
        partitions.append(items[i:i + actual_size])
    if len(partitions) < min_batches and partitions:
        while len(partitions) < min_batches:
            partitions.append([])
    return partitions


def retry_calculator(
    attempt: int,
    base_delay: float = 1.0,
    max_delay: float = 300.0,
    multiplier: float = 2.0,
    jitter: bool = True,
) -> float:
    """指数退避重试延迟计算器，返回下次等待秒数。"""
    import random
    delay = base_delay * (multiplier ** max(attempt - 1, 0))
    delay = min(delay, max_delay)
    if jitter:
        delay = delay * (0.5 + random.random() * 0.5)
    return round(delay, 3)