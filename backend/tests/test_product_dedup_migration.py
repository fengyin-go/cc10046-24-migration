"""商品去重 V2 键迁移与留档口径测试。"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.compute import build_product_dedup_key_v2
from app.routers.chains import run_data_process
from app.services.product_dedup import ProductDedupMigrationService


def _service() -> ProductDedupMigrationService:
    return ProductDedupMigrationService(seed=True)


def test_pipe_in_values_do_not_collide_in_v2():
    first = {"商品组": "A|B", "地区": "C"}
    second = {"商品组": "A", "地区": "B|C"}
    fields = ["商品组", "地区"]

    k1 = build_product_dedup_key_v2(first, fields)
    k2 = build_product_dedup_key_v2(second, fields)
    assert k1 != k2
    assert k1.startswith("v2:") and k2.startswith("v2:")


def test_jia_legacy_counts_are_archived():
    svc = _service()
    legacy = svc.run_legacy_dedup("甲", save_receipt=False)
    assert (
        legacy["original_count"],
        legacy["deduped_count"],
        legacy["removed_count"],
    ) == (2, 1, 1)
    assert legacy["matches_archived"] is True
    receipt = svc.receipts["RCPT-JIA-OLD"]
    assert (
        receipt["original_count"],
        receipt["deduped_count"],
        receipt["removed_count"],
    ) == (2, 1, 1)


def test_seeded_jia_publishes_v2_and_lookup_separates_products():
    svc = _service()
    processing = svc.data_processing("甲", 2)
    assert processing["available"] is True
    assert processing["read_version"] == 2
    assert (
        processing["original_count"],
        processing["deduped_count"],
        processing["removed_count"],
    ) == (2, 2, 0)

    first = svc.lookup_products({}, {
        "batch_id": "甲", "商品组": "A|B", "地区": "C",
    })
    second = svc.lookup_products({}, {
        "batch_id": "甲", "商品组": "A", "地区": "B|C",
    })
    assert first["products"][0]["商品ID"] == "GOODS-A1"
    assert second["products"][0]["商品ID"] == "GOODS-A2"


def test_yi_requires_raw_rows_then_continue_and_keep_archive():
    svc = _service()
    unavailable = svc.data_processing("乙", 2)
    assert unavailable["available"] is False
    assert unavailable["status"] == "待回填"
    assert unavailable["missing_evidence"]

    raw_rows = [
        {"商品ID": "B1", "商品组": "X|Y", "地区": "Z"},
        {"商品ID": "B2", "商品组": "X", "地区": "Y|Z"},
        {"商品ID": "B3", "商品组": "D", "地区": "E"},
    ]
    detail = svc.continue_migration("乙", {"rows": raw_rows})
    assert detail["status"] == "已发布"
    assert detail["new_counts"]["deduped_count"] == 3
    assert len(detail["actual_retained_products"]) == 3
    assert detail["retained_count_matches_detail"] is True

    old = svc.run_legacy_dedup("乙", save_receipt=False)
    assert old["steps"]["dedup"] == {
        "original_count": 3,
        "deduped_count": 2,
        "removed_count": 1,
    }
    assert old["matches_archived"] is True

    receipt = svc.reopen_receipt("RCPT-YI-OLD", {})
    assert receipt["status"] == "已重开"
    assert (
        receipt["original_count"],
        receipt["deduped_count"],
        receipt["removed_count"],
    ) == (3, 2, 1)

    first_pub = svc.publish_v2("乙", {})
    second_pub = svc.publish_v2("乙", {})
    assert first_pub["publication_id"] == second_pub["publication_id"]
    assert second_pub["already_published"] is True


def test_legacy_chain_shape_remains_three_dedup_fields():
    result = run_data_process({})
    assert result["chain"] == "data-process"
    assert set(result["steps"]["dedup"]) == {
        "original_count", "deduped_count", "removed_count",
    }
