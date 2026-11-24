"""兼容导入路径：商品去重迁移服务的顶层别名。"""
from app.services.product_dedup import (  # noqa: F401
    DedupMigrationService,
    EvidenceMissingError,
    ProductDedupMigrationService,
    ProductDedupService,
    product_dedup_service,
)

service = product_dedup_service
migration_service = product_dedup_service
