"""通用迁移入口别名。"""
from app.services.product_dedup import (  # noqa: F401
    EvidenceMissingError,
    ProductDedupMigrationService,
    product_dedup_service,
    service,
)

dedup_migration_service = product_dedup_service
