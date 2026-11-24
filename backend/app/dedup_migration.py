"""导入别名：商品去重迁移场景。"""
from app.services.product_dedup import *  # noqa: F401,F403
from app.services.product_dedup import (  # noqa: F401
    DedupMigrationService,
    ProductDedupMigrationService,
    ProductDedupService,
    product_dedup_service,
    service,
)

migration_service = product_dedup_service
