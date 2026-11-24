"""服务层导入别名：商品去重键迁移。"""
from app.services.product_dedup import *  # noqa: F401,F403
from app.services.product_dedup import (  # noqa: F401
    DedupMigrationService,
    ProductDedupMigrationService,
    ProductDedupService,
    product_dedup_service,
    service,
)
