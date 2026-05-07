from staging_queries.gcp import exports

from ai.chronon.group_by import DefaultAggregation
from ai.chronon.types import Accuracy, EntitySource, GroupBy, Query, selects

"""
Mirrors dim_listings.v1 but uses an EntitySource with a mutation_table so
point-in-time / temporal lookups against listing dimensions reflect mutations
instead of only daily snapshots. Defaults to mutation_time_column="mutation_ts"
and reversal_column="is_before", which match the dim_listing_mutations schema.
"""

source = EntitySource(
    snapshot_table=exports.dim_listings.table,
    mutation_table=exports.dim_listing_mutations.table,
    query=Query(
        selects=selects(
            listing_id="listing_id",
            merchant_id="merchant_id",
            headline="headline",
            brief_description="brief_description",
            long_description="long_description",
            price_cents="price_cents",
            currency="currency",
            inventory_count="inventory_count",
            primary_category="primary_category",
            is_active="is_active",
            weight_grams="weight_grams",
            tags="tags",
            is_expensive="IF(price_cents > 10000, 1, 0)",
            is_in_stock="IF(inventory_count > 0, 1, 0)",
            main_image_path="main_image_path",
            secondary_image_paths="secondary_image_paths",
        ),
        start_partition="2025-01-01",
    ),
)

v2 = GroupBy(
    sources=[source],
    keys=["listing_id"],
    online=True,
    version=0,
    accuracy=Accuracy.TEMPORAL,
    aggregations=DefaultAggregation(keys=["listing_id"], sources=[source]),
)
