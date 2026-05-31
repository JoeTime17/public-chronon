"""Canary fixture: ModelTransforms with NO explicit `output_namespace=`.

Relies on the enclosing `gcp` team's `outputNamespace="data"` default. Compile
must stamp that namespace onto the compiled Thrift.
"""

from joins.gcp import demo
from models.gcp import click_through_rate

from ai.chronon.data_types import DataType
from ai.chronon.types import JoinSource, ModelTransforms

source = JoinSource(join=demo.derivations_v1)

v1 = ModelTransforms(
    sources=[source],
    models=[click_through_rate.ctr_model],
    passthrough_fields=[
        "user_id",
        "listing_id",
        "user_id_click_event_average_7d",
        "listing_id_price_cents",
        "price_log",
        "price_bucket",
    ],
    version=1,
    key_fields=[
        ("user_id_click_event_average_7d", DataType.DOUBLE),
        ("listing_id_price_cents", DataType.LONG),
        ("price_log", DataType.DOUBLE),
        ("price_bucket", DataType.INT),
    ],
)
