from staging_queries.azure import exports

from ai.chronon.types import Aggregation, EventSource, GroupBy, Operation, Query, TimeUnit, Window, selects
from ai.chronon.types import EnvironmentVariables

"""
This GroupBy aggregates user activity metrics from the user-activities-v0 topic.
It tracks various user behaviors (views, clicks, purchases, favorites, add_to_cart)
with last_k, sum, and average aggregations over multiple time windows.
"""

serde = "serde=apicurio_registry"
registry_host = "registry_host=apicurio-registry-apicurio-registry.zipline-system.svc.cluster.local"
registry_port = "registry_port=8080"
artifact_id = "artifact_id=user-activities-js"
schema_registry_kv = f"{serde}/{registry_host}/{registry_port}/{artifact_id}"

security_protocol = "security.protocol=SASL_SSL"
sasl_mechanism = "sasl.mechanism=PLAIN"
# Note: Sasl jaas config contains secrets that are read by the Flink app after being injected via Key Vault
kafka_sasl_kv = f"{security_protocol}/{sasl_mechanism}"

source = EventSource(
    # This is the Iceberg table that receives the EventHub data
    table=exports.user_activities.table,
    topic=f"kafka://user-activities/{schema_registry_kv}/{kafka_sasl_kv}",
    query=Query(
            selects=selects(
                user_id="user_id",
                listing_id="listing_id",
                # Create binary flags for each event type
                view_event="IF(event_type = 'view', 1, 0)",
                click_event="IF(event_type = 'click', 1, 0)",
                purchase_event="IF(event_type = 'purchase', 1, 0)",
                favorite_event="IF(event_type = 'favorite', 1, 0)",
                add_to_cart_event="IF(event_type = 'add_to_cart', 1, 0)",
                # Device type flags
                is_mobile="IF(device_type = 'mobile', 1, 0)",
                is_desktop="IF(device_type = 'desktop', 1, 0)",
                is_tablet="IF(device_type = 'tablet', 1, 0)",
                # Activity structs for last_k tracking
                user_event_struct="STRUCT(event_type, listing_id, unix_millis(TIMESTAMP(event_time_ms)) as timestamp)",
            ),
            time_column="unix_millis(TIMESTAMP(event_time_ms))",
    ),
)

# Define window sizes for aggregations (1d, 7d, 14d, 30d)
window_sizes = [Window(length=days, time_unit=TimeUnit.DAYS) for days in [1, 7, 14, 30]]

# Event type columns for aggregations
event_columns = ["view_event", "click_event", "purchase_event", "favorite_event", "add_to_cart_event"]
device_columns = ["is_mobile", "is_desktop", "is_tablet"]
last_k_columns = ["user_event_struct"]

aggregations = []

# Event type aggregations - Sum and Average over various windows
aggregations.extend([
    Aggregation(input_column=col, operation=Operation.SUM, windows=window_sizes)
    for col in event_columns
])

aggregations.extend([
    Aggregation(input_column=col, operation=Operation.AVERAGE, windows=window_sizes)
    for col in event_columns
])

# Device type aggregations - Sum over various windows
aggregations.extend([
    Aggregation(input_column=col, operation=Operation.SUM, windows=window_sizes)
    for col in device_columns
])

# Last K aggregations - Keep track of recent activity patterns
aggregations.extend([
    Aggregation(input_column=col, operation=Operation.LAST_K(128), windows=window_sizes)
    for col in last_k_columns
])

v1 = GroupBy(
    sources=[source],
    keys=["user_id"],  # Aggregate by user
    online=False,
    version=1,
    aggregations=aggregations,
    step_days=30,
)
