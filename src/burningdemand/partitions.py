# burningdemand_dagster/partitions.py
from dagster import (
    DailyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
)

source_partitions = StaticPartitionsDefinition(
    ["github", "stackoverflow", "reddit", "hackernews"]
)
daily_partitions = DailyPartitionsDefinition(start_date="2024-10-22")
source_day_partitions = MultiPartitionsDefinition(
    {"source": source_partitions, "date": daily_partitions}
)
