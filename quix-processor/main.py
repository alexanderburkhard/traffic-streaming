import os
from datetime import timedelta, datetime
from quixstreams import Application
from quixstreams.sinks.community.postgresql import PostgreSQLSink
from quixstreams.models import TimestampType
from typing import Any, Optional, List, Tuple

# Set up the Quix Application
app = Application(
    broker_address=os.environ["KAFKA_BROKER"],
    consumer_group="quix-processor-consumer-group",
    auto_create_topics=True,
)

def custom_ts_extractor(
    value: Any,
    headers: Optional[List[Tuple[str, bytes]]],
    timestamp: float,
    timestamp_type: TimestampType,
) -> int:
    
    try:
        dt_obj = datetime.strptime(value['timestamp'], '%Y-%m-%d %H:%M:%S.%f')
    except:
        dt_obj = datetime.strptime(value['timestamp'], '%Y-%m-%d %H:%M:%S')
    milliseconds = int(dt_obj.timestamp() * 1000)
    value['timestamp'] = milliseconds
    return value["timestamp"]


input_topic = app.topic(name="traffic-events",
                        value_deserializer="json",
                        timestamp_extractor=custom_ts_extractor)

sdf = app.dataframe(input_topic)

#sdf = sdf[["timestamp", "highway_id", "plate", "speed", "ev"]]

#sdf = sdf.group_by('highway_id')

sdf['new_keys'] = sdf.apply(lambda value, key, timestamp, headers: key, metadata=True)

sdf = (
    sdf.apply(lambda value: value["speed"])
    .sliding_window(duration_ms=timedelta(minutes=30))
    .mean()
    .final()
    .apply(
        lambda result: {
            "avg_speed": result["value"],
            "window_start_ms": datetime.fromtimestamp(result["start"]/1000),
            "window_end_ms": datetime.fromtimestamp(result["end"]/1000)
        })
)

sdf['new_keys'] = sdf.apply(lambda value, key, timestamp, headers: str(key)[2:3], metadata=True)

postgres_sink = PostgreSQLSink(
    host="postgresql",
    port=5432,
    dbname="traffic_db",
    user="pguser",
    password="pgpass",
    table_name="traffic_averages",
    schema_auto_update=True
)

#sdf.update(lambda row: print(row))
sdf.sink(postgres_sink)

# Run the streaming application
if __name__ == "__main__":
    app.run()
