import os
from datetime import timedelta
from quixstreams import Application
from quixstreams.sinks.community.postgresql import PostgreSQLSink

# Set up the Quix Application
app = Application(
    broker_address=os.environ["KAFKA_BROKER"],
    consumer_group="quix-processor-consumer-group",
    auto_create_topics=True,
)

input_topic = app.topic(name="traffic-events", value_deserializer="json")

sdf = app.dataframe(input_topic)

#sdf = sdf[["timestamp", "highway_id", "plate", "speed", "ev"]]

#sdf = sdf.group_by('highway_id')

sdf = (
    # Extract "speed" value from the message
    sdf.apply(lambda value: value["speed"])

    # You can also pass duration_ms and step_ms as integers of milliseconds
    .sliding_window(duration_ms=timedelta(minutes=5))

    # Specify the "mean" aggregate function
    .mean()

    # Emit updates for each incoming message
    .final()

    # Unwrap the aggregated result to match the expected output format
    .apply(
        lambda result: {
            "avg_speed": result["value"],
            "window_start_ms": result["start"],
            "window_end_ms": result["end"],
        }
    )
)

postgres_sink = PostgreSQLSink(
    host="postgresql",
    port=5432,
    dbname="traffic_db",
    user="pguser",
    password="pgpass",
    table_name="traffic_averages",
    schema_auto_update=True
)

sdf.sink(postgres_sink)

# Run the streaming application
if __name__ == "__main__":
    app.run()

