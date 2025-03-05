import os
from datetime import timedelta
from quixstreams import Application, State
from quixstreams.models.serializers import JSONSerializer, JSONDeserializer
import pandas as pd

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

sdf.print_table(size=100)
#sdf.update(lambda row: print(row))

# Run the streaming application
if __name__ == "__main__":
    app.run()

# output_topic = app.topic(
#     "highway-speed-average", value_serializer=JSONSerializer()
# )

# def calculate_average_speed(state: State, value):

#     highway_id = value["highway_id"]
#     speed = value["speed"]
#     timestamp = pd.to_datetime(value["timestamp"])
#     five_minutes_ago = timestamp - pd.Timedelta(minutes=5)

#     if highway_id not in state:
#         state[highway_id] = pd.DataFrame(columns=['timestamp', 'speed'])

#     df = state[highway_id]
#     df = pd.concat([df, pd.DataFrame([{'timestamp': timestamp, 'speed': speed}])], ignore_index=True)
#     df['timestamp'] = pd.to_datetime(df['timestamp'])

#     df = df[df['timestamp'] >= five_minutes_ago]

#     if len(df) > 0:
#         average_speed = df["speed"].mean()
#     else:
#         average_speed = 0

#     state[highway_id] = df
#     return {"highway_id": highway_id, "average_speed": average_speed, 'timestamp':str(timestamp)}


# sdf = app.dataframe(input_topic)

# # apply the calculation to the dataframe
# calculated_df = sdf.apply(calculate_average_speed, stateful=True)

# # push it to the output topic
# calculated_df.to_topic(output_topic)

# if __name__ == "__main__":
#     app.run(commit_every=1)
