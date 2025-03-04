-- Create the traffic_events table
CREATE TABLE IF NOT EXISTS traffic_events (
    start TIMESTAMP,
    end TIMESTAMP,
    avg_speed DOUBLE PRECISION
);