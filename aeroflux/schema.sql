CREATE SCHEMA IF NOT EXISTS swim;

CREATE TABLE IF NOT EXISTS swim.raw_messages (
    id BIGSERIAL PRIMARY KEY,
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    swim_received_at TIMESTAMPTZ,
    stored_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    solace_destination TEXT,
    xml_root_tag TEXT,
    flight_message_count INTEGER,
    message_types TEXT[],
    payload_size_bytes INTEGER,
    raw_xml TEXT NOT NULL,
    UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS idx_raw_messages_stored_at
    ON swim.raw_messages (stored_at DESC);

CREATE INDEX IF NOT EXISTS idx_raw_messages_types
    ON swim.raw_messages USING GIN (message_types);
