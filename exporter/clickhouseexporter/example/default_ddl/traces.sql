-- Default Spans and Traces table DDL with Phase 2 implementation

-- Main spans table (renamed from otel_traces)
CREATE TABLE IF NOT EXISTS otel_spans (
    Timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),
    TraceId String CODEC(ZSTD(1)),
    SpanId String CODEC(ZSTD(1)),
    ParentSpanId String CODEC(ZSTD(1)),
    TraceState String CODEC(ZSTD(1)),
    SpanName LowCardinality(String) CODEC(ZSTD(1)),
    SpanKind LowCardinality(String) CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeName String CODEC(ZSTD(1)),
    ScopeVersion String CODEC(ZSTD(1)),
    SpanAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    Duration UInt64 CODEC(ZSTD(1)),
    StatusCode LowCardinality(String) CODEC(ZSTD(1)),
    StatusMessage String CODEC(ZSTD(1)),
    Events Nested (
        Timestamp DateTime64(9),
        Name LowCardinality(String),
        Attributes Map(LowCardinality(String), String)
    ) CODEC(ZSTD(1)),
    Links Nested (
        TraceId String,
        SpanId String,
        TraceState String,
        Attributes Map(LowCardinality(String), String)
    ) CODEC(ZSTD(1)),
    INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_span_attr_key mapKeys(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_span_attr_value mapValues(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_duration Duration TYPE minmax GRANULARITY 1
) ENGINE = MergeTree()
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SpanName, toDateTime(Timestamp))
TTL toDate(Timestamp) + toIntervalDay(180)
SETTINGS index_granularity=8192, ttl_only_drop_parts = 1;

-- Traces aggregation table for optimized trace queries
CREATE TABLE IF NOT EXISTS traces_agg (
    TraceId String CODEC(ZSTD(1)),
    
    -- Root span information
    TimestampState AggregateFunction(min, DateTime64(9)) CODEC(ZSTD(1)),
    SpanNameState AggregateFunction(argMin, LowCardinality(String), DateTime64(9)) CODEC(ZSTD(1)),
    DurationState AggregateFunction(argMin, UInt64, DateTime64(9)) CODEC(ZSTD(1)),
    
    -- Trace-level aggregations
    SpanCountState AggregateFunction(count, UInt64) CODEC(ZSTD(1)),
    ServicesState AggregateFunction(groupUniqArray, LowCardinality(String)) CODEC(ZSTD(1)),
    
    -- Indexing for fast lookups
    INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1
) ENGINE = AggregatingMergeTree()
ORDER BY TraceId
TTL toDate(minMerge(TimestampState)) + toIntervalDay(180)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

-- Materialized view to populate traces aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS traces_agg_mv
TO traces_agg AS
SELECT
    TraceId,
    minState(Timestamp) AS TimestampState,
    argMinState(SpanName, Timestamp) AS SpanNameState,
    argMinState(Duration, Timestamp) AS DurationState,
    countState() AS SpanCountState,
    groupUniqArrayState(ServiceName) AS ServicesState
FROM otel_spans
GROUP BY TraceId;

-- Simplified traces view for easy trace-level queries
CREATE VIEW IF NOT EXISTS traces AS
SELECT
    TraceId,
    -- Extract final values from aggregate functions using State suffix columns
    minMerge(TimestampState) AS Timestamp,
    argMinMerge(SpanNameState) AS SpanName,
    argMinMerge(DurationState) AS Duration,
    -- Trace-level metrics
    countMerge(SpanCountState) AS SpanCount,
    groupUniqArrayMerge(ServicesState) AS Services
FROM traces_agg
GROUP BY TraceId;

-- Simplified spans view for easy span-level queries
CREATE VIEW IF NOT EXISTS spans AS
SELECT
    Timestamp,
    TraceId,
    SpanId,
    ParentSpanId,
    TraceState,
    SpanName,
    SpanKind,
    ServiceName,
    ResourceAttributes,
    SpanAttributes,
    Duration,
    StatusCode,
    StatusMessage
FROM otel_spans;
