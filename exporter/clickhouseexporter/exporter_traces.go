// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clickhouseexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter"

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2" // For register database driver.
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/traceutil"
)

type tracesExporter struct {
	client    *sql.DB
	insertSQL string

	logger *zap.Logger
	cfg    *Config
}

func newTracesExporter(logger *zap.Logger, cfg *Config) (*tracesExporter, error) {
	client, err := newClickhouseClient(cfg)
	if err != nil {
		return nil, err
	}

	return &tracesExporter{
		client:    client,
		insertSQL: renderInsertTracesSQL(cfg),
		logger:    logger,
		cfg:       cfg,
	}, nil
}

func (e *tracesExporter) start(ctx context.Context, _ component.Host) error {
	if !e.cfg.shouldCreateSchema() {
		return nil
	}

	if err := createDatabase(ctx, e.cfg); err != nil {
		return err
	}

	return createTracesTable(ctx, e.cfg, e.client)
}

// shutdown will shut down the exporter.
func (e *tracesExporter) shutdown(_ context.Context) error {
	if e.client != nil {
		return e.client.Close()
	}
	return nil
}

func (e *tracesExporter) pushTraceData(ctx context.Context, td ptrace.Traces) error {
	start := time.Now()
	err := doWithTx(ctx, e.client, func(tx *sql.Tx) error {
		statement, err := tx.PrepareContext(ctx, e.insertSQL)
		if err != nil {
			return fmt.Errorf("PrepareContext:%w", err)
		}
		defer func() {
			_ = statement.Close()
		}()
		for i := 0; i < td.ResourceSpans().Len(); i++ {
			spans := td.ResourceSpans().At(i)
			res := spans.Resource()
			resAttr := internal.AttributesToMap(res.Attributes())
			serviceName := internal.GetServiceName(res.Attributes())

			for j := 0; j < spans.ScopeSpans().Len(); j++ {
				rs := spans.ScopeSpans().At(j).Spans()
				scopeName := spans.ScopeSpans().At(j).Scope().Name()
				scopeVersion := spans.ScopeSpans().At(j).Scope().Version()
				for k := 0; k < rs.Len(); k++ {
					r := rs.At(k)
					spanAttr := internal.AttributesToMap(r.Attributes())
					status := r.Status()
					eventTimes, eventNames, eventAttrs := convertEvents(r.Events())
					linksTraceIDs, linksSpanIDs, linksTraceStates, linksAttrs := convertLinks(r.Links())
					_, err = statement.ExecContext(ctx,
						r.StartTimestamp().AsTime(),
						traceutil.TraceIDToHexOrEmptyString(r.TraceID()),
						traceutil.SpanIDToHexOrEmptyString(r.SpanID()),
						traceutil.SpanIDToHexOrEmptyString(r.ParentSpanID()),
						r.TraceState().AsRaw(),
						r.Name(),
						r.Kind().String(),
						serviceName,
						resAttr,
						scopeName,
						scopeVersion,
						spanAttr,
						r.EndTimestamp().AsTime().Sub(r.StartTimestamp().AsTime()).Nanoseconds(),
						status.Code().String(),
						status.Message(),
						eventTimes,
						eventNames,
						eventAttrs,
						linksTraceIDs,
						linksSpanIDs,
						linksTraceStates,
						linksAttrs,
					)
					if err != nil {
						return fmt.Errorf("ExecContext:%w", err)
					}
				}
			}
		}
		return nil
	})
	duration := time.Since(start)
	e.logger.Debug("insert traces", zap.Int("records", td.SpanCount()),
		zap.String("cost", duration.String()))
	return err
}

func convertEvents(events ptrace.SpanEventSlice) (times []time.Time, names []string, attrs []column.IterableOrderedMap) {
	for i := 0; i < events.Len(); i++ {
		event := events.At(i)
		times = append(times, event.Timestamp().AsTime())
		names = append(names, event.Name())
		attrs = append(attrs, internal.AttributesToMap(event.Attributes()))
	}
	return
}

func convertLinks(links ptrace.SpanLinkSlice) (traceIDs []string, spanIDs []string, states []string, attrs []column.IterableOrderedMap) {
	for i := 0; i < links.Len(); i++ {
		link := links.At(i)
		traceIDs = append(traceIDs, traceutil.TraceIDToHexOrEmptyString(link.TraceID()))
		spanIDs = append(spanIDs, traceutil.SpanIDToHexOrEmptyString(link.SpanID()))
		states = append(states, link.TraceState().AsRaw())
		attrs = append(attrs, internal.AttributesToMap(link.Attributes()))
	}
	return
}

const (
	// language=ClickHouse SQL
	createSpansTableSQL = `
CREATE TABLE IF NOT EXISTS otel_spans %s (
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
) ENGINE = %s
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SpanName, toDateTime(Timestamp))
%s
SETTINGS index_granularity=8192, ttl_only_drop_parts = 1;
`
	// language=ClickHouse SQL
	insertSpansSQLTemplate = `INSERT INTO otel_spans (
                        Timestamp,
                        TraceId,
                        SpanId,
                        ParentSpanId,
                        TraceState,
                        SpanName,
                        SpanKind,
                        ServiceName,
					    ResourceAttributes,
						ScopeName,
						ScopeVersion,
                        SpanAttributes,
                        Duration,
                        StatusCode,
                        StatusMessage,
                        Events.Timestamp,
                        Events.Name,
                        Events.Attributes,
                        Links.TraceId,
                        Links.SpanId,
                        Links.TraceState,
                        Links.Attributes
                        ) VALUES (
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?
                                  )`
)

const (
	// language=ClickHouse SQL
	createTracesAggTableSQL = `
CREATE TABLE IF NOT EXISTS traces_agg %s (
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
%s
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
`

	// language=ClickHouse SQL
	createTracesAggMvSQL = `
CREATE MATERIALIZED VIEW IF NOT EXISTS traces_agg_mv %s
TO %s.traces_agg AS
SELECT
    TraceId,
    minState(Timestamp) AS TimestampState,
    argMinState(SpanName, Timestamp) AS SpanNameState,
    argMinState(Duration, Timestamp) AS DurationState,
    countState() AS SpanCountState,
    groupUniqArrayState(ServiceName) AS ServicesState
FROM %s.otel_spans
GROUP BY TraceId;
`

	// language=ClickHouse SQL
	createTracesViewSQL = `
CREATE VIEW IF NOT EXISTS traces %s AS
SELECT
    TraceId,
    -- Extract final values from aggregate functions using State suffix columns
    minMerge(TimestampState) AS Timestamp,
    argMinMerge(SpanNameState) AS TraceName,
    argMinMerge(DurationState) AS Duration,
    -- Trace-level metrics
    countMerge(SpanCountState) AS SpanCount,
    groupUniqArrayMerge(ServicesState) AS Services
FROM %s.traces_agg
GROUP BY TraceId;
`

	// language=ClickHouse SQL
	createSpansViewSQL = `
CREATE VIEW IF NOT EXISTS spans %s AS
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
FROM %s.otel_spans;
`
)

func createTracesTable(ctx context.Context, cfg *Config, db *sql.DB) error {
	// Create the main spans table (renamed from traces)
	if _, err := db.ExecContext(ctx, renderCreateSpansTableSQL(cfg)); err != nil {
		return fmt.Errorf("exec create spans table sql: %w", err)
	}

	// Create the traces aggregation table
	if _, err := db.ExecContext(ctx, renderCreateTracesAggTableSQL(cfg)); err != nil {
		return fmt.Errorf("exec create traces agg table sql: %w", err)
	}

	// Create the materialized view for traces aggregation
	if _, err := db.ExecContext(ctx, renderCreateTracesAggMvSQL(cfg)); err != nil {
		return fmt.Errorf("exec create traces agg materialized view sql: %w", err)
	}

	// Create the traces view for simplified access
	if _, err := db.ExecContext(ctx, renderCreateTracesViewSQL(cfg)); err != nil {
		return fmt.Errorf("exec create traces view sql: %w", err)
	}

	// Create the spans view for simplified access
	if _, err := db.ExecContext(ctx, renderCreateSpansViewSQL(cfg)); err != nil {
		return fmt.Errorf("exec create spans view sql: %w", err)
	}

	return nil
}

func renderInsertTracesSQL(cfg *Config) string {
	return strings.ReplaceAll(insertSpansSQLTemplate, "'", "`")
}

func renderCreateSpansTableSQL(cfg *Config) string {
	ttlExpr := generateTTLExpr(cfg.TTL, "toDateTime(Timestamp)")
	return fmt.Sprintf(createSpansTableSQL, cfg.clusterString(), cfg.tableEngineString(), ttlExpr)
}

func renderCreateTracesAggTableSQL(cfg *Config) string {
	ttlExpr := generateTTLExpr(cfg.TTL, "toDateTime(minMerge(TimestampState))")
	return fmt.Sprintf(createTracesAggTableSQL, cfg.clusterString(), ttlExpr)
}

func renderCreateTracesAggMvSQL(cfg *Config) string {
	return fmt.Sprintf(createTracesAggMvSQL, cfg.clusterString(), cfg.Database, cfg.Database)
}

func renderCreateTracesViewSQL(cfg *Config) string {
	return fmt.Sprintf(createTracesViewSQL, cfg.clusterString(), cfg.Database)
}

func renderCreateSpansViewSQL(cfg *Config) string {
	return fmt.Sprintf(createSpansViewSQL, cfg.clusterString(), cfg.Database)
}
