// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package internal // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal"

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

const (
	// language=ClickHouse SQL
	createNumericTableSQL = `
CREATE TABLE IF NOT EXISTS %s %s (
    -- Time and identification
    Timestamp DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    MetricName LowCardinality(String) CODEC(ZSTD(1)),

    -- OpenTelemetry context
    ResourceSchemaUrl LowCardinality(String) CODEC(ZSTD(1)),
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeSchemaUrl LowCardinality(String) CODEC(ZSTD(1)),
    ScopeName String CODEC(ZSTD(1)),
    ScopeVersion String CODEC(ZSTD(1)),
    ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    MetricAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),

    -- Metric data
    Value Float64 CODEC(ZSTD(1)),
    IsMonotonic Bool CODEC(ZSTD(1)),
    StartTimeUnix DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    AggregationTemporality Nullable(Enum8('UNSPECIFIED' = 0, 'DELTA' = 1, 'CUMULATIVE' = 2)) CODEC(ZSTD(1)),

    -- Metadata
    MetricDescription String CODEC(ZSTD(1)),
    MetricUnit LowCardinality(String) CODEC(ZSTD(1)),

    -- Exemplars
    Exemplars Nested (
		FilteredAttributes Map(LowCardinality(String), String),
		TimeUnix DateTime64(9),
		Value Float64,
		SpanId String,
		TraceId String
    ) CODEC(ZSTD(1)),

    -- Bloom filter indexes
    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_value mapValues(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_metric_attr_key mapKeys(MetricAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_metric_attr_value mapValues(MetricAttributes) TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = %s
%s
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, MetricName, Timestamp)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
`

	// language=ClickHouse SQL
	createNumMetricsViewSQL = `
CREATE VIEW IF NOT EXISTS num_metrics AS
SELECT
    Timestamp,
    ServiceName,
    MetricName,
    
    -- metric info
    Value,
    IsMonotonic,
    StartTimeUnix,
    AggregationTemporality,

    -- attributes
    ResourceAttributes,
    ScopeAttributes,
    MetricAttributes
FROM %s
`

	// language=ClickHouse SQL
	insertNumericTableSQL = `INSERT INTO %s (
    Timestamp,
    ServiceName,
    MetricName,
    ResourceSchemaUrl,
    ResourceAttributes,
    ScopeSchemaUrl,
    ScopeName,
    ScopeVersion,
    ScopeAttributes,
    MetricAttributes,
    Value,
    IsMonotonic,
    StartTimeUnix,
    AggregationTemporality,
    MetricDescription,
    MetricUnit,
    Exemplars.FilteredAttributes,
    Exemplars.TimeUnix,
    Exemplars.Value,
    Exemplars.SpanId,
    Exemplars.TraceId) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
)

type numericModel struct {
	metricName        string
	metricDescription string
	metricUnit        string
	metadata          *MetricsMetaData
	metricType        pmetric.MetricType
	gauge             pmetric.Gauge
	sum               pmetric.Sum
}

type numericMetrics struct {
	numericModels []*numericModel
	insertSQL     string
	count         int
}

func (n *numericMetrics) insert(ctx context.Context, db *sql.DB) error {
	if n.count == 0 {
		return nil
	}
	start := time.Now()
	err := doWithTx(ctx, db, func(tx *sql.Tx) error {
		statement, err := tx.PrepareContext(ctx, n.insertSQL)
		if err != nil {
			return err
		}

		defer func() {
			_ = statement.Close()
		}()

		for _, model := range n.numericModels {
			resAttr := AttributesToMap(model.metadata.ResAttr)
			scopeAttr := AttributesToMap(model.metadata.ScopeInstr.Attributes())
			serviceName := GetServiceName(model.metadata.ResAttr)

			// Calculate metric type-specific values
			var isMonotonic bool
			var aggregationTemporality interface{}
			var dataPointCount int
			
			if model.metricType == pmetric.MetricTypeGauge {
				isMonotonic = false
				aggregationTemporality = nil // NULL for gauges
				dataPointCount = model.gauge.DataPoints().Len()
			} else if model.metricType == pmetric.MetricTypeSum {
				isMonotonic = model.sum.IsMonotonic()
				aggregationTemporality = int8(model.sum.AggregationTemporality()) // Convert to int8 for Enum8
				dataPointCount = model.sum.DataPoints().Len()
			}
			
			// Handle data points for both gauge and sum
			for i := 0; i < dataPointCount; i++ {
				var dp interface{
					Timestamp() pcommon.Timestamp
					StartTimestamp() pcommon.Timestamp
					Attributes() pcommon.Map
					IntValue() int64
					DoubleValue() float64
					ValueType() pmetric.NumberDataPointValueType
					Exemplars() pmetric.ExemplarSlice
				}
				
				if model.metricType == pmetric.MetricTypeGauge {
					dp = model.gauge.DataPoints().At(i)
				} else {
					dp = model.sum.DataPoints().At(i)
				}
				
				attrs, times, values, traceIDs, spanIDs := convertExemplars(dp.Exemplars())
				_, err = statement.ExecContext(ctx,
					dp.Timestamp().AsTime(),
					serviceName,
					model.metricName,
					model.metadata.ResURL,
					resAttr,
					model.metadata.ScopeURL,
					model.metadata.ScopeInstr.Name(),
					model.metadata.ScopeInstr.Version(),
					scopeAttr,
					AttributesToMap(dp.Attributes()),
					getValue(dp.IntValue(), dp.DoubleValue(), dp.ValueType()),
					isMonotonic,
					dp.StartTimestamp().AsTime(),
					aggregationTemporality,
					model.metricDescription,
					model.metricUnit,
					attrs,
					times,
					values,
					spanIDs,
					traceIDs,
				)
				if err != nil {
					return fmt.Errorf("ExecContext numeric:%w", err)
				}
			}
		}
		return err
	})
	duration := time.Since(start)
	if err != nil {
		logger.Debug("insert numeric metrics fail", zap.Duration("cost", duration))
		return fmt.Errorf("insert numeric metrics fail:%w", err)
	}
	logger.Debug("insert numeric metrics", zap.Int("records", n.count),
		zap.Duration("cost", duration))
	return nil
}

func (n *numericMetrics) Add(resAttr pcommon.Map, resURL string, scopeInstr pcommon.InstrumentationScope, scopeURL string, metrics any, name string, description string, unit string) error {
	metadata := &MetricsMetaData{
		ResAttr:    resAttr,
		ResURL:     resURL,
		ScopeURL:   scopeURL,
		ScopeInstr: scopeInstr,
	}

	// Handle gauge metrics
	if gauge, ok := metrics.(pmetric.Gauge); ok {
		n.count += gauge.DataPoints().Len()
		n.numericModels = append(n.numericModels, &numericModel{
			metricName:        name,
			metricDescription: description,
			metricUnit:        unit,
			metadata:          metadata,
			metricType:        pmetric.MetricTypeGauge,
			gauge:             gauge,
		})
		return nil
	}

	// Handle sum metrics
	if sum, ok := metrics.(pmetric.Sum); ok {
		n.count += sum.DataPoints().Len()
		n.numericModels = append(n.numericModels, &numericModel{
			metricName:        name,
			metricDescription: description,
			metricUnit:        unit,
			metadata:          metadata,
			metricType:        pmetric.MetricTypeSum,
			sum:               sum,
		})
		return nil
	}

	return errors.New("metrics param is not type of Gauge or Sum")
}