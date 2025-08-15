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
	createDistributionTableSQL = `
CREATE TABLE IF NOT EXISTS %s %s (
    -- Time and identification
    TimeUnix DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    StartTimeUnix DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    MetricName LowCardinality(String) CODEC(ZSTD(1)),
    MetricDescription String CODEC(ZSTD(1)),
    MetricUnit LowCardinality(String) CODEC(ZSTD(1)),
    
    -- Distribution type discriminator
    DistributionType Enum8('HIST' = 1, 'EXP_HIST' = 2) CODEC(ZSTD(1)),
    
    -- OpenTelemetry context
    ResourceSchemaUrl LowCardinality(String) CODEC(ZSTD(1)),
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeSchemaUrl LowCardinality(String) CODEC(ZSTD(1)),
    ScopeName String CODEC(ZSTD(1)),
    ScopeVersion String CODEC(ZSTD(1)),
    ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
    MetricAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    
    -- Common distribution fields
    Count UInt64 CODEC(Delta, ZSTD(1)),
    Sum Float64 CODEC(ZSTD(1)),
    Min Float64 CODEC(ZSTD(1)),
    Max Float64 CODEC(ZSTD(1)),
    
    -- Traditional histogram fields (null for exponential histograms)
    BucketCounts Array(UInt64) CODEC(ZSTD(1)),
    ExplicitBounds Array(Float64) CODEC(ZSTD(1)),
    
    -- Exponential histogram fields (null for traditional histograms)
    Scale Int32 CODEC(ZSTD(1)),
    ZeroCount UInt64 CODEC(ZSTD(1)),
    PositiveOffset Int32 CODEC(ZSTD(1)),
    PositiveBucketCounts Array(UInt64) CODEC(ZSTD(1)),
    NegativeOffset Int32 CODEC(ZSTD(1)),
    NegativeBucketCounts Array(UInt64) CODEC(ZSTD(1)),
    
    -- OpenTelemetry metadata
    AggregationTemporality Int32 CODEC(ZSTD(1)),
    
    -- Exemplars for trace correlation
    Exemplars Nested (
		FilteredAttributes Map(LowCardinality(String), String),
		TimeUnix DateTime64(9),
		Value Float64,
		SpanId String,
		TraceId String
    ) CODEC(ZSTD(1)),
    
    -- Indexes for efficient querying
    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_value mapValues(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_metric_attr_key mapKeys(MetricAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_metric_attr_value mapValues(MetricAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_distribution_type DistributionType TYPE set(0) GRANULARITY 1
    
) ENGINE = %s
%s
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, TimeUnix)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
`

	// language=ClickHouse SQL
	createDistMetricsViewSQL = `
CREATE VIEW IF NOT EXISTS dist_metrics AS
SELECT
    TimeUnix as Timestamp,
    ServiceName,
    MetricName,
    MetricDescription,
    MetricUnit,
    DistributionType,
    
    -- Core distribution statistics
    Count,
    Sum,
    Min,
    Max,
    
    -- Traditional histogram fields
    BucketCounts,
    ExplicitBounds,
    
    -- Exponential histogram fields
    Scale,
    ZeroCount,
    PositiveOffset,
    PositiveBucketCounts,
    NegativeOffset,
    NegativeBucketCounts,
    
    -- Simplified attributes
    ResourceAttributes,
    MetricAttributes
    
FROM %s
`

	// language=ClickHouse SQL
	insertDistributionTableSQL = `INSERT INTO %s (
    TimeUnix,
    StartTimeUnix,
    ServiceName,
    MetricName,
    MetricDescription,
    MetricUnit,
    DistributionType,
    ResourceSchemaUrl,
    ResourceAttributes,
    ScopeSchemaUrl,
    ScopeName,
    ScopeVersion,
    ScopeAttributes,
    ScopeDroppedAttrCount,
    MetricAttributes,
    Count,
    Sum,
    Min,
    Max,
    BucketCounts,
    ExplicitBounds,
    Scale,
    ZeroCount,
    PositiveOffset,
    PositiveBucketCounts,
    NegativeOffset,
    NegativeBucketCounts,
    AggregationTemporality,
    Exemplars.FilteredAttributes,
    Exemplars.TimeUnix,
    Exemplars.Value,
    Exemplars.SpanId,
    Exemplars.TraceId) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
)

type distributionModel struct {
	metricName        string
	metricDescription string
	metricUnit        string
	metadata          *MetricsMetaData
	metricType        pmetric.MetricType
	histogram         pmetric.Histogram
	expHistogram      pmetric.ExponentialHistogram
}

type distributionMetrics struct {
	distributionModels []*distributionModel
	insertSQL          string
	count              int
}

func (d *distributionMetrics) insert(ctx context.Context, db *sql.DB) error {
	if d.count == 0 {
		return nil
	}
	start := time.Now()
	err := doWithTx(ctx, db, func(tx *sql.Tx) error {
		statement, err := tx.PrepareContext(ctx, d.insertSQL)
		if err != nil {
			return err
		}

		defer func() {
			_ = statement.Close()
		}()

		for _, model := range d.distributionModels {
			resAttr := AttributesToMap(model.metadata.ResAttr)
			scopeAttr := AttributesToMap(model.metadata.ScopeInstr.Attributes())
			serviceName := GetServiceName(model.metadata.ResAttr)

			// Determine distribution type and data points
			var distributionType int8
			var dataPointCount int
			var aggregationTemporality int32

			if model.metricType == pmetric.MetricTypeHistogram {
				distributionType = 1 // 'HIST'
				dataPointCount = model.histogram.DataPoints().Len()
				aggregationTemporality = int32(model.histogram.AggregationTemporality())
			} else if model.metricType == pmetric.MetricTypeExponentialHistogram {
				distributionType = 2 // 'EXP_HIST'
				dataPointCount = model.expHistogram.DataPoints().Len()
				aggregationTemporality = int32(model.expHistogram.AggregationTemporality())
			}

			// Process data points
			for i := 0; i < dataPointCount; i++ {
				if model.metricType == pmetric.MetricTypeHistogram {
					dp := model.histogram.DataPoints().At(i)
					attrs, times, values, traceIDs, spanIDs := convertExemplars(dp.Exemplars())
					
					_, err = statement.ExecContext(ctx,
						dp.Timestamp().AsTime(),
						dp.StartTimestamp().AsTime(),
						serviceName,
						model.metricName,
						model.metricDescription,
						model.metricUnit,
						distributionType,
						model.metadata.ResURL,
						resAttr,
						model.metadata.ScopeURL,
						model.metadata.ScopeInstr.Name(),
						model.metadata.ScopeInstr.Version(),
						scopeAttr,
						model.metadata.ScopeInstr.DroppedAttributesCount(),
						AttributesToMap(dp.Attributes()),
						dp.Count(),
						dp.Sum(),
						dp.Min(),
						dp.Max(),
						// Traditional histogram fields
						convertSliceToArraySet(dp.BucketCounts().AsRaw()),
						convertSliceToArraySet(dp.ExplicitBounds().AsRaw()),
						// Exponential histogram fields (empty for traditional histograms)
						int32(0), // Scale
						uint64(0), // ZeroCount
						int32(0), // PositiveOffset
						convertSliceToArraySet([]uint64{}), // PositiveBucketCounts
						int32(0), // NegativeOffset
						convertSliceToArraySet([]uint64{}), // NegativeBucketCounts
						aggregationTemporality,
						attrs,
						times,
						values,
						spanIDs,
						traceIDs,
					)
				} else { // ExponentialHistogram
					dp := model.expHistogram.DataPoints().At(i)
					attrs, times, values, traceIDs, spanIDs := convertExemplars(dp.Exemplars())
					
					_, err = statement.ExecContext(ctx,
						dp.Timestamp().AsTime(),
						dp.StartTimestamp().AsTime(),
						serviceName,
						model.metricName,
						model.metricDescription,
						model.metricUnit,
						distributionType,
						model.metadata.ResURL,
						resAttr,
						model.metadata.ScopeURL,
						model.metadata.ScopeInstr.Name(),
						model.metadata.ScopeInstr.Version(),
						scopeAttr,
						model.metadata.ScopeInstr.DroppedAttributesCount(),
						AttributesToMap(dp.Attributes()),
						dp.Count(),
						dp.Sum(),
						dp.Min(),
						dp.Max(),
						// Traditional histogram fields (empty for exponential histograms)
						convertSliceToArraySet([]uint64{}), // BucketCounts
						convertSliceToArraySet([]float64{}), // ExplicitBounds
						// Exponential histogram fields
						dp.Scale(),
						dp.ZeroCount(),
						dp.Positive().Offset(),
						convertSliceToArraySet(dp.Positive().BucketCounts().AsRaw()),
						dp.Negative().Offset(),
						convertSliceToArraySet(dp.Negative().BucketCounts().AsRaw()),
						aggregationTemporality,
						attrs,
						times,
						values,
						spanIDs,
						traceIDs,
					)
				}
				
				if err != nil {
					return fmt.Errorf("ExecContext distribution:%w", err)
				}
			}
		}
		return err
	})
	duration := time.Since(start)
	if err != nil {
		logger.Debug("insert distribution metrics fail", zap.Duration("cost", duration))
		return fmt.Errorf("insert distribution metrics fail:%w", err)
	}
	logger.Debug("insert distribution metrics", zap.Int("records", d.count),
		zap.Duration("cost", duration))
	return nil
}

func (d *distributionMetrics) Add(resAttr pcommon.Map, resURL string, scopeInstr pcommon.InstrumentationScope, scopeURL string, metrics any, name string, description string, unit string) error {
	metadata := &MetricsMetaData{
		ResAttr:    resAttr,
		ResURL:     resURL,
		ScopeURL:   scopeURL,
		ScopeInstr: scopeInstr,
	}

	// Handle histogram metrics
	if histogram, ok := metrics.(pmetric.Histogram); ok {
		d.count += histogram.DataPoints().Len()
		d.distributionModels = append(d.distributionModels, &distributionModel{
			metricName:        name,
			metricDescription: description,
			metricUnit:        unit,
			metadata:          metadata,
			metricType:        pmetric.MetricTypeHistogram,
			histogram:         histogram,
		})
		return nil
	}

	// Handle exponential histogram metrics
	if expHistogram, ok := metrics.(pmetric.ExponentialHistogram); ok {
		d.count += expHistogram.DataPoints().Len()
		d.distributionModels = append(d.distributionModels, &distributionModel{
			metricName:        name,
			metricDescription: description,
			metricUnit:        unit,
			metadata:          metadata,
			metricType:        pmetric.MetricTypeExponentialHistogram,
			expHistogram:      expHistogram,
		})
		return nil
	}

	return errors.New("metrics param is not type of Histogram or ExponentialHistogram")
}