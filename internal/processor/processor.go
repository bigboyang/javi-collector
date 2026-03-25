// Package processor implements an OTel Collector–style pluggable processor pipeline.
//
// Each Processor transforms, filters, or enriches spans/metrics/logs.
// A Pipeline chains multiple processors sequentially so that the output of one
// becomes the input of the next.
//
// Usage:
//
//	pipeline := processor.NewPipeline(
//	    processor.NewCardinalityProcessor(processor.DefaultCardinalityLimits()),
//	)
//	spans, err = pipeline.ProcessSpans(ctx, spans)
package processor

import (
	"context"

	"github.com/kkc/javi-collector/internal/model"
)

// Processor is the interface every pipeline stage must implement.
type Processor interface {
	ProcessSpans(ctx context.Context, spans []*model.SpanData) ([]*model.SpanData, error)
	ProcessMetrics(ctx context.Context, metrics []*model.MetricData) ([]*model.MetricData, error)
	ProcessLogs(ctx context.Context, logs []*model.LogData) ([]*model.LogData, error)
}

// Pipeline executes a chain of Processors in declaration order.
// It is safe for concurrent use.
type Pipeline struct {
	processors []Processor
}

// NewPipeline builds a pipeline from the supplied processors.
// An empty pipeline is a no-op pass-through.
func NewPipeline(processors ...Processor) *Pipeline {
	return &Pipeline{processors: processors}
}

// Enabled returns true when at least one processor is registered.
func (p *Pipeline) Enabled() bool { return len(p.processors) > 0 }

// ProcessSpans runs spans through every processor in order.
func (p *Pipeline) ProcessSpans(ctx context.Context, spans []*model.SpanData) ([]*model.SpanData, error) {
	var err error
	for _, proc := range p.processors {
		spans, err = proc.ProcessSpans(ctx, spans)
		if err != nil {
			return spans, err
		}
	}
	return spans, nil
}

// ProcessMetrics runs metrics through every processor in order.
func (p *Pipeline) ProcessMetrics(ctx context.Context, metrics []*model.MetricData) ([]*model.MetricData, error) {
	var err error
	for _, proc := range p.processors {
		metrics, err = proc.ProcessMetrics(ctx, metrics)
		if err != nil {
			return metrics, err
		}
	}
	return metrics, nil
}

// ProcessLogs runs logs through every processor in order.
func (p *Pipeline) ProcessLogs(ctx context.Context, logs []*model.LogData) ([]*model.LogData, error) {
	var err error
	for _, proc := range p.processors {
		logs, err = proc.ProcessLogs(ctx, logs)
		if err != nil {
			return logs, err
		}
	}
	return logs, nil
}
