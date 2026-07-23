package limits

import (
	"errors"
	"fmt"
	"time"
)

const (
	DefaultTraceDepth    uint32 = 4
	HardMaxTraceDepth    uint32 = 32
	DefaultTraceNodes    uint32 = 10_000
	HardMaxTraceNodes    uint32 = 100_000
	DefaultAddressPage   uint32 = 1_000
	HardMaxAddressPage   uint32 = 10_000
	DefaultFrontierBatch uint32 = 10_000
	HardMaxFrontierBatch uint32 = 10_000
	DefaultLayerTimeout         = 30 * time.Second
)

var (
	ErrDepthOutOfRange    = errors.New("max depth must be between 1 and 32")
	ErrNodesOutOfRange    = errors.New("max nodes must be between 1 and 100000")
	ErrPageOutOfRange     = errors.New("page limit must be between 1 and 10000")
	ErrFrontierOutOfRange = errors.New("frontier batch must be between 1 and 10000")
)

type Trace struct {
	MaxDepth      uint32
	MaxNodes      uint32
	FrontierBatch uint32
	LayerTimeout  time.Duration
}

func DefaultTrace() Trace {
	return Trace{
		MaxDepth:      DefaultTraceDepth,
		MaxNodes:      DefaultTraceNodes,
		FrontierBatch: DefaultFrontierBatch,
		LayerTimeout:  DefaultLayerTimeout,
	}
}

func (value Trace) Validate() error {
	if value.MaxDepth == 0 || value.MaxDepth > HardMaxTraceDepth {
		return ErrDepthOutOfRange
	}
	if value.MaxNodes == 0 || value.MaxNodes > HardMaxTraceNodes {
		return ErrNodesOutOfRange
	}
	if value.FrontierBatch == 0 || value.FrontierBatch > HardMaxFrontierBatch {
		return ErrFrontierOutOfRange
	}
	if value.LayerTimeout <= 0 || value.LayerTimeout > DefaultLayerTimeout {
		return fmt.Errorf("layer timeout must be between 1ns and %s", DefaultLayerTimeout)
	}
	return nil
}

func ValidatePage(limit uint32) error {
	if limit == 0 || limit > HardMaxAddressPage {
		return ErrPageOutOfRange
	}
	return nil
}
