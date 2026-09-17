package tieredkv

import (
	"github.com/Fixxar/tieredkv/pkg/tieredkv/core"
)

// Common errors re-exported from core
var (
	ErrKeyNotFound    = core.ErrKeyNotFound
	ErrKeyTooLarge    = core.ErrKeyTooLarge
	ErrValueTooLarge  = core.ErrValueTooLarge
	ErrCorruptData    = core.ErrCorruptData
	ErrDBClosed       = core.ErrDBClosed
	ErrInvalidOptions = core.ErrInvalidOptions
	ErrReadOnly       = core.ErrReadOnly
)

// Type aliases re-exported from core for top-level convenience
type (
	TierType        = core.TierType
	CompressionType = core.CompressionType
	Metadata        = core.Metadata
	Stats           = core.Stats
)

// Constants re-exported from core
const (
	TierHot  = core.TierHot
	TierCold = core.TierCold

	CompressionNone   = core.CompressionNone
	CompressionSnappy = core.CompressionSnappy
	CompressionZstd   = core.CompressionZstd
)
