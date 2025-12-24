package connector

import (
	"fmt"
	"time"
)

// OffsetManager quản lý offset cho connector
// Hỗ trợ các mode: SIMPLE_INCREMENTING, TIMESTAMP, CUSTOM
// Dựa trên kiến trúc offset management của Confluent HTTP Source Connector
type OffsetManager struct {
	config OffsetConfig
	offset Offset
}

// NewOffsetManager tạo offset manager mới
func NewOffsetManager(config OffsetConfig) *OffsetManager {
	om := &OffsetManager{
		config: config,
		offset: Offset{
			Mode:      config.Mode,
			Value:     config.InitialValue,
			LastCount: 0,
		},
	}

	// Set giá trị mặc định nếu chưa có
	if om.offset.Value == nil {
		switch config.Mode {
		case OffsetModeSimpleIncrementing:
			om.offset.Value = int64(0)
		case OffsetModeTimestamp:
			om.offset.Value = time.Time{}
		case OffsetModeCustom:
			om.offset.Value = ""
		}
	}

	return om
}

// GetOffset trả về offset hiện tại
func (om *OffsetManager) GetOffset() Offset {
	return om.offset
}

// SetOffset thiết lập offset
func (om *OffsetManager) SetOffset(offset Offset) error {
	if offset.Mode != om.config.Mode {
		return fmt.Errorf("offset mode mismatch: expected %s, got %s", om.config.Mode, offset.Mode)
	}
	om.offset = offset
	return nil
}

// Increment tăng offset (cho SIMPLE_INCREMENTING mode)
func (om *OffsetManager) Increment(recordCount int) error {
	if om.config.Mode != OffsetModeSimpleIncrementing {
		return fmt.Errorf("increment only supported for SIMPLE_INCREMENTING mode")
	}

	currentValue, ok := om.offset.Value.(int64)
	if !ok {
		return fmt.Errorf("invalid offset value type for SIMPLE_INCREMENTING")
	}

	incrementBy := om.config.IncrementBy
	if incrementBy == 0 {
		incrementBy = 1
	}

	om.offset.Value = currentValue + int64(incrementBy*recordCount)
	om.offset.LastCount = recordCount
	return nil
}

// UpdateTimestamp cập nhật timestamp (cho TIMESTAMP mode)
func (om *OffsetManager) UpdateTimestamp(timestamp time.Time) error {
	if om.config.Mode != OffsetModeTimestamp {
		return fmt.Errorf("update timestamp only supported for TIMESTAMP mode")
	}
	om.offset.Value = timestamp
	return nil
}

// UpdateCustom cập nhật custom offset value
func (om *OffsetManager) UpdateCustom(value interface{}) error {
	if om.config.Mode != OffsetModeCustom {
		return fmt.Errorf("update custom only supported for CUSTOM mode")
	}
	om.offset.Value = value
	return nil
}

// Reset reset offset về giá trị ban đầu
func (om *OffsetManager) Reset() {
	om.offset.Value = om.config.InitialValue
	om.offset.LastCount = 0
}

