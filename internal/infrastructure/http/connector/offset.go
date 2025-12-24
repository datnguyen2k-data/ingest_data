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
		case OffsetModeCursorBased:
			om.offset.Value = "" // Cursor rỗng cho lần đầu
			om.offset.HasMore = true
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

// UpdateCursor cập nhật cursor (cho CURSOR_BASED mode)
// cursor: cursor mới từ response
// hasMore: có thêm dữ liệu không
func (om *OffsetManager) UpdateCursor(cursor string, hasMore bool) error {
	if om.config.Mode != OffsetModeCursorBased {
		return fmt.Errorf("update cursor only supported for CURSOR_BASED mode")
	}
	om.offset.Value = cursor
	om.offset.HasMore = hasMore
	return nil
}

// GetCursor trả về cursor hiện tại (cho CURSOR_BASED mode)
func (om *OffsetManager) GetCursor() (string, bool, error) {
	if om.config.Mode != OffsetModeCursorBased {
		return "", false, fmt.Errorf("get cursor only supported for CURSOR_BASED mode")
	}
	cursor, ok := om.offset.Value.(string)
	if !ok {
		return "", false, fmt.Errorf("invalid offset value type for CURSOR_BASED")
	}
	return cursor, om.offset.HasMore, nil
}

// HasMoreData kiểm tra còn dữ liệu không (cho CURSOR_BASED mode)
func (om *OffsetManager) HasMoreData() bool {
	if om.config.Mode != OffsetModeCursorBased {
		return false
	}
	return om.offset.HasMore
}

// Reset reset offset về giá trị ban đầu
func (om *OffsetManager) Reset() {
	om.offset.Value = om.config.InitialValue
	om.offset.LastCount = 0
	om.offset.HasMore = false

	// Reset HasMore cho cursor-based
	if om.config.Mode == OffsetModeCursorBased {
		om.offset.HasMore = true
	}
}
