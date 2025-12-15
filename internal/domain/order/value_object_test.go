package order

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPrice(t *testing.T) {
	price, err := NewPrice(100)

	assert.NoError(t, err)
	assert.Equal(t, float64(100), price.Value())
}

func TestNewPrice_Invalid(t *testing.T) {
	price, err := NewPrice(-1)

	assert.Error(t, err)
	assert.Equal(t, float64(0), price.Value())
}
