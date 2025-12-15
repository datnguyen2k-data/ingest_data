package order

import "errors"
// OrderStatus defines basic lifecycle labels.
const (
	StatusPending  = "pending"
	StatusAccepted = "accepted"
	StatusFailed   = "failed"
)


type Price struct {
	value float64
}

func (p Price) Value() float64 {
	return p.value
}

func NewPrice(value float64) (Price, error) {
	if value <= 0 {
		return Price{}, errors.New("price must be greater than zero")
	}
	return Price{value: value}, nil
}