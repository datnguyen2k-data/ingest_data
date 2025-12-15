package order

import "time"

type Order struct {
	ID         string
	CustomerID string
	ProductID  string
	Quantity   int
	Price      float64
	Status     string
	CreatedAt  time.Time
}

func NewOrder(id, customerID, productID string, quantity int, price float64) (*Order, error) {
	if id == "" || customerID == "" || productID == "" {
		return nil, ErrMissingField
	}
	if quantity <= 0 {
		return nil, ErrInvalidQuantity
	}
	if price <= 0 {
		return nil, ErrInvalidPrice
	}

	return &Order{
		ID:         id,
		CustomerID: customerID,
		ProductID:  productID,
		Quantity:   quantity,
		Price:      price,
		Status:     "pending",
		CreatedAt:  time.Now().UTC(),
	}, nil
}
