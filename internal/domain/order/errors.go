package order

import "errors"

var (
	ErrInvalidQuantity = errors.New("quantity must be greater than zero")
	ErrInvalidPrice    = errors.New("price must be greater than zero")
	ErrMissingField    = errors.New("required field is missing")
)
