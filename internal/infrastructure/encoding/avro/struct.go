package avro

// PancakeOrderStruct matches the Avro schema defined in schema.go
// Confluent Avro Serializer uses struct tags to map fields
type PancakeOrderStruct struct {
	ID              *string `avro:"id"`
	ShopID          *int64  `avro:"shop_id"` // Simplified to long for Union ["null", "string", "long"] handling - serializer might need custom logic or we force int64
	BillFullName    *string `avro:"bill_full_name"`
	BillPhoneNumber *string `avro:"bill_phone_number"`
	InsertedAt      *string `avro:"inserted_at"`
	UpdatedAt       *string `avro:"updated_at"`

	TotalQuantity *float64 `avro:"total_quantity"`
	TotalCost     *float64 `avro:"total_cost"`
	TotalAmount   *float64 `avro:"total_amount"`
	TotalDiscount *float64 `avro:"total_discount"`
	FinalAmount   *float64 `avro:"final_amount"`

	ShippingFee *float64 `avro:"shipping_fee"`
	CodFee      *float64 `avro:"cod_fee"`
	PartnerFee  *float64 `avro:"partner_fee"`

	Status     *string `avro:"status"`
	IsPreOrder *bool   `avro:"is_pre_order"`
	Note       *string `avro:"note"`

	BillAddress  *string `avro:"bill_address"`
	BillDistrict *string `avro:"bill_district_name"`
	BillProvince *string `avro:"bill_province_name"`
	BillWard     *string `avro:"bill_ward_name"`

	Items []OrderItemStruct `avro:"items"`
}

type OrderItemStruct struct {
	ID            *string  `avro:"id"`
	VariationID   *string  `avro:"variation_id"`
	ProductID     *string  `avro:"product_id"`
	Quantity      *float64 `avro:"quantity"`
	Price         *float64 `avro:"price"`
	RetailPrice   *float64 `avro:"retail_price"`
	OriginalPrice *float64 `avro:"original_price"`
	Name          *string  `avro:"name"`
	IsCombo       *bool    `avro:"is_combo"`
}
