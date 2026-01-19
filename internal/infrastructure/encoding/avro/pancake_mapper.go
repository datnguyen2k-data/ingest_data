package avro

import (
	"fmt"
)

// ToPancakeOrderNative converts a raw JSON map to a structure compatible with goavro Union requirements.
// goavro requires Union values to be wrapped in map[string]interface{}{"type": value}.
func ToPancakeOrderNative(data map[string]interface{}) (map[string]interface{}, error) {
	out := make(map[string]interface{})

	// Helper to set optional string field
	setString := func(key string) {
		if v, ok := data[key]; ok && v != nil {
			out[key] = map[string]interface{}{"string": fmt.Sprintf("%v", v)}
		} else {
			out[key] = nil
		}
	}

	// Helper to set optional boolean field
	setBool := func(key string) {
		if v, ok := data[key]; ok && v != nil {
			if b, ok := v.(bool); ok {
				out[key] = map[string]interface{}{"boolean": b}
			}
		} else {
			out[key] = nil
		}
	}

	// Fields mapping
	setString("id")

	// shop_id is ["null", "string", "long"]
	if v, ok := data["shop_id"]; ok && v != nil {
		// Detect type
		switch v.(type) {
		case string:
			out["shop_id"] = map[string]interface{}{"string": v}
		case float64, int, int64:
			// Treat as long
			var val int64
			switch t := v.(type) {
			case float64:
				val = int64(t)
			case int:
				val = int64(t)
			case int64:
				val = t
			}
			out["shop_id"] = map[string]interface{}{"long": val}
		default:
			// Fallback to string representation
			out["shop_id"] = map[string]interface{}{"string": fmt.Sprintf("%v", v)}
		}
	} else {
		out["shop_id"] = nil
	}

	setString("bill_full_name")
	setString("bill_phone_number")
	setString("inserted_at")
	setString("updated_at")

	// Financials -> ["null", "long", "double"]. Prefer double for safety if float, but schema says long/double union?
	// Schema: {"name": "total_quantity", "type": ["null", "long", "double"], "default": null}
	// We check if it has decimal point? Or just prefer double?
	// Let's prefer 'double' to be safe with currency/quantities in JSON
	setNumberUnion := func(key string) {
		if v, ok := data[key]; ok && v != nil {
			// Always wrap as double for simplicity if supported?
			// Schema has "long" AND "double".
			// If we wrap as "double", it matches one of them.
			var val float64
			switch t := v.(type) {
			case float64:
				val = t
			case int:
				val = float64(t)
			}
			out[key] = map[string]interface{}{"double": val}
		} else {
			out[key] = nil
		}
	}

	setNumberUnion("total_quantity")
	setNumberUnion("total_cost")
	setNumberUnion("total_amount")
	setNumberUnion("total_discount")
	setNumberUnion("final_amount")

	setNumberUnion("shipping_fee")
	setNumberUnion("cod_fee")
	setNumberUnion("partner_fee")

	setString("status")
	setBool("is_pre_order")
	setString("note")

	setString("bill_address")
	setString("bill_district_name")
	setString("bill_province_name")
	setString("bill_ward_name")

	// Array items
	if v, ok := data["items"]; ok && v != nil {
		if rawItems, ok := v.([]interface{}); ok {
			mappedItems := make([]interface{}, 0, len(rawItems))
			for _, ri := range rawItems {
				if rMap, ok := ri.(map[string]interface{}); ok {
					mItem := make(map[string]interface{})
					// Map item fields
					// Simple helper within loop
					setItemString := func(k string) {
						if val, exists := rMap[k]; exists && val != nil {
							mItem[k] = map[string]interface{}{"string": fmt.Sprintf("%v", val)}
						} else {
							mItem[k] = nil
						}
					}
					setItemNumber := func(k string) {
						if val, exists := rMap[k]; exists && val != nil {
							var dv float64
							switch t := val.(type) {
							case float64:
								dv = t
							case int:
								dv = float64(t)
							}
							mItem[k] = map[string]interface{}{"double": dv}
						} else {
							mItem[k] = nil
						}
					}

					setItemString("id")
					setItemString("variation_id")
					setItemString("product_id")
					setItemNumber("quantity")
					setItemNumber("price")
					setItemNumber("retail_price")
					setItemNumber("original_price")
					setItemString("name")
					if val, exists := rMap["is_combo"]; exists && val != nil {
						if b, ok := val.(bool); ok {
							mItem["is_combo"] = map[string]interface{}{"boolean": b}
						}
					} else {
						mItem["is_combo"] = nil
					}

					mappedItems = append(mappedItems, mItem)
				}
			}
			// items is ["null", {"type": "array", ...}]
			// So wrap array in "array" isn't needed!
			// Wait, Union is ["null", {"type": "array"...}]
			// The TYPE of the 2nd option is "array".
			// goavro: "A map[string]interface{} with a single key matching the Avro type name"
			// The type name of an array is "array".
			out["items"] = map[string]interface{}{"array": mappedItems}
		} else {
			out["items"] = nil
		}
	} else {
		out["items"] = nil
	}

	return out, nil
}

// ToPancakeOrderStruct converts a raw JSON map to the specific PancakeOrderStruct
func ToPancakeOrderStruct(data map[string]interface{}) (*PancakeOrderStruct, error) {
	out := &PancakeOrderStruct{}

	getString := func(key string) *string {
		if v, ok := data[key]; ok && v != nil {
			s := fmt.Sprintf("%v", v)
			return &s
		}
		return nil
	}

	getFloat := func(key string) *float64 {
		if v, ok := data[key]; ok && v != nil {
			var val float64
			switch t := v.(type) {
			case float64:
				val = t
			case int:
				val = float64(t)
			case int64:
				val = float64(t)
			default:
				val = 0.0
			}
			return &val
		}
		return nil
	}

	getInt64 := func(key string) *int64 {
		if v, ok := data[key]; ok && v != nil {
			var val int64
			switch t := v.(type) {
			case float64:
				val = int64(t)
			case int:
				val = int64(t)
			case int64:
				val = t
			}
			return &val
		}
		return nil
	}

	getBool := func(key string) *bool {
		if v, ok := data[key]; ok && v != nil {
			if b, ok := v.(bool); ok {
				return &b
			}
		}
		return nil
	}

	out.ID = getString("id")
	out.ShopID = getInt64("shop_id") // Best effort for shop_id
	out.BillFullName = getString("bill_full_name")
	out.BillPhoneNumber = getString("bill_phone_number")
	out.InsertedAt = getString("inserted_at")
	out.UpdatedAt = getString("updated_at")

	out.TotalQuantity = getFloat("total_quantity")
	out.TotalCost = getFloat("total_cost")
	out.TotalAmount = getFloat("total_amount")
	out.TotalDiscount = getFloat("total_discount")
	out.FinalAmount = getFloat("final_amount")

	out.ShippingFee = getFloat("shipping_fee")
	out.CodFee = getFloat("cod_fee")
	out.PartnerFee = getFloat("partner_fee")

	out.Status = getString("status")
	out.IsPreOrder = getBool("is_pre_order")
	out.Note = getString("note")

	out.BillAddress = getString("bill_address")
	out.BillDistrict = getString("bill_district_name")
	out.BillProvince = getString("bill_province_name")
	out.BillWard = getString("bill_ward_name")

	if v, ok := data["items"]; ok && v != nil {
		if rawItems, ok := v.([]interface{}); ok {
			out.Items = make([]OrderItemStruct, 0, len(rawItems))
			for _, ri := range rawItems {
				if rMap, ok := ri.(map[string]interface{}); ok {
					item := OrderItemStruct{
						ID:            nil,
						VariationID:   nil,
						ProductID:     nil,
						Quantity:      nil,
						Price:         nil,
						RetailPrice:   nil,
						OriginalPrice: nil,
						Name:          nil,
						IsCombo:       nil,
					}

					// Helpers just for item scope
					getItemString := func(k string) *string {
						if val, exists := rMap[k]; exists && val != nil {
							s := fmt.Sprintf("%v", val)
							return &s
						}
						return nil
					}
					getItemFloat := func(k string) *float64 {
						if val, exists := rMap[k]; exists && val != nil {
							var f float64
							switch t := val.(type) {
							case float64:
								f = t
							case int:
								f = float64(t)
							}
							return &f
						}
						return nil
					}

					item.ID = getItemString("id")
					item.VariationID = getItemString("variation_id")
					item.ProductID = getItemString("product_id")
					item.Quantity = getItemFloat("quantity")
					item.Price = getItemFloat("price")
					item.RetailPrice = getItemFloat("retail_price")
					item.OriginalPrice = getItemFloat("original_price")
					item.Name = getItemString("name")

					if val, exists := rMap["is_combo"]; exists && val != nil {
						if b, ok := val.(bool); ok {
							item.IsCombo = &b
						}
					}

					out.Items = append(out.Items, item)
				}
			}
		}
	}

	return out, nil
}
