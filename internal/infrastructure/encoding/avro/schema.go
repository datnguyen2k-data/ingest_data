package avro

// PancakeOrderSchema is the Avro schema for Pancake orders
// Note: We use a "string" type for many fields (like ID) to be safe with JSON numbers/strings mixing.
// We also use "union" types ["null", "type"] for optional fields.
const PancakeOrderSchema = `{
	"type": "record",
	"name": "PancakeOrder",
	"namespace": "com.pancake.order",
	"fields": [
		{"name": "id", "type": ["null", "string"], "default": null},
		{"name": "shop_id", "type": ["null", "string", "long"], "default": null},
		{"name": "bill_full_name", "type": ["null", "string"], "default": null},
		{"name": "bill_phone_number", "type": ["null", "string"], "default": null},
		{"name": "inserted_at", "type": ["null", "string"], "default": null},
		{"name": "updated_at", "type": ["null", "string"], "default": null},
		
		{"name": "total_quantity", "type": ["null", "long", "double"], "default": null},
		{"name": "total_cost", "type": ["null", "long", "double"], "default": null},
		{"name": "total_amount", "type": ["null", "long", "double"], "default": null},
		{"name": "total_discount", "type": ["null", "long", "double"], "default": null},
		{"name": "final_amount", "type": ["null", "long", "double"], "default": null},
		
		{"name": "items", "type": ["null", {
			"type": "array",
			"items": {
				"type": "record",
				"name": "OrderItem",
				"fields": [
					{"name": "id", "type": ["null", "string"], "default": null},
					{"name": "variation_id", "type": ["null", "string"], "default": null},
					{"name": "product_id", "type": ["null", "string"], "default": null},
					{"name": "quantity", "type": ["null", "long", "double"], "default": null},
					{"name": "price", "type": ["null", "long", "double"], "default": null},
					{"name": "retail_price", "type": ["null", "long", "double"], "default": null},
					{"name": "original_price", "type": ["null", "long", "double"], "default": null},
					{"name": "name", "type": ["null", "string"], "default": null},
					{"name": "is_combo", "type": ["null", "boolean"], "default": null}
				]
			}
		}], "default": null},

		{"name": "status", "type": ["null", "string"], "default": null},
		{"name": "is_pre_order", "type": ["null", "boolean"], "default": null},
		{"name": "note", "type": ["null", "string"], "default": null},
		
		{"name": "shipping_fee", "type": ["null", "long", "double"], "default": null},
		{"name": "cod_fee", "type": ["null", "long", "double"], "default": null},
		{"name": "partner_fee", "type": ["null", "long", "double"], "default": null},
		
		{"name": "bill_address", "type": ["null", "string"], "default": null},
		{"name": "bill_district_name", "type": ["null", "string"], "default": null},
		{"name": "bill_province_name", "type": ["null", "string"], "default": null},
		{"name": "bill_ward_name", "type": ["null", "string"], "default": null}
	]
}`
