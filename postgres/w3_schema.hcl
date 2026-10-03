table "categories" {
  schema = schema.public
  column "category_id" {
    null           = false
    type           = serial
  }
  column "category_name" {
    null = true
    type = varchar(255)
  }
  column "description" {
    null = true
    type = varchar(255)
  }
  primary_key {
    columns = [column.category_id]
  }
}
table "customers" {
  schema = schema.public
  column "customer_id" {
    null           = false
    type           = serial
  }
  column "customer_name" {
    null = true
    type = varchar(255)
  }
  column "contact_name" {
    null = true
    type = varchar(255)
  }
  column "address" {
    null = true
    type = varchar(255)
  }
  column "city" {
    null = true
    type = varchar(255)
  }
  column "postal_code" {
    null = true
    type = varchar(255)
  }
  column "country" {
    null = true
    type = varchar(255)
  }
  primary_key {
    columns = [column.customer_id]
  }
}
table "employees" {
  schema = schema.public
  column "employee_id" {
    null           = false
    type           = serial
  }
  column "last_name" {
    null = true
    type = varchar(255)
  }
  column "first_name" {
    null = true
    type = varchar(255)
  }
  column "birth_date" {
    null = true
    type = date
  }
  column "photo" {
    null = true
    type = varchar(255)
  }
  column "notes" {
    null = true
    type = text
  }
  primary_key {
    columns = [column.employee_id]
  }
}
table "order_details" {
  schema = schema.public
  column "order_detail_id" {
    null           = false
    type           = serial
  }
  column "order_id" {
    null = true
    type = int
  }
  column "product_id" {
    null = true
    type = int
  }
  column "quantity" {
    null = true
    type = int
  }
  primary_key {
    columns = [column.order_detail_id]
  }
  foreign_key "order_details_ibfk_1" {
    columns     = [column.order_id]
    ref_columns = [table.orders.column.order_id]
    on_update   = NO_ACTION
    on_delete   = NO_ACTION
  }
  foreign_key "order_details_ibfk_2" {
    columns     = [column.product_id]
    ref_columns = [table.products.column.product_id]
    on_update   = NO_ACTION
    on_delete   = NO_ACTION
  }
  index "order_id" {
    columns = [column.order_id]
  }
  index "product_id" {
    columns = [column.product_id]
  }
}
table "orders" {
  schema = schema.public
  column "order_id" {
    null           = false
    type           = serial
  }
  column "customer_id" {
    null = true
    type = int
  }
  column "employee_id" {
    null = true
    type = int
  }
  column "order_date" {
    null = true
    type = date
  }
  column "shipper_id" {
    null = true
    type = int
  }
  primary_key {
    columns = [column.order_id]
  }
  foreign_key "orders_ibfk_1" {
    columns     = [column.customer_id]
    ref_columns = [table.customers.column.customer_id]
    on_update   = NO_ACTION
    on_delete   = NO_ACTION
  }
  foreign_key "orders_ibfk_2" {
    columns     = [column.employee_id]
    ref_columns = [table.employees.column.employee_id]
    on_update   = NO_ACTION
    on_delete   = NO_ACTION
  }
  foreign_key "orders_ibfk_3" {
    columns     = [column.shipper_id]
    ref_columns = [table.shippers.column.shipper_id]
    on_update   = NO_ACTION
    on_delete   = NO_ACTION
  }
  index "CustomerID" {
    columns = [column.customer_id]
  }
  index "EmployeeID" {
    columns = [column.employee_id]
  }
  index "ShipperID" {
    columns = [column.shipper_id]
  }
}
table "products" {
  schema = schema.public
  column "product_id" {
    null           = false
    type           = serial
  }
  column "product_name" {
    null = true
    type = varchar(255)
  }
  column "supplier_id" {
    null = true
    type = int
  }
  column "category_id" {
    null = true
    type = int
  }
  column "unit" {
    null = true
    type = varchar(255)
  }
  column "price" {
    null = true
    type = numeric
  }
  primary_key {
    columns = [column.product_id]
  }
  foreign_key "products_ibfk_1" {
    columns     = [column.category_id]
    ref_columns = [table.categories.column.category_id]
    on_update   = NO_ACTION
    on_delete   = NO_ACTION
  }
  foreign_key "products_ibfk_2" {
    columns     = [column.supplier_id]
    ref_columns = [table.suppliers.column.supplier_id]
    on_update   = NO_ACTION
    on_delete   = NO_ACTION
  }
  index "CategoryID" {
    columns = [column.category_id]
  }
  index "SupplierID" {
    columns = [column.supplier_id]
  }
}
table "shippers" {
  schema = schema.public
  column "shipper_id" {
    null           = false
    type           = serial
  }
  column "shipper_name" {
    null = true
    type = varchar(255)
  }
  column "phone" {
    null = true
    type = varchar(255)
  }
  primary_key {
    columns = [column.shipper_id]
  }
}
table "suppliers" {
  schema = schema.public
  column "supplier_id" {
    null           = false
    type           = serial
  }
  column "supplier_name" {
    null = true
    type = varchar(255)
  }
  column "contact_name" {
    null = true
    type = varchar(255)
  }
  column "address" {
    null = true
    type = varchar(255)
  }
  column "city" {
    null = true
    type = varchar(255)
  }
  column "postal_code" {
    null = true
    type = varchar(255)
  }
  column "country" {
    null = true
    type = varchar(255)
  }
  column "phone" {
    null = true
    type = varchar(255)
  }
  primary_key {
    columns = [column.supplier_id]
  }
}

schema "public" {
  comment = "standard public schema"
}
