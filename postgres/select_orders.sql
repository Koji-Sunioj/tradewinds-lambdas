create function get_orders (in max_superbase_order_id int)
	returns table (
		order_id int,
		product_id int,
		product_name varchar,
	    sale numeric,
   		price numeric,
		quantity int,
		week date,
		category_name varchar,
		customer_name varchar,
		customer_city varchar,
		customer_country varchar,
		seller text,
		shipper_name varchar,
		supplier_name varchar
	)
language plpgsql
as $$
begin
	if max_superbase_order_id is null then 
		return query
		select 
			orders.order_id,
			order_details.product_id,
			products.product_name,
			products.price * order_details.quantity sale,
			products.price,
			order_details.quantity,	 
			orders.order_date - (to_char(orders.order_date, 'ID')::int - 1) week,
			categories.category_name,
			customers.customer_name,
			customers.city customer_city,
			customers.country customer_country,
			employees.first_name || ' ' ||  employees.last_name seller,
			shippers.shipper_name,
			suppliers.supplier_name
		from orders
			join order_details on 
			order_details.order_id = orders.order_id
			join customers on 
			customers.customer_id = orders.customer_id
			join employees on 
			employees.employee_id = orders.employee_id
			join shippers on 
			shippers.shipper_id = orders.shipper_id
			join products on
			products.product_id = order_details.product_id
			join categories on 
			categories.category_id = products.category_id
			join suppliers on
			suppliers.supplier_id = products.supplier_id
		order by orders.order_date,orders.order_id;
	elseif max_superbase_order_id > 0 then
		return query
		select 
			orders.order_id,
			order_details.product_id,
			products.product_name,
				products.price * order_details.quantity sale,
			products.price,
			order_details.quantity,	 
			orders.order_date - (to_char(orders.order_date, 'ID')::int - 1) week,
			categories.category_name,
			customers.customer_name,
			customers.city customer_city,
			customers.country customer_country,
			employees.first_name || ' ' ||  employees.last_name seller,
			shippers.shipper_name,
			suppliers.supplier_name
		from orders
			join order_details on 
			order_details.order_id = orders.order_id
			join customers on 
			customers.customer_id = orders.customer_id
			join employees on 
			employees.employee_id = orders.employee_id
			join shippers on 
			shippers.shipper_id = orders.shipper_id
			join products on
			products.product_id = order_details.product_id
			join categories on 
			categories.category_id = products.category_id
			join suppliers on
			suppliers.supplier_id = products.supplier_id
		where orders.order_id > max_superbase_order_id
		order by orders.order_date,orders.order_id;
	end if;
end;
$$;

