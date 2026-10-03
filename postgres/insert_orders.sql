create function insert_orders(in customer_ids int[],in employee_ids int[],
    in order_dates text[],in shipper_ids int[]) returns setof int as  
$$
    insert into orders (customer_id,employee_id,order_date,shipper_id)
    select unnest($1),unnest($2),unnest($3)::date,unnest($4) returning order_id  
$$ language sql;
