create function insert_order_details(in order_ids int[],in quantitites int[],
    in product_ids int[]) returns setof int as  
$$
    insert into order_details (order_id,quantity,product_id)
    select unnest($1),unnest($2),unnest($3) returning order_detail_id;  
$$ language sql;
