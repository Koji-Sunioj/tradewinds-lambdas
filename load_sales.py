import os
import boto3
import random
import base64
import psycopg2
import psycopg2.extras
import traceback

from zoneinfo import ZoneInfo
from psycopg2.extras import RealDictRow
from datetime import datetime, timedelta

db_params = os.environ.get("DB_ADMIN")
encoded = db_params.encode("utf-8")
db_name,user,password,port = base64.b64decode(encoded).decode("utf-8").split(",")

def handler(event, context):
    today = datetime.utcnow()
    helsinki = ZoneInfo('Europe/Helsinki')
    offset = helsinki.utcoffset(today).seconds / 3600
    helsinki_today  = (today + timedelta(hours=offset)).strftime("%Y-%m-%d")

    connect_pooler = "postgres://%s.ljqjiwbxuqrihfepogdg:%s@aws-1-eu-north-1.pooler.supabase.com:%s/%s" % (user,password,port,db_name)
    conn = psycopg2.connect(connect_pooler)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    id_select = "select \
        (select json_agg(customer_id) from customers) as customers,\
        (select json_agg(shipper_id) from shippers) as shippers,\
        (select json_agg(employee_id) from employees) as employees,\
        (select json_agg(product_id) from products) as products;"

    cursor.execute(id_select)
    table_ids = cursor.fetchone()

    new_orders = []

    for n in range(random.randint(1,10)):
        customer = random.choice(table_ids["customers"])
        employee = random.choice(table_ids["employees"])
        shipper = random.choice(table_ids["shippers"])
        new_orders.append({"customer":customer,"employee":employee,"date":helsinki_today,"shipper":shipper})

    try:
        init_matrix = [list(array.values()) for array in new_orders]
        new_orders_matrix = [list(n) for n in zip(*init_matrix)]
        cursor.callproc("insert_orders", (*new_orders_matrix,))
        conn.commit()
        print("rows inserted on table 'orders': %s" % cursor.rowcount)
        order_ids = cursor.fetchall()

        weights = []
        random_quantities = [*range(1,101,1)]

        for n in range(10,0,-1): 
            something = [n] * 10
            weights.extend(something)

        new_ids = []
        new_quantities = []
        new_products = []

        for new_order in order_ids:
            n_products = random.randint(1,10)
            new_ids.extend([new_order["insert_orders"]] * n_products)
            new_quantities.extend(random.choices(random_quantities,weights=weights,k=n_products))
            new_products.extend(random.choices(table_ids["products"],k=n_products))

        cursor.callproc('insert_order_details',(new_ids,new_quantities,new_products))
        conn.commit()
        print("rows inserted on table 'order_details': %s" % cursor.rowcount)
        conn.close()
        
        
    except Exception as error:
        print(traceback.format_exc())
        conn.rollback()
