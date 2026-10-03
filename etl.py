import sys
import json
import boto3
import base64

from databricks import sql
from pyspark.sql import DataFrame
from pyspark.context import SparkContext

from awsglue.job import Job
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

import pyspark.sql.functions as F

from aggregates import (week_total_sales, week_category_perc, mean_sale_per_order,
                        category_employee_sales, top_customers, top_products_w_category, country_sales)

def insert_to_databricks(frame) -> str:
    insert = "insert into tradewinds.default.sales (order_id,product_id,product_name,sale,price,quantity,week, \
        category_name,customer_name,customer_city,customer_country,seller,shipper_name,supplier_name) values "
    rows = []

    for row in frame.rdd.collect():
        row_dict = row.asDict()
        sql_row = '({order_id},{product_id},"{product_name}",{sale},{price},{quantity},"{week}","{category_name}"\
            ,"{customer_name}","{customer_city}","{customer_country}","{seller}","{shipper_name}","{supplier_name}")'.format(**row_dict)
        rows.append(sql_row)

    return insert + ",\n".join(rows) + ";"


def supabase_query(db_name, username, password, query) -> DataFrame:
    return spark.read.jdbc(
        url="jdbc:postgresql://aws-1-eu-north-1.pooler.supabase.com:6543/%s" % db_name,
        table=query,
        properties={
            "user": "%s.ljqjiwbxuqrihfepogdg" % username,
            "password": password,
            "driver": "org.postgresql.Driver",
        },
    )


def write_json(dictionary, bucket):
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Body=json.dumps(dictionary),
        Key="front_end/app_data/sales.json",
    )

sc = SparkContext.getOrCreate()
sc.setLogLevel("FATAL")
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

args = getResolvedOptions(
    sys.argv, ["db_analyst", "etl_bucket", "databricks_host", "databricks_token"])
db_params, databricks_token, databricks_host = args[
    "db_analyst"], args["databricks_token"], args["databricks_host"]
encoded = db_params.encode("utf-8")
db_name, username, password, port = base64.b64decode(
    encoded).decode("utf-8").split(",")


conn = sql.connect(server_hostname=databricks_host,
                   http_path="/sql/1.0/warehouses/47594432480270a0",
                   access_token=databricks_token,
                   )
cursor = conn.cursor()
cursor.execute("select max(order_id) order_id from tradewinds.default.sales;")

databricks_max_id = cursor.fetchone()["order_id"]

supabase_max_id = supabase_query(
    db_name, username, password, "(select max(order_id) order_id from orders) something").select('order_id').collect()[0]['order_id']

databricks_updated = False

if databricks_max_id == None:
    sales_frame = supabase_query(
        db_name, username, password, "(select * from get_orders(null)) sales")

    insert_command = insert_to_databricks(sales_frame)
    cursor.execute(insert_command)
    databricks_updated = True

elif supabase_max_id > databricks_max_id:
    sales_frame = supabase_query(
        db_name, username, password, "(select * from get_orders(%s)) sales" % databricks_max_id)

    insert_command = insert_to_databricks(sales_frame)
    cursor.execute(insert_command)
    databricks_updated = True

if databricks_updated:
    select_order = "select order_id,product_id,product_name,sale,price,quantity,week,category_name,\
        customer_name,customer_city,customer_country,seller,shipper_name,supplier_name \
        from tradewinds.default.sales order by order_id,product_id;"
    cursor.execute(select_order)
    sales_orders = cursor.fetchall()

    sales_frame = spark.createDataFrame(sales_orders).withColumn(
        "category_name", F.regexp_replace("category_name", "/|\s", "_"))

    top_ten_products = top_products_w_category(sales_frame)
    top_ten_customers = top_customers(sales_frame)
    week_sales = week_total_sales(sales_frame)
    categories_weekly_share = week_category_perc(sales_frame)
    mean_sales = mean_sale_per_order(sales_frame)
    heatmap = category_employee_sales(sales_frame)
    countries = country_sales(sales_frame)

    write_object = {
        "top_ten_products": top_ten_products,
        "top_ten_customers": top_ten_customers,
        "weekly_sales": week_sales,
        "categories_weekly_share": categories_weekly_share,
        "mean_sale_per_order_week": mean_sales,
        "employee_sales_per_category": heatmap,
        "country_sales": countries,
    }

    write_json(write_object, args["etl_bucket"])
