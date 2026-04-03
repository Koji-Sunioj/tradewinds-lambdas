import sys
from awsglue.utils import getResolvedOptions

import json
import base64
import pyspark.sql.functions as F

from awsglue.job import Job
from awsglue.context import GlueContext
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["db_analyst", "etl_bucket"])

db_params = args["db_analyst"]
print(db_params)
encoded = db_params.encode("utf-8")
db_name, user, password, port = base64.b64decode(encoded).decode("utf-8").split(",")

def get_sales(db_name, username, password):
    sales_frame = spark.read.jdbc(
        url="jdbc:postgresql://aws-1-eu-north-1.pooler.supabase.com:6543/%s" % db_name,
        table="(select * from get_orders()) sales",
        properties={
            "user": "%s.ljqjiwbxuqrihfepogdg" % username,
            "password": password,
            "driver": "org.postgresql.Driver",
        },
    )

    with_categories_formatted = sales_frame.withColumn(
        "category_name", F.regexp_replace("category_name", "/|\s", "_")
    )
    return with_categories_formatted


def week_total_sales(frame):
    grouped = (
        frame.groupby("week")
        .agg({"sale": "sum"})
        .withColumnRenamed("sum(sale)", "sales")
    )
    return (
        grouped.orderBy("week")
        .toPandas()
        .astype({"sales": float})
        .to_dict(orient="records")
    )


def week_category_perc(frame):
    pivoted = frame.groupBy("week").pivot("category_name").sum("sale")
    categories = [
        row.category_name for row in frame.select("category_name").distinct().collect()
    ]
    summed = pivoted.withColumn("sum", F.expr(" + ".join(categories)))

    rounded_map = {
        category: F.expr("round(%s,2)" % category) for category in categories
    }
    rounded_map["sum"] = F.expr("round(sum,2)")
    category_share = summed.withColumns(rounded_map)

    sum_map = {
        category: F.expr("(%s / sum) * 100" % category) for category in categories
    }
    float_map = {category: float for category in categories}

    with_percentage = category_share.withColumns(sum_map).orderBy("week").drop("sum")
    return (
        with_percentage.toPandas().astype(float_map).round(2).to_dict(orient="records")
    )


def mean_sale_per_order(frame):
    orders = (
        frame.groupby("week", "order_id")
        .agg({"sale": "sum"})
        .withColumnRenamed("sum(sale)", "sales")
    )
    mean_sales = (
        orders.groupby("week")
        .agg({"sales": "mean"})
        .withColumnRenamed("avg(sales)", "mean_sale_per_order")
        .orderBy("week")
    )
    return (
        mean_sales.toPandas()
        .astype({"mean_sale_per_order": "float"})
        .round(2)
        .to_dict(orient="records")
    )


def category_employee_sales(frame):
    grouped = (
        frame.groupby("seller", "category_name")
        .agg({"sale": "sum"})
        .withColumnRenamed("sum(sale)", "sales")
    )
    employee_sales = grouped.groupby("category_name").pivot("seller").sum("sales")

    float_map = {
        column: float for column in employee_sales.columns if "category" not in column
    }
    return (
        employee_sales.toPandas().astype(float_map).fillna(0).to_dict(orient="records")
    )


def top_customers(frame):
    customer_sales = (
        frame.groupby("customer_name")
        .sum("sale")
        .withColumnRenamed("sum(sale)", "sales")
    )

    top_ten_customers = customer_sales.orderBy("sales", ascending=False).limit(10)
    return (
        top_ten_customers.toPandas().astype({"sales": float}).to_dict(orient="records")
    )


def top_products_w_category(frame):
    top_products = (
        frame.groupby("category_name", "product_name")
        .agg({"sale": "sum"})
        .withColumnRenamed("sum(sale)", "sales")
        .orderBy("sales", ascending=False)
        .limit(10)
    )

    return top_products.toPandas().astype({"sales": "float"}).to_dict(orient="records")


def write_json(dictionary, bucket):
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Body=json.dumps(dictionary),
        Key="app_data/sales.json",
        ACL="public-read",
    )

def country_sales(frame):
    last_two_weeks = [
        row.week
        for row in frame.select("week")
        .distinct()
        .orderBy("week", ascending=False)
        .collect()
    ][:2]
    this_week, last_week = last_two_weeks[0], last_two_weeks[1]
    last_two_weeks = (
        frame[frame.week.isin(last_two_weeks)]
        .groupBy("customer_country")
        .pivot("week")
        .sum("sale")
    )

    countries = last_two_weeks.na.fill(0).withColumn(
        "week_change", F.col(this_week) - F.col(last_week)
    )
    float_map = {
        column: float for column in countries.columns if "country" not in column
    }
    return countries.toPandas().astype(float_map).to_dict(orient="records")


sc = SparkContext.getOrCreate()
sc.setLogLevel("FATAL")
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

sales_frame = get_sales(db_name,username,password)

top_ten_products = top_products_w_category(sales_frame)
top_ten_customers = top_customers(sales_frame)
week_sales = week_total_sales(sales_frame)
categories_weekly_share = week_category_perc(sales_frame)
mean_sales = mean_sale_per_order(sales_frame)
heatmap = category_employee_sales(sales_frame)
countries = country_sales(sales_frame)


write_object = {
    "top_ten_products": top_ten_products,
    "to_ten_customers": top_ten_customers,
    "weekly_sales": week_sales,
    "categories_weekly_share": categories_weekly_share,
    "mean_sale_per_order_week": mean_sales,
    "employee_sales_per_category": heatmap,
    "country_sales": countries,
}

write_json(write_object, args["etl_bucket"])
