import pyspark.sql.functions as F


def week_total_sales(frame):
    grouped = (
        frame.groupby("week")
        .agg({"sale": "sum"})
        .withColumnRenamed("sum(sale)", "sales")
    )
    return (
        grouped.orderBy("week")
        .toPandas()
        .astype({"sales": float, "week": str})
        .to_dict(orient="records")
    )


def week_category_perc(frame):
    pivoted = frame.groupBy("week").pivot(
        "category_name").sum("sale").fillna(0)
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
    dict_map = {category: float for category in categories}
    dict_map["week"] = str

    with_percentage = category_share.withColumns(
        sum_map).orderBy("week").drop("sum")
    return with_percentage.toPandas().astype(dict_map).round(2).to_dict(orient="list")


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
        .astype({"mean_sale_per_order": "float", "week": str})
        .round(2)
        .to_dict(orient="records")
    )


def category_employee_sales(frame):
    grouped = (
        frame.groupby("seller", "category_name")
        .agg({"sale": "sum"})
        .withColumnRenamed("sum(sale)", "sales")
    )
    employee_sales = grouped.groupby(
        "category_name").pivot("seller").sum("sales")

    float_map = {
        column: float for column in employee_sales.columns if "category" not in column
    }
    employee_frame = (
        employee_sales.toPandas()
        .astype(float_map)
        .fillna(0)
        .sort_values("category_name")
    )

    employees = [
        col for col in employee_frame.columns if col != "category_name"]
    categories = employee_frame["category_name"].to_list()

    matrix = []
    for x, row in enumerate(employee_frame[employees].T.values):
        for y, value in enumerate(row):
            matrix.append([x, y, value])

    return {"categories": categories, "employees": employees, "matrix": matrix}


def top_customers(frame):
    customer_sales = (
        frame.groupby("customer_name")
        .sum("sale")
        .withColumnRenamed("sum(sale)", "sales")
    )

    top_ten_customers = customer_sales.orderBy(
        "sales", ascending=False).limit(10)
    return (
        top_ten_customers.toPandas().astype(
            {"sales": float}).to_dict(orient="records")
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


def country_sales(frame):
    iso_map = {
        "argentina": "ar",
        "spain": "es",
        "switzerland": "ch",
        "italy": "it",
        "venezuela": "ve",
        "belgium": "be",
        "norway": "no",
        "sweden": "se",
        "usa": "us",
        "france": "fr",
        "mexico": "mx",
        "brazil": "br",
        "austria": "at",
        "poland": "pl",
        "uk": "gb",
        "ireland": "ie",
        "germany": "de",
        "denmark": "dk",
        "canada": "ca",
        "finland": "fi",
        "portugal": "pt",
    }
    date_format = "%Y-%m-%d"

    last_two_weeks = [
        row.week
        for row in frame.select("week")
        .distinct()
        .orderBy("week", ascending=False)
        .collect()
    ][:2]
    this_week = last_two_weeks[0].strftime(date_format)
    last_week = last_two_weeks[1].strftime(date_format)
    two_week_sales = (
        frame[frame.week.isin([this_week, last_week])]
        .groupBy("customer_country")
        .pivot("week")
        .sum("sale")
    )

    with_iso = two_week_sales.withColumn(
        "customer_country", F.lower(F.col("customer_country"))
    ).replace(to_replace=iso_map, subset="customer_country")
    countries = with_iso.na.fill(0).withColumn(
        "week_change", F.col(this_week) - F.col(last_week)
    )
    float_map = {
        column: float for column in countries.columns if "country" not in column
    }
    formatted_frame = countries.toPandas().astype(float_map)

    current_sales = formatted_frame[[
        "customer_country", this_week]].values.tolist()
    change_sales = formatted_frame[[
        "customer_country", "week_change"]].values.tolist()
    return {"sales": current_sales, "week-change": change_sales, "period": this_week}

