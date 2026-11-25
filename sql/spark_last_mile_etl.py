"""PySpark SQL reference transformation matching the SQLite star-schema lane capacity mart."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_last_mile_lane_mart(spark: SparkSession, input_dir: str, output_dir: str) -> None:
    orders = spark.read.csv(f"{input_dir}/olist_orders_dataset.csv", header=True, inferSchema=True)
    items = spark.read.csv(f"{input_dir}/olist_order_items_dataset.csv", header=True, inferSchema=True)
    sellers = spark.read.csv(f"{input_dir}/olist_sellers_dataset.csv", header=True, inferSchema=True)
    customers = spark.read.csv(f"{input_dir}/olist_customers_dataset.csv", header=True, inferSchema=True)

    items_agg = items.groupBy("order_id").agg(
        F.first("seller_id").alias("seller_id"),
        F.count("*").alias("item_count"),
        F.sum("price").alias("order_gmv"),
        F.sum("freight_value").alias("freight_cost"),
    )

    clean = (
        orders.filter(F.col("order_status") == "delivered")
        .join(items_agg, "order_id", "inner")
        .join(sellers.select("seller_id", "seller_state"), "seller_id", "inner")
        .join(customers.select("customer_id", "customer_state"), "customer_id", "inner")
        .filter(
            F.col("order_delivered_carrier_date").isNotNull()
            & F.col("order_delivered_customer_date").isNotNull()
            & (F.col("order_delivered_carrier_date") >= F.col("order_purchase_timestamp"))
            & (F.col("order_delivered_customer_date") >= F.col("order_delivered_carrier_date"))
        )
        .withColumn("lane_id", F.concat_ws("->", F.col("seller_state"), F.col("customer_state")))
        .withColumn(
            "last_mile_days",
            (F.unix_timestamp("order_delivered_customer_date") - F.unix_timestamp("order_delivered_carrier_date")) / 86400.0,
        )
        .withColumn(
            "is_late",
            F.when(F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date"), 1).otherwise(0),
        )
    )

    w = Window.orderBy(F.desc("late_rate_pct"), F.desc("shipment_count"))
    lane_mart = (
        clean.groupBy("lane_id", "seller_state", "customer_state")
        .agg(
            F.count("*").alias("shipment_count"),
            F.round(F.avg("last_mile_days"), 3).alias("avg_last_mile_days"),
            F.sum("is_late").alias("late_shipments"),
            F.round(100.0 * F.sum("is_late") / F.count("*"), 3).alias("late_rate_pct"),
        )
        .filter(F.col("shipment_count") >= 50)
        .withColumn("escalation_priority_rank", F.rank().over(w))
    )
    lane_mart.write.mode("overwrite").partitionBy("seller_state").parquet(f"{output_dir}/lane_capacity_mart")
