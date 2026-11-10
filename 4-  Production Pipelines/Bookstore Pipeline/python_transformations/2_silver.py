from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table
@dp.expect_or_drop("positive_quantity", "quantity > 0")
@dp.expect_or_fail("valid_customer", "f_name IS NOT NULL AND l_name IS NOT NULL")
@dp.expect("recent_order", "order_timestamp >= '2022-07-15'")
def orders_cleaned():
    orders_df  = spark.readStream.table("orders_raw")
    customers_df = spark.read.table("customers")

    return (orders_df.join(customers_df, "customer_id", "left")
                     .withColumn("order_timestamp", F.to_timestamp(F.col("order_timestamp")))
                     .withColumn("f_name", F.expr("profile:first_name"))
                     .withColumn("l_name", F.expr("profile:last_name"))
                     .withColumn("country", F.expr("profile:address:country"))
                     .select("order_id", "quantity", "customer_id", "order_timestamp", "f_name", "l_name", "books", "country")
            )