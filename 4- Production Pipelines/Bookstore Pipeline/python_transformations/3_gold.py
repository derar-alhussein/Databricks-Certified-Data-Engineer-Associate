from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view
def cn_daily_customer_books():
    return (  spark.read.table("orders_cleaned")
                        .filter(F.col("country") == "China")
                        .groupBy(
                            "customer_id",
                            "f_name",
                            "l_name",
                            F.date_trunc("DD", F.col("order_timestamp")).alias("order_date")
                        )
                       .agg(
                            F.sum("quantity").alias("books_counts")
                        )
            )