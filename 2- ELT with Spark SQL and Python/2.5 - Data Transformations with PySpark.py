# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

customers_df = spark.read.table("customers")

# COMMAND ----------

display(customers_df)

# COMMAND ----------

display(customers_df.select("customer_id", "email"))

# COMMAND ----------

from pyspark.sql.functions import col

display( customers_df.select(col("customer_id"), col("email").alias("customer_email"), col("updated").cast("timestamp")) )

# COMMAND ----------

display(customers_df.drop("profile", "updated"))

# COMMAND ----------

from pyspark.sql.functions import col, split

display(customers_df.withColumn("domain", split(col("email"), "@").getItem(1)))

# COMMAND ----------

display(customers_df.withColumnRenamed("email", "customer_email"))

# COMMAND ----------

display(customers_df)

# COMMAND ----------

new_customers_df = customers_df.withColumnRenamed("email", "customer_email")
display(new_customers_df)

# COMMAND ----------

from pyspark.sql.functions import col, split

new_customers_df = customers_df.withColumn("customer_domain", split(col("email"), "@").getItem(1)) \
                               .withColumnRenamed("email", "customer_email") \
                               .drop("profile", "updated")
 
display(new_customers_df)

# COMMAND ----------

from pyspark.sql import functions as F

new_customers_df = (
                    customers_df.withColumn("customer_domain", F.split(F.col("email"), "@").getItem(1))
                                .withColumnRenamed("email", "customer_email")
                                .drop("profile", "updated")
                    )
 
display(new_customers_df)

# COMMAND ----------

orders_df = spark.table("orders")

display(orders_df.describe())

# COMMAND ----------

display(orders_df.summary())

# COMMAND ----------

#deduped_df = orders_df.distinct()
deduped_df = orders_df.dropDuplicates()
display(deduped_df)

# COMMAND ----------

deduped_df = orders_df.dropDuplicates(["customer_id"])
display(deduped_df)

# COMMAND ----------

display(customers_df.summary())

# COMMAND ----------

display(customers_df)

# COMMAND ----------

#clean_df = customers_df.fillna({"email": "unknown@example.com"})
clean_df = customers_df.na.fill({"email": "unknown@example.com"})
display(clean_df)

# COMMAND ----------

#clean_df = customers_df.na.drop()
clean_df = customers_df.dropna()
display(clean_df)

# COMMAND ----------

joined_df = customers_df.join(orders_df, "customer_id", "left")
display(joined_df)

# COMMAND ----------

anti_joined_df = customers_df.join(orders_df, "customer_id", "left_anti")
display(anti_joined_df)

# COMMAND ----------

display(orders_df.where("customer_id = 'C01194'"))

# COMMAND ----------

books_df = spark.table("books")

cross_joined_df = customers_df.crossJoin(books_df)
display(cross_joined_df.orderBy("customer_id"))

# COMMAND ----------

print(f"customers count: {customers_df.count()}")
print(f"books count: {books_df.count()}")
print(f"customer-book pair: {cross_joined_df.count()}")

# COMMAND ----------

from pyspark.sql.functions import explode, broadcast

exploded_orders_df = (orders_df.withColumn("book", explode("books"))
                                .select("*", "book.book_id", "book.subtotal")
                                .drop("books", "book")
                     )

orders_details_df = exploded_orders_df.join(broadcast(books_df), "book_id", "inner")
display(orders_details_df)

# COMMAND ----------

spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 20 * 1024 * 1024)

# COMMAND ----------

spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

# COMMAND ----------

customers_details_df = spark.sql("""SELECT customer_id, email, updated,
                                        profile:first_name, profile:last_name, profile:gender,
                                        profile:address:street,
                                        profile:address:city,
                                        profile:address:country
                                    FROM customers
                                """)

display(customers_details_df)

# COMMAND ----------

customers_details_df.write.mode("overwrite").saveAsTable("customers_details")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_details
