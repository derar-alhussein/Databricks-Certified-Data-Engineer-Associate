# Databricks notebook source
import time

# COMMAND ----------

def path_exists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    msg = str(e)
    if ("com.databricks.sql.io.CloudFileNotFoundException" in msg
        or "java.io.FileNotFoundException" in msg):
      return False
    else:
      raise

# COMMAND ----------

def download_dataset(source, target):
    files = dbutils.fs.ls(source)

    for f in files:
        source_path = f"{source}/{f.name}"
        target_path = f"{target}/{f.name}"
        if not path_exists(target_path):
            print(f"Copying {f.name} ...")
            dbutils.fs.cp(source_path, target_path, True)

# COMMAND ----------

data_source_uri = "s3://dalhussein-courses/datasets/bookstore/v1/"

db_name = "default"

catalogs = spark.sql("SHOW CATALOGS").collect()
hive_exists = any(row.catalog == 'hive_metastore' for row in catalogs)
if hive_exists:
    data_catalog = 'hive_metastore'
    dataset_bookstore = 'dbfs:/mnt/demo-datasets/bookstore'
    checkpoints_bookstore = 'dbfs:/mnt/demo/checkpoints'

    spark.sql(f"USE CATALOG {data_catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {db_name}")
    spark.sql(f"USE SCHEMA {db_name}")
else:
    data_catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
    dataset_volume_name = "bookstore_dataset"
    checkpoints_volume_name = "bookstore_checkpoints"
    dataset_bookstore = f"/Volumes/{data_catalog}/{db_name}/{dataset_volume_name}"
    checkpoints_bookstore = f"/Volumes/{data_catalog}/{db_name}/{checkpoints_volume_name}"

    spark.sql(f"USE CATALOG {data_catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {db_name}")
    spark.sql(f"USE SCHEMA {db_name}")

    spark.sql(f"CREATE VOLUME IF NOT EXISTS {dataset_volume_name}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {checkpoints_volume_name}")

try:
    spark.conf.set(f"dataset.bookstore", dataset_bookstore)
    spark.conf.set("fs.s3a.endpoint", "s3.eu-west-3.amazonaws.com")
    spark.conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
except:
    pass

print(f"Data Catalog: {data_catalog}")

# COMMAND ----------

def get_index(dir):
    files = dbutils.fs.ls(dir)
    index = 0
    if files:
        file = max(files).name
        index = int(file.rsplit('.', maxsplit=1)[0])
    return index+1

# COMMAND ----------

def set_current_catalog(catalog_name):
    spark.sql(f"USE CATALOG {catalog_name}")

# COMMAND ----------

# Structured Streaming
streaming_dir = f"{dataset_bookstore}/orders-streaming"
raw_dir = f"{dataset_bookstore}/orders-raw"

def load_file(current_index):
    latest_file = f"{str(current_index).zfill(2)}.parquet"
    print(f"Loading {latest_file} file to the bookstore dataset")
    dbutils.fs.cp(f"{streaming_dir}/{latest_file}", f"{raw_dir}/{latest_file}")

    
def load_new_data(all=False):
    index = get_index(raw_dir)
    if index >= 10:
        print("No more data to load\n")

    elif all == True:
        while index <= 10:
            load_file(index)
            index += 1
    else:
        load_file(index)
        index += 1

# COMMAND ----------

# DLT
streaming_orders_dir = f"{dataset_bookstore}/orders-json-streaming"
streaming_books_dir = f"{dataset_bookstore}/books-streaming"

raw_orders_dir = f"{dataset_bookstore}/orders-json-raw"
raw_books_dir = f"{dataset_bookstore}/books-cdc"

def load_json_file(current_index):
    latest_file = f"{str(current_index).zfill(2)}.json"
    print(f"Loading {latest_file} orders file to the bookstore dataset")
    dbutils.fs.cp(f"{streaming_orders_dir}/{latest_file}", f"{raw_orders_dir}/{latest_file}")
    print(f"Loading {latest_file} books file to the bookstore dataset")
    dbutils.fs.cp(f"{streaming_books_dir}/{latest_file}", f"{raw_books_dir}/{latest_file}")

    
def load_new_json_data(all=False):
    index = get_index(raw_orders_dir)
    if index >= 10:
        print("No more data to load\n")

    elif all == True:
        while index <= 10:
            load_json_file(index)
            index += 1
    else:
        load_json_file(index)
        index += 1

# COMMAND ----------

download_dataset(data_source_uri, dataset_bookstore)
set_current_catalog(data_catalog)
