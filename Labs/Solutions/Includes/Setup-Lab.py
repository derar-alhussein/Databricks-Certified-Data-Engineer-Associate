# Databricks notebook source
data_source_uri = "wasbs://course-resources@dalhussein.blob.core.windows.net/datasets/school/v1/"
dataset_school = 'dbfs:/mnt/DE-Associate/datasets/school'
checkpoint_path = 'dbfs:/mnt/DE-Associate/checkpoints/school'
dlt_path = 'dbfs:/mnt/DE-Associate/dlt/school'
db_name = 'DE_Associate_School'
dlt_db_name = 'DE_Associate_School_DLT'
spark.conf.set(f"dataset.school", dataset_school)

# COMMAND ----------

def clean_up():
    print("Removing Checkpoints ...")
    dbutils.fs.rm(checkpoint_path, True)
    print("Removing DLT storage location ...")
    dbutils.fs.rm(dlt_path, True)
    print("Dropping Database ...")
    spark.sql(f"DROP SCHEMA IF EXISTS {db_name} CASCADE")
    print("Dropping DLT database ...")
    spark.sql(f"DROP SCHEMA IF EXISTS {dlt_db_name} CASCADE")
    print("Removing Dataset ...")
    dbutils.fs.rm(dataset_school, True)
    print("Done")

# COMMAND ----------

try:
    clean = int(dbutils.widgets.get("clean"))
except:
    clean = 0

if clean:
    clean_up()

# COMMAND ----------

def path_exists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
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

def get_index(dir):
    files = dbutils.fs.ls(dir)
    index = 0
    if files:
        file = max(files).name
        index = int(file.rsplit('.', maxsplit=1)[0])
    return index+1

# COMMAND ----------

# Structured Streaming
streaming_dir = f"{dataset_school}/enrollments-streaming"
raw_dir = f"{dataset_school}/enrollments-raw"

def load_file(current_index):
    latest_file = f"{str(current_index).zfill(2)}.parquet"
    print(f"Loading {latest_file} file to the school dataset")
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
streaming_enrollments_dir = f"{dataset_school}/enrollments-json-streaming"
streaming_courses_dir = f"{dataset_school}/courses-streaming"

raw_enrollments_dir = f"{dataset_school}/enrollments-json-raw"
raw_courses_dir = f"{dataset_school}/courses-cdc"

def load_json_file(current_index):
    latest_file = f"{str(current_index).zfill(2)}.json"
    print(f"Loading {latest_file} enrollments file to the school dataset")
    dbutils.fs.cp(f"{streaming_enrollments_dir}/{latest_file}", f"{raw_enrollments_dir}/{latest_file}")
    #print(f"Loading {latest_file} courses file to the school dataset")
    #dbutils.fs.cp(f"{streaming_courses_dir}/{latest_file}", f"{raw_courses_dir}/{latest_file}")

    
def load_new_json_data(all=False):
    index = get_index(raw_enrollments_dir)
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



# COMMAND ----------

download_dataset(data_source_uri, dataset_school)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {db_name}")
spark.sql(f"USE {db_name}")
print(f"Schema for the hands-on labs: {db_name}")

# COMMAND ----------



# COMMAND ----------


