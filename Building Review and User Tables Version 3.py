# Databricks notebook source
# MAGIC %md
# MAGIC # Building Review and User Tables
# MAGIC The review and user data files are the largest files you will be using in class and in your project.  Loading a DataFrame, selecting the needed columns, and caching the data from the JSON for either data file is slow, so this notebook instead loads the data and creates Spark tables named `reviews_without_text_table` and `user_table` based on the Parquet format that's common in Big Data.  Going forward, the tables can be used (on that same running cluster) the same as a temporary view. On a subsequently created cluster, the table definition will be gone, but the Parquet files used to build the tables will still exist and this notebook will rebuild them in less than a minute. 
# MAGIC
# MAGIC Since the original code below excludes some of the fields in each set
# MAGIC of JSON data files when building the tables initially, if you have already run this notebook and created the tables, but then want to include a different set of fields for either table,
# MAGIC rerun the cell that builds that table, but ***temporarily*** set the corresponding variable to force the rebuilding of the table from the JSON data files to True.  Be sure after the table is rebuilt to set it back to False.
# MAGIC
# MAGIC <span style="color:red;">Be sure to run this notebook before we cover SQL or you use the SQL Examples notebook</span>
# MAGIC
# MAGIC The first time this notebook is run, it will take up to 45 minutes to build both tables from the JSON files.  To rerun if you come back to your account on a new cluster, it should take less than a minute.
# MAGIC
# MAGIC As noted above, once the tables are built, they can be used in Spark SQL queries the same as temporary views in Spark SQL queries.
# MAGIC
# MAGIC **NOTE:** The following cell includes imports and function definitions that are needed when running the cells further down for building the `reviews_without_text_table` and `user_table`, so the next cell always needs to be run.

# COMMAND ----------

import pyspark.sql.functions as f

def directory_exists(path):
  '''Checks if a directory exists and contains some files.
     If the directory exists, but contains no files,
     then treated the same as if the directory did not exist.
     Returns True if the directory exists and contains files, 
     otherwise False
  '''
  try:
    file_list = dbutils.fs.ls(path)
    if len(file_list) > 0:
      return(True)
    else:
      return(False)
  except Exception:
    return(False) #if empty or path does not exist
  

def file_exists(path):
  '''Checks if the path provided is a file.
     If the path exists, the list function 
     will contain a list with one element.
     Returns True if the file exists and contains files, 
     otherwise False
  '''
  try:
    file_list = dbutils.fs.ls(path)
    if len(file_list) > 0:
      return(True)
    else:
      return(False)
  except Exception:
    return(False) #if path does not exist


def build_table(table_path, table_name):
  '''Given the path to where the files for the table are, which is 
     generally under /user/hive/warehouse, and the name of the table,
     this function builds the table.  Note that it assumes the table
     was originally built using PARQUET.
  '''
  print(f"building {table_name} from existing table files")
  spark.sql(f"""
    CREATE TABLE {table_name} 
    USING PARQUET 
    LOCATION '{table_path}' 
  """)
    

def table_summary(field_name, table_name):
  '''Given a table name and a field to count in the table, 
     this function generates a count of the number of records 
     and shows the first 10 rows truncated at 22 characters. 
  '''
  spark.sql(f"""
    SELECT COUNT({field_name}) AS record_count
    FROM {table_name}
  """).show()
  # show 10 rows from the table
  spark.sql(f"""
    SELECT * 
    FROM {table_name} LIMIT 10 
  """).show(truncate=22)


def process_table(table_name, table_path, data_path, field_name, force_rebuild, json_read_function,summary=True):
  '''This routine is called to create the tables based on the review or user data
     The 7 parameters passed are:
     1. table_name: string with the name of the table being created
     2. table_path: string with the path in DBFS to where the table files in a Parquet format are located.  Usually under /user/hive/warehouse/
     3. data_path: string with the DBFS path of the compressed JSON data file
     4. field_name: string with the name of a field within the resulting table that will always be included and not Null
     5. force_rebuild: a Boolean value as to whether the table should be rebuilt from the JSON files even if the table files exist
     6. json_read_function: the name of a function that takes one parameter, the path to the JSON data, and returns the cached DataFrame that was created
     7. summary: optional boolean that specifies if a summary of the created table should be shown. Default is True
  '''
  if ( spark.catalog._jcatalog.tableExists(table_name) and force_rebuild == False):
    print(f"{table_name} table exists")
  else:
    # If the table files exist and the download is NOT being forced, build from the existing table files
    if directory_exists(table_path) and force_rebuild == False:
      build_table(table_path, table_name)
    else: # create dataframe from JSON files and save as a table
      # delete existing table files if they exist
      try:
        dbutils.fs.rm(table_path, recurse=True)
      except Exception:
        pass #if the directory did not exist, then the files already were gone or never existed
      # Check that the JSON data file exists
      if file_exists(data_path) == False:
        raise Exception(f"The path to the compressed JSON data file: {data_path} does not exist.")
      print(f"building {table_name} from JSON file")
      df_temp = json_read_function(data_path)
      # Does not already exist as a table, so write it out
      df_temp.write.mode("overwrite").format('parquet').saveAsTable(table_name)
      df_temp.unpersist()
  # Regardless of whether table was built from existing PARQUET files or JSON files, 
  # show some summary information for the table if the summary parameter is True
  if summary:
    table_summary(field_name, table_name)
  

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Build the table for the review data
# MAGIC The following cell uses the functions defined in the cell above, so that must be run first.  
# MAGIC
# MAGIC The code can be used in other projects to build the review table.  If different fields are desired in the table, 
# MAGIC then the `create_review_dataframe` function should be edited, but if that function is changed, and the table already exists, then 
# MAGIC the `FORCE_REVIEW_REBUILD` constant should ***temporarily*** be set to True.  When building the table from the JSON files, it
# MAGIC will require pproximately 25 minutes, but when rebuilding the table from the table files later (on a new cluster), it will 
# MAGIC take less than 1 minute.

# COMMAND ----------

# If you want to force the building of the table from the JSON files, set FORCE_REVIEW_REBUILD to True.
# Only do this if records are missing from the table, it's not able to create the table, or you want to change the fields 
# included in the table since it is time consuming.
# ************************************************************************************************
# AFTER RUNNING IT TO REBUILD FROM THE JSON DATA BE SURE THAT FORCE_REVIEW_REBUILD IS SET TO FALSE
# ************************************************************************************************
FORCE_REVIEW_REBUILD = False 
REVIEW_DATA_PATH = "/yelp/review.bz2"
REVIEW_TBL_PATH = "/user/hive/warehouse/reviews_without_text_table"
REVIEW_TBL_NAME = "reviews_without_text_table"
REVIEW_FIELD_NAME = "business_id" # field from the review table that's always included and never Null


def create_review_dataframe(data_path):
  df_reviews = spark.read.json(REVIEW_DATA_PATH).\
  select("review_id","business_id","user_id","cool","funny","useful","date","stars").cache()
  return(df_reviews)
  

process_table(REVIEW_TBL_NAME, REVIEW_TBL_PATH, REVIEW_DATA_PATH, REVIEW_FIELD_NAME, FORCE_REVIEW_REBUILD, create_review_dataframe, summary=True)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Build the table for the user data
# MAGIC The following cell uses the functions defined in the first code cell above, so that must be run first.  
# MAGIC
# MAGIC The code can be used in other projects to build the user table.  If different fields are desired in the table, 
# MAGIC then the `create_user_dataframe` function should be edited, but if that function is changed, and the table already exists, then 
# MAGIC the `FORCE_USER_REBUILD` constant should ***temporarily*** be set to True.  When building the table from the JSON files, it
# MAGIC will require pproximately 18 minutes, but when rebuilding the table from the table files later (on a new cluster), it will 
# MAGIC take less than 1 minute.

# COMMAND ----------

#If you want to force the building of the table from the JSON files, set FORCE_USER_REBUILD to True.
# Only do this if records are missing from the table, it's not able to create the table, or you want to change the fields 
# included in the table since it is time consuming.
# **********************************************************************************************
# AFTER RUNNING IT TO REBUILD FROM THE JSON DATA BE SURE THAT FORCE_USER_REBUILD IS SET TO FALSE
# **********************************************************************************************
FORCE_USER_REBUILD = False 
USER_DATA_PATH = "/yelp/user.bz2"
USER_TBL_PATH = "/user/hive/warehouse/user_table"
USER_TBL_NAME = "user_table"
USER_FIELD_NAME = "user_id" # field from the user table that's always included and never Null


def create_user_dataframe(data_path):
  df_users = spark.read.json(USER_DATA_PATH).\
  withColumn("friend_count",f.size(f.split(f.col("friends"),'\s*,\s*') ) ).drop("friends")
  # withColumnRenamed("elite","original_elite").\
  # withColumn("elite", f.regexp_replace( f.col("original_elite"), '20,20','2020').alias("elite") ).\
  # drop("original_elite").cache()
  return(df_users)
  

process_table(USER_TBL_NAME, USER_TBL_PATH, USER_DATA_PATH, USER_FIELD_NAME, FORCE_USER_REBUILD, create_user_dataframe, summary=True)