# Databricks notebook source
# MAGIC %md
# MAGIC ### Functions used to create (and recreate) our tables
# MAGIC The following cell contains some functions we will be using to write out our DataFrame as a table and also to recreate that table later.

# COMMAND ----------

from pyspark.sql import DataFrame

PATH_PREFIX = "/user/hive/warehouse/"

def clear_table(table_name, table_path):
  '''If the table exists then drop it.
     If the table files exist, delete them.
  '''
  spark.sql(f"DROP TABLE IF EXISTS {table_name}")
  try:
    dbutils.fs.rm(table_path, recurse=True)
  except Exception:
    pass #if the directory did not exist, then the files already were gone 

def check_dataframe_name(dataframe_name):
  '''If given a variable name, this function determines if the 
     the name passed is an actual valid variable name for a DataFrame,
     and if it is, returns that DataFrame.  If the variable does 
     not exist or is not a DataFrame, it returns None.  This is used so
     we can use the same code to create or recreate a table, but if the 
     table files exist,we don't need to run the code to create the 
     DataFrame it's based on.
  '''
  if (dataframe_name is None):
    df = None
  elif (dataframe_name in locals() ):
    df = locals()[dataframe_name]
  elif (dataframe_name in globals() ):
    df = globals()[dataframe_name]
  else:
    df = None
  if df is not None and isinstance(df,DataFrame):
    return(df)
  else:
    return(None)

def files_exist(path):
  '''If the directory path passed as a parameter exists and is a non-empty 
     directory (the directory contains files), this function returns true.
  '''
  files_exist = False # default, the directory does not exist or is empty
  try:
    file_list = dbutils.fs.ls(path)
    if len(file_list) > 0:
      return(True)
  except Exception:
    pass # files-exist is still False
  return(False) #if empty or path does not exist

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

def table_summary(table_name):
  '''Given a table name, this function will print the
     number of records in the table and then print 10
     records from the table.
  '''
  spark.sql(f"""
    SELECT COUNT(*) AS record_count
    FROM {table_name}
  """).show()
  # show 10 rows from the table
  spark.sql(f"""
    SELECT * 
    FROM {table_name} LIMIT 10 
  """).show(truncate=22)

def process_or_create_table(table_name, dataframe_name=None, summary=False, delete=False):
  '''This method is passed a table name and a DataFrame name.  If a table by that name 
     already exists, there is nothing to create.  If the files exist in the hive warehouse, 
     then the table is recreated from those files.  If the files don't exist, but the name 
     passed for the DataFrame name is an actual DataFrame, then the DataFrame is saved out as a table.
     If the table fiels don't exist and the name passed as a parameter is not an existing DataFrame, 
     then an error message is printed. If the table exists or could be created, and summary is True, 
     then a record count and 10 rows are printed.
  '''
  table_path = PATH_PREFIX + table_name
  if(delete):
    clear_table(table_name, table_path)
  df = check_dataframe_name(dataframe_name)
  if ( spark.catalog._jcatalog.tableExists(table_name) ):
    print(f"{table_name} table exists")
  elif files_exist(table_path): # If the table files exist build from the existing table files
    build_table(table_path, table_name)
  elif df is not None: # save the specified DataFrame as a table using the Parquet file format
    print(f"Saving the {dataframe_name} DataFrame as table: {table_name}")
    df.write.mode("overwrite").format('parquet').saveAsTable(table_name)
  else:
    print("The table files do not exist and no DataFrame was specified to be saved as a table, so the table was not created.")
    return
  # show some summary information for the table if a field name was specified
  if(summary):
    table_summary(table_name)

# COMMAND ----------

# MAGIC %md #### Create Table
# MAGIC In the notebook that is generating the DataFrame that is written to a table, we can add a function call to the `process_or_create_table` function defined in this notebook to either rebuild the table definition from the saved Parquet files or rebuild the table from the DataFrame usedto generate the table. Following is the sytax for this method call:
# MAGIC
# MAGIC `process_or_create_table(TBL_NAME, DF_NAME, summary=True, delete=False)`
# MAGIC
#<<<<<<< Bring-it-all-together--User-and-Gender
# MAGIC **Both the name of the table and the name of the DataFrame ust be passed as strings.**
=======
#<<<<<<< Spark-SQL-Joins
# MAGIC **Both the name of the table and the name of the DataFrame must be passed as strings.**
#=======
#<<<<<<< Introduction-to-Windown-Functions
# MAGIC **Both the name of the table and the name of the DataFrame ust be passed as strings.**
#=======
# MAGIC **Both the name of the table and the name of the DataFrame must be passed as strings.**
#>>>>>>> main
#>>>>>>> main
#>>>>>>> main
# MAGIC
# MAGIC **If we are calling this method from a notebook that does not contain the DataFrame used to write the table (but we want to rebuild the table from the Parquet files), then the DataFrame name should be omitted or passed as `None`.**
# MAGIC
# MAGIC If we want to see a record count and show the first few rows, `summary` should be True.
# MAGIC
# MAGIC If we want to force the table to be recreated from the DataFrame, then `delete` should be True, but be sure to set it back to False after the table is recreated. 
# MAGIC
# MAGIC **NOTE:** Since we want to be able to come back to this notebook and run this cell without creating the DataFrame variable, 
# MAGIC we need to pass the name of the DataFrame as a string so we can determine if it's a valid variable.  If not, then it will
# MAGIC be treated the same as if we did not specify a DataFrame.  This allows us to come back to this notebook and just run the 
#<<<<<<< Bring-it-all-together--User-and-Gender
# MAGIC function definitions above and then the following cell without first running the code to create `df_bus_reviews`. 
#=======
#<<<<<<< Spark-SQL-Joins
# MAGIC function definitions above and then a cell similar to the following cell without first running the code to create `df_some_data`. 
#=======
#<<<<<<< Introduction-to-Windown-Functions
# MAGIC function definitions above and then the following cell without first running the code to create `df_bus_reviews`. 
#=======
# MAGIC function definitions above and then a cell similar to the following cell without first running the code to create `df_some_data`. 
#>>>>>>> main
#>>>>>>> main
#>>>>>>> main
# MAGIC

# COMMAND ----------

#<<<<<<< Bring-it-all-together--User-and-Gender
# TBL_NAME = "business_reviews_table"
# DF_NAME = "df_bus_reviews"
#=======
#<<<<<<< Spark-SQL-Joins
# TBL_NAME = "some_table"
# DF_NAME = "df_some_data"
#=======
#<<<<<<< Introduction-to-Windown-Functions
# TBL_NAME = "business_reviews_table"
# DF_NAME = "df_bus_reviews"
#=======
# TBL_NAME = "some_table"
# DF_NAME = "df_some_data"
#>>>>>>> main
#>>>>>>> main
#>>>>>>> main
# process_or_create_table(TBL_NAME, DF_NAME, summary=True, delete=False)