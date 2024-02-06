# Databricks notebook source
# MAGIC %md
# MAGIC %md 
# MAGIC Copyright 2022, Scott Jensen, San Jose State University
# MAGIC
# MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-sa/4.0/88x31.png" /></a><br /><span xmlns:dct="http://purl.org/dc/terms/" property="dct:title">This notebook</span> by <span xmlns:cc="http://creativecommons.org/ns#" property="cc:attributionName">Scott Jensen, Ph.D.</span> is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/">Creative Commons Attribution-ShareAlike 4.0 International License</a>.
# MAGIC
# MAGIC # Purpose of this Notebook:
# MAGIC In prior exercises and in your team project, you will be writing out tables that you will then use as a data source in Tableau.  
# MAGIC
# MAGIC This code defines some functions we will be using to rebuild previously created tables.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Functions for processing tables
# MAGIC The following code cell defines the `process_tables` function and its supporting functions.
# MAGIC
# MAGIC This notebook is similar to the `Rebuild My Tables` notebook, except that this version only defines the function and 
# MAGIC does not directly invoke any code.
# MAGIC
# MAGIC #### Alternative uses:
# MAGIC
# MAGIC If you want to rebuild all of the tables listed for the hive warehouse directory, run:<br/>
# MAGIC `process_tables()`
# MAGIC
# MAGIC If you want to rebuild one table (assuming it's called "my_table"), run:<br/>
# MAGIC `process_tables("my_table")`
# MAGIC
# MAGIC If you had the following three tables you want to rebuild: moe_table, larry_table, curly_table, run them as a list:<br/>
# MAGIC `tables = ["moe_table","larry_table","curly_table"]`<br/>
# MAGIC `process_tables(tables)`
# MAGIC
# MAGIC #### Suppressing the summary:
# MAGIC
# MAGIC All of the above will recreate the tables and then print a record count and a 10-row summary.  If you do not want a summary, include the parameter: `summary=False`

# COMMAND ----------

PATH_PREFIX = "/user/hive/warehouse/"

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
  
  
def process_table(table_name, summary):
  '''This method is provided a string that should be a table name, but there is no guarantee.'''
  if len(table_name) == 0:
    print("A blank table name was passed and could not be processed")
    return
  table_path = PATH_PREFIX + table_name
  if ( spark.catalog._jcatalog.tableExists(table_name) ):
    print(f"{table_name} table exists")
    if summary == True:
      try:
        table_summary(table_name)
      except Exception:
        print(f"A summary could not be generated for the table {table_name}, check the table type. ")
        pass
    return
  elif files_exist(table_path): # If the table files exist build from the existing table files
    try:
      build_table(table_path, table_name)
      if summary == True:
        table_summary(table_name)
    except Exception:
      print(f"Could not create the table {table_name}, check the data type.")
    finally:
      return      
  else:
    print(f"The table files do not exist for {table_name}, so the table was not created.")
    return
  
def get_table_list():
  '''Returns a list of the directory names in the user hive warehouse'''
  table_list = []
  fileinfo_list = dbutils.fs.ls(PATH_PREFIX)
  for fileinfo in fileinfo_list:
    filename = fileinfo.name.strip().rstrip('/')
    filename
    if len(filename) > 0:
      table_list.append(filename)
  return(table_list)
  

def process_tables(table_names=None, summary=True):
  '''This method is optionally passed a string with a table name or a list of table names.  For each table, If a table by that name 
     already exists, there is nothing to create.  If the files exist in the hive warehouse, 
     then the table is recreated from those files.  If the files don't exist, an exception is encountered and prints the offending table name.
     If no parameter is passed for the table name(s), then the program tries to rebuild any table in the hive warehouse.
     If the table(s) exist or could be created, and summary is True, 
     then a record count and 10 rows are printed.
  '''
  tables = []
  if table_names is None:
    tables = get_table_list()
    print(f"building {len(tables)} tables from DBFS")
  elif isinstance(table_names, str):
    tables.append(table_names.strip() )
    print(f"building {table_names}")
  elif isinstance(table_names, list):
    tables = table_names
    print(f"building {len(tables)} tables provided as list")
  else:
    raise TypeError("optional table_names must be str of single table or list of tables")
  for table in tables:
    process_table(table.strip(), summary)