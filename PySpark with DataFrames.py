# Databricks notebook source
# MAGIC %md 
# MAGIC Truc Nguyen

# COMMAND ----------

# MAGIC %md 
# MAGIC Copyright Scott Jensen, San Jose State University
# MAGIC
# MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-sa/4.0/88x31.png" /></a><br /><span xmlns:dct="http://purl.org/dc/terms/" property="dct:title">This ds4all notebook</span> by <span xmlns:cc="http://creativecommons.org/ns#" property="cc:attributionName">Scott Jensen,Ph.D.</span> is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/">Creative Commons Attribution-ShareAlike 4.0 International License</a>.

# COMMAND ----------

# MAGIC %md
# MAGIC # PySpark and DataFrames
# MAGIC
# MAGIC In this notebook we are going to introduce some basic ideas about PySpark (since we will be using Python).  
# MAGIC The documentation for the classes and functions in that module can be found <a href="http://spark.apache.org/docs/latest/api/python/pyspark.sql.html" target="_blank">here</a>.
# MAGIC If you click on that link, it can be a bit intimidating - remember that page is documenting *every* method and function in PySpark.  At the top of that
# MAGIC list are the major classes in the PySpark SQL module.  This is the syntax for using PySpark.  Almost anything you can do in PySpark you can also do using Spark SQL through the `spark.sql` method for the Spark session, and we will generally use `spark.sql` instead of PySpark. In this notebook we will cover the PySpark methods we will be using, particularly for creating DataFrames.
# MAGIC
# MAGIC We will start with the SparkSession, which is created by Databricks when you spun up your cluster and it's always named `spark` by convention (not just in Databricks - that's a Spark convention).
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Loading Data
# MAGIC
# MAGIC In this course we will be loading the following data:
# MAGIC * **The Yelp dataset files** which use a JSONL (JSON Lines) format with each entry (business, user, review, etc.) as a JSON object on a separate line - one JSON object per line.
# MAGIC * **The Yelp categories definitions** from Yelp's Fusion developer API.  This is data they make available to programmers who want to develop an app  that uses Yelp data (on a user's behalf) and all of the requests and responses are made in a JSON format though their Fusion application programming interface (API).  We are going to use their category definitions which are a ***controlled vocabulary*** used to define the categories businesses are in.  It's referred to as a controlled vocabulary because businesses can only select from the pre-defined list provided by Yelp. This file is also in JSON, but uses a **multi-line** format.  Early versions of Spark could not handle this format, but it's now an option.  The entire file is a single JSON array of over 1,500 JSON objects.
# MAGIC * **Gender name data from the Social Security Administration** that we will use to predict the gender of Yelp users.  This is in a comma-separated value (CSV) format.  Each year's data (from 1880 until last year) is in a separate file and each file has no column headings - the first row of the file is a row of data.  We will load this data in a future exercise on Data Wrangling. We can load all of the files into a single DataFrame.
# MAGIC * **The metro areas for each business**. This is a CSV file you will generate later in the semester, so we won't be loading it in this notebook, but you will use Tableau to generate the file.  This CSV file will have headers, but we will need to modify them since they are not formatted well (good for humans, not for computers).
# MAGIC
# MAGIC In this exercise we will use with the Yelp business data and read the JSON data into a DataFrame. <strong><a href="https://www.yelp.com/dataset/documentation/main" target="_blank">Based on the metadata provided by Yelp</a></strong>, we have some information about the data, but it may not all be up-to-date.  One thing that is clear is that each business is a JSON object and some of the values for name:value pairs in the data have nested JSON objects for their value.
# MAGIC
# MAGIC In Step 1 we will walk through reading our business JSON file (zipped up in the bzip2 format). 
# MAGIC In future exercises we will just say we are reading the data - from experience you will know how to do that (or know which
# MAGIC of your notebooks to look at for an example).
# MAGIC
# MAGIC To read the data file, we start with the `read` method from our `SparkSession` variable named `spark`.
# MAGIC
# MAGIC If you look at the minimal PySpark documentation for the **<a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.read.html#pyspark-sql-sparksession-read" target="_blank">read</a>** method, you will see that it returns a `DataFrameReader` 
# MAGIC which "can be used to read data in as a DataFrame".  If we entered the following in a cell, the variable we are creating on the left side of the = sign (named `dfr`) would be populated with an instance of a `DataFrameReader`.
# MAGIC
# MAGIC `dfr = spark.read`
# MAGIC
# MAGIC However, we want to read the business data into a DataFrame, not just create a DataFrameReader, so we won't stop there.  If you take a look at the documentation for 
# MAGIC a **<a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.html#pyspark-sql-dataframereader" target="_blank">DataFrameReader</a>**.  The first method you will probably see is the `csv()` method which reads data in from a csv file such as our data from the 
# MAGIC Social Security Administration.  However, the business data is a JSON file, 
# MAGIC so scroll on down to the **<a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.json.html#pyspark-sql-dataframereader-json" target="_blank">json</a>** method.
# MAGIC You will see there are a ***lot*** of possible optional parameters, but for our purposes their defaults are fine, we just need to provide a path to our data file (the only required parameter).  
# MAGIC
# MAGIC In the cell below, add the path to your business data as a string (meaning the path is enclosed in quotes).  If you are not sure what that
# MAGIC path is, go take a look at your DBFS and come back here.  To see what is in DBFS on your account, click the `Data` icon in the toolbar on the left in Databricks, and then click the `Create Table` button.
# MAGIC You may be saying, "but I'm not creating a table at the moment", however, this is the path to seeing what you have stored on DBFS. In the `Create New Table` screen that appears, click on the `DBFS` button in the row of buttons on the top.  You are now in your DBFS, and if you loaded the Yelp data the way we discussed in class, it's in the `/yelp` directory, so click on that folder and then click on the business data file. Whenever you have a file selected in DBFS, the path to get there is listed at the bottom of the screen, so you can copy and paste that path into the code below.
# MAGIC
# MAGIC The result of the `json` method of the `DataFrameReader` is a DataFrame, so we assign the result to a variable named `df_business`.
# MAGIC Keep in mind what is happening here.  The code `spark.read` generates a `DataFrameReader`, so the "dot" after that code is saying 
# MAGIC to call the `json` method from that class on that `DataFrameReader` which was created.
# MAGIC
# MAGIC ## Step 1a: Loading the business data
# MAGIC
# MAGIC The following line reads your Yelp business data file into a DataFrame.  Be sure it has the correct path to your data.

# COMMAND ----------

#check data source
dbutils.fs.ls("yelp")

# COMMAND ----------

# Add the path to your business data on DBFS as a parameter of the json method.
df_business = spark.read.json("dbfs:/yelp/business.bz2")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1c: Metadata about our data
# MAGIC The following cell calls the following two methods:
# MAGIC * **<a href="" target="_blank">count()</a>** which returns how many rows (in this case businesses) there are in the DataFrame.  Here we have enclosed the call to the `count` method inside a Python print function.
# MAGIC * **<a href="" target="_blank">printSchema()</a>** prints the schema metadata about our DataFrame to the output.  Since we did not specify a schema with the structure and data types of the fields in our data, Spark *inferred* the fields and data type for each field when the data was loaded.
# MAGIC
# MAGIC In general, both of these steps should be included when loading any data file into a DataFrame, but could be done in the same cell where the data is loaded.  In addition, it's recommended that you print out the record count whenever you do a significant transformation to a DataFrame using PySpark or a `spark.sql` query.
# MAGIC
# MAGIC In the call to the `printSchema` method, you may have noticed all of the fields say `nullable = true`.  Since we did not provide a schema, and JSON does not require any fields, Spark made the assumption that any field in the data could be Null (has no value) or could be missing for any particular business.  You will also see that Spark inferred the data type for each field. It does this by first making one pass through the data to look for the most restrictive data type possible for a field and determine the schema.  A second pass then loads the data.
# MAGIC
# MAGIC #### The next cell is step 1c:

# COMMAND ----------

print ("record count:", df_business.count() )
df_business.printSchema()

# COMMAND ----------

# MAGIC %md ## Step 2: What does our data look like?
# MAGIC
# MAGIC The DataFrame class has a method named **<a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.show.html#pyspark-sql-dataframe-show" target="_blank">show()</a>** that will show us the first 20 rows.  
# MAGIC
# MAGIC Since some fields, such as the business address or the text of a review can be long, the default is to show the first 20 characters of each column.  This is known as truncating the data (truncate means "chop off").  
# MAGIC
# MAGIC We can include optional parameters to say how many rows we want and whether to truncate the fields (or how many characters to truncate at). Example: `df_business.show(truncate=False)`
# MAGIC
# MAGIC If the result is wrapping around, we can add `vertical=True` to print each column for a row of data on a separate line (you will probably want to reduce the number of rows - such as the following example shows only the first 2 rows). 
# MAGIC
# MAGIC Example: `df_business.show(2, vertical=True)`
# MAGIC
# MAGIC You may have also noticed that when there are nested structures (such as the value for the `attributes` name:value pair is itself a JSON object converted to a DataFrame structure). Although all of the values for that nested JSON object are shown in a single "attributes" column, behind the scenes that nested structure can still be queried - we will explore an example shortly.
# MAGIC
# MAGIC #### In the following code cell:
# MAGIC
# MAGIC Add a call to the `show` method to show the rows (20 by default) in your `df_business` DataFrame.
# MAGIC
# MAGIC
# MAGIC #### The next cell is Step 2:

# COMMAND ----------

# Add a call to the show() method here
df_business.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 3: <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.select.html#pyspark.sql.DataFrame.select" target="_blank">Select</a> in PySpark
# MAGIC Throughout the semester we will usually query our data using `spark.sql` instead of PySpark, but you can use either in your project (just be sure your teammates are OK with PySpark).
# MAGIC
# MAGIC Since every `spark.sql` query requires creating a temporary view first (or a table), there are times when we want to do something simple and a short PySpark bit of code is quicker to write (PySpark works directly on DataFrames).  An example of that is when we want to just select a subset of the columns from a DataFrame.  In those cases, you can use the `select` method.  If you are familiar with SQL, that should sound familiar - it works the same as the SELECT clause in a QL query, but the syntax is a bit different.
# MAGIC
# MAGIC At a minimum, a SQL query requires two clauses: SELECT and FROM.  SELECT says what columns you want back, and FROM says the table or temporary view the data is in.  With PySpark, the `selest` method is a DataFrame method, so it's called on a DataFrame using the dot notation in Python - so no FROM clause is needed since the DataFrame it's called on is the FROM.
# MAGIC
# MAGIC When calling the `select` method in PySpark, you can either:
# MAGIC
# MAGIC * provide a comma-separated list of column names in quotes
# MAGIC * provide a comma-separate list of field names prefixed with the DataFrame's name and a dot
# MAGIC
# MAGIC The first approach is easier if you just want a few existing columns.  
# MAGIC
# MAGIC #### In the following code cell:
# MAGIC Select the `business_id`, `name`, `state`, and `stars` columns from the `df_business` DataFrame.
# MAGIC
# MAGIC #### The next cell is step 3a:

# COMMAND ----------

# Add your code for Step 3a here
df_business.select("business_id","name","state","stars").show()

# COMMAND ----------

# MAGIC %md
# MAGIC In Step 1c you printed the schema for the business data.  Each name:value pair in the JSON object for a business became a column in the DataFrame you created, with the `name` becoming the column name in the DataFrame and the `value` becoming the value in the row for that business.  If the data for a business does not include a specific name:value pair (but there is a column in the DataFrame because it exists for another business), the value in that column for the row representing that business is null in the DataFrame.
# MAGIC
# MAGIC What about the name:value pairs for `attributes` and `hours`?  In those name:value pairs, the value is a nested JSON object.  If you look at the output in Step 2, when we call the DataFrame's `show` method, the `attributes` column appears to show the *value* for the name:value pairs within `attributes`, but it does not show the corresponding `name`.  We did not lose the names for the name:value pairs within attributes, they are just not shown.  If we wanted to use the PySpark `select` method to include the `RestaurantsPriceRange2` attribute (a value 1-4 for <code>$ - $$$$</code> indicating how expensive a business is), we can use the "dot" notation familiar from Python to reach inside the attributes and include that attribute: `attributes.RestaurantsPriceRange2`.
# MAGIC
# MAGIC #### In the following code cell:<br/>
# MAGIC Copy the code you wrote in Step 3a to select the `business_id`, `name`, `state`, and `stars` fields, but use the "dot" notation to also include the `Saturday` column from the nested JSON object for the `hours` name:value pair in the business data. 
# MAGIC
# MAGIC #### The next cell is step 3b:

# COMMAND ----------

# Add your code here
df_business.select("business_id","name","state","stars","hours.Saturday").show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 4: <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html#pyspark.sql.DataFrame.filter" target="_blank">Filter</a> in PySpark
# MAGIC
# MAGIC If you are familiar with the SQL database language, the `filter` method for DataFrames is similar to the SQL `WHERE` clause, and in fact you can also use `where` instead of `filter` (they do the same thing).  The `select` method allowed you to select specific columns from an existing DataFrame and it returned a *new* DataFrame with just those columns (it did not modify the existing DataFrame).
# MAGIC
# MAGIC The `filter` method allows you to specify a condition for which *rows* from the existing DataFrame should be included in the new DataFrame you will generate. The easiest approach is to specify your condition as a string (in quotation marks).  If specified as a string, we can refer to the columns in the existing DataFrame just by their column names.  If we specify our condition not in a string, each field needs to be specified as coming from a specific DataFrame.
# MAGIC
# MAGIC #### Chaining PySpark methods:
# MAGIC As mentioned above, both the `select` and `filter` methods generate a new DataFrame as their output, so just like other methods in Python, we can chain these PySpark methods together.  By chaining the `select` and `filter` methods together, our PySpark would be similar to a SQL query with SELECT, FROM, and WHERE clauses (if you are already familiar with SQL).
# MAGIC
# MAGIC #### In the following code cell:
# MAGIC
# MAGIC Copy the code you wrote in Step 3a to select 4 columns from the business data, but add the following:
# MAGIC * Chain a call to the `filter` method onto the end of your call to the `select` method.  This will feed the DataFrame resulting from the `select` method into your call to the `filter` method. The filter method should include ONLY those businesses where the `state` column contains `CA`.
# MAGIC   * The parameter for the filter method should be in quotation marks (use double-quote marks).
# MAGIC   * The `state` column contains a string value, so `CA` needs to be in quotation marks, but you will need to use single-quotes here since the entire condition for the `filter` method is in double-quotes.
# MAGIC * Name the new DataFrame `df_ca_businesses`.
# MAGIC
# MAGIC #### The next cell is step 4:
# MAGIC

# COMMAND ----------

# Add your code here
df_ca_businesses= df_business.select("business_id","name","state","stars").filter("state=='CA'").show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 5: Display in PySpark
# MAGIC We will just give a brief example here, but the `display()` function can be used directly with a DataFrame as a parameter in Spark to show some basic charts.
# MAGIC
# MAGIC In Databricks, `display()` also works as a DataFrame *method*, but you won't find this in the Spark API documentation since it is specific to Databricks.
# MAGIC
# MAGIC In the following cell we will call the `display` function using the DataFrame we generated back in step 3a to show the businesses in each state and a breakdown as to the percentage of businesses in each rating category (1 to 5 stars in 1/2-star increments).
# MAGIC
# MAGIC #### Plot Options:
# MAGIC * Keys: The field to show on the x-axis (what defines the bars in a bar chart).
# MAGIC * Values: the field that gets aggregated in each bar in the bar chart (we will be counting businesses).
# MAGIC * Series grouping: The field that specifies the lines in a line chart or the segments in a stacked bar chart.
# MAGIC * Aggregation: How Spark should be aggregating the field in the `Values` field.  For example, if we select `SUM` when `Values` is set to the `stars` field, a 5-star restaurant has 5 times the impact of a 1-star restaurant.  However, if we select `COUNT`, we are counting *rows* in the DataFrame, and a 5-star restaurant and a 1-star restaurant count the same (they are each 1 row in the DataFrame).
# MAGIC * Display type: The type of chart you are creating (there may be additional options above the `Display type` field depending on the chart type selected).
# MAGIC
# MAGIC #### Step 5a: In the next code cell:
# MAGIC * Add the name of the DataFrame you generated in Step 3a as the only parameter to the `display` function in the following cell.
# MAGIC * Create a bar chart with a bar for each rating level.
# MAGIC * On the y-axis, count the number of businesses with each rating (HINT: although you should theoretically be able to count *anything*, the Values input requires a numeric field)
# MAGIC
# MAGIC #### The next cell is Step 5:

# COMMAND ----------

# Put your DataFrame's name as the parameter
# Set the chart and plot options
business_stars=df_business.select("business_id","name","state","stars")
display(business_stars)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5b: Sorting in PySpark using <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.orderBy.html#pyspark-sql-dataframe-orderby" target="_blank">orderBy</a>
# MAGIC
# MAGIC The bar chart probably came out as bars from 1 to 5 stars in order (by 1/2-star increments).
# MAGIC
# MAGIC What if we wanted to see the bars from left to right in decreasing order?
# MAGIC
# MAGIC We can use the PySpark `orderBy` method to first sort the stars field in our DataFrame from highest to lowest in descending order.  The `orderBy` parameter takes a column name (as a string) as it's parameter.  If you want to sort on multiple columns, they can be specified as a list of strings (in the order they should be sorted).  By default, the sort order is ascending (1 to 5 in the case of the star ratings).  To specify descending order, we would need to include a second optional parameter `ascending` and set its value to False.  When you use the capital "F" in False, the syntax color-coding will show the boolean value as purple.  If you forget and use a lower-case "f", it is not actually a boolean value and will cause an error (unless you already defined a boolean variable named false).  Keep in mind that this is a Python mwthod, so the name of the optional parameter is NOT in quotes.
# MAGIC
# MAGIC The `orderBy` method must be specified with a capital 'B' (case matters).  Alternately, `sort` can be used instead of `orderBy` and has the same syntax.
# MAGIC
# MAGIC ### Edit your code in step 5a above:
# MAGIC * Sort the DataFrame in descending order based on the `stars` field
# MAGIC * The `orderBy` method returns a new DataFrame, so use the `orderBy` method on your DataFrame *within* the parenthesis for your call to the `display` function. 

# COMMAND ----------

business_stars=df_business.select("business_id","name","state","stars")
display(business_stars.orderBy('stars', ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 6: Summary Statistics
# MAGIC
# MAGIC PySpark has two DataFrame methods that will provide summary statistics about every numeric column in your DataFrame:
# MAGIC * **<a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.describe.html#pyspark.sql.DataFrame.describe" target="_blank">describe</a>**: returns a DataFrame with the count, mean, stddev, min, and max for all columns in the DataFrame.  If you want to describe only some of the columns, those should be provided as a Python string list of the columns you want to describe.
# MAGIC * **<a href="" target="_blank">summary</a>**: returns a DataFrame with the following statistics for numeric columns: count, mean, stddev, min, max, and approximate quartiles.  You can alternately specify a list of the statistics you want (from those listed) or specify arbitrary percentiles (instead of quartiles: 25%, 50%, and 75%).
# MAGIC
# MAGIC #### Insert two code cells below and add the following: 
# MAGIC
# MAGIC * In the first code cell, call the `describe` method on the `df_business` DataFrame from Step 1a and show the results, but call it on a DataFrame where you have first selected the `stars`, `review_count`, and `is_open` fields (the `describe` method should be chained after the `select` method).
# MAGIC * In the second code cell, call the `summary` method on a DataFrame that first selects the `stars` and `review_count` fields from the `df_business` DataFrame and show the results.

# COMMAND ----------

df_business.select("stars","review_count", "is_open").describe().show()

# COMMAND ----------

df_business.select('stars','review_count').summary().show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Deliverable
# MAGIC
# MAGIC Be sure to complete all of the steps above (including those we left in class for you to do on your own).
# MAGIC
# MAGIC After checking your results against the result notebook linked in the assignment (you may want to compare results as you complete each step), publish your notebook and upload the published link.