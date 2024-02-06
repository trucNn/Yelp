# Databricks notebook source
# MAGIC %md 
# MAGIC Copyright Scott Jensen, San Jose State University
# MAGIC
# MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-sa/4.0/88x31.png" /></a><br /><span xmlns:dct="http://purl.org/dc/terms/" property="dct:title">This notebook</span> by <span xmlns:cc="http://creativecommons.org/ns#" property="cc:attributionName">Scott Jensen,Ph.D.</span> is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/">Creative Commons Attribution-ShareAlike 4.0 International License</a>.

# COMMAND ----------

# MAGIC %md
# MAGIC # Spark SQL Joins
# MAGIC
# MAGIC To answer our question this semester, we are going to need to bring together multiple data files:<br/>
# MAGIC <img src="https://www.sjsu.edu/people/scott.jensen/courses/Yelp%20Project%20Data-all%20tables.png"/>
# MAGIC
# MAGIC To bring the data together, we need to match records in one temporary view or table with another temporary view or table.  Telling Spark how to match these records is known as a join in SQL.
# MAGIC
# MAGIC In This notebook we will start with the default join which is an INNER JOIN, but we will discuss all of the following:
# MAGIC * INNER JOIN
# MAGIC * LEFT OUTER JOIN
# MAGIC * FULL OUTER JOIN
# MAGIC * LEFT SEMI JOIN
# MAGIC * LEFT ANTI JOIN
# MAGIC
# MAGIC If you are right-handed, you may be wondering what the deal is with all this lefty stuff.  When we join two data sources, the JOIN expression is between the two temporary views or tables, and the data source before the JOIN is on the left of the join expression, and the data source after the JOIN is on the right-hand side of the join expression.  In the following example, the `categories` view  is on the left, and the `business` view is on the right, with the join expression between them being an `INNER JOIN`:
# MAGIC
# MAGIC `categories INNER JOIN business`
# MAGIC
# MAGIC ### In This Notebook: 
# MAGIC You will bring together the business data, the business category definitions, and the metro area for each business.
# MAGIC
# MAGIC At the end of the notebook, you will generate a table that has a row for each business and identifies which metro area it is in and what top-level categories it operates in (e.g., Restaurants, Shopping, Active Life, etc.).
# MAGIC
# MAGIC To do this, we will have the following joins:
# MAGIC
# MAGIC <img src="https://www.sjsu.edu/people/scott.jensen/courses/categories-business-metro.png"/>

# COMMAND ----------

# MAGIC %md # Identifying the Metro Area for Each Business
# MAGIC
# MAGIC Previously we used Tableau to identify the metro areas by lassoing the geographic clusters of businesses and identifying the metro area.  A calculated field was then added in Tableau that provided a text label for the metro area.
# MAGIC
# MAGIC When we added that calculated field in Tableau, it became a new ***column*** in the data Source tab in Tableau.  We exported the Data Source from Tableau, and we included only the business_id, metro area, and state.
# MAGIC
# MAGIC Before we can add the metro area column to our business data, we need to load the business data and do some data wrangling to structure our data.
# MAGIC
# MAGIC ### Step 1a: Loading the Business Data

# COMMAND ----------

df_business_data = spark.read.json('/yelp/business.bz2')

print ("record count:", df_business_data.count() )
df_business_data.show(100)
df_business_data.printSchema()

df_business_data.createOrReplaceTempView("business_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1b: Data Wrangling - Structuring Our Data
# MAGIC
# MAGIC We need to decide what data we need for each business
# MAGIC
# MAGIC This process is iterative, but for now we are going to include the following:
# MAGIC * business_id (so we can join with the metro area definitions and at some later point, with the review data)
# MAGIC * name (something human readable)
# MAGIC * categories (so we can identify the type of business and join with the category data)
# MAGIC * state 
# MAGIC * city
# MAGIC * review_count (so we can decide how much data we have for each business)
# MAGIC
# MAGIC **<span style="color:red; font-size:1.5em;">NOTE:</span>**  If your team decides to extend the project question by looking at any attributes, you will need those fields here
# MAGIC
# MAGIC #### The following cell is Step 1b:

# COMMAND ----------

df_business = spark.sql("""
SELECT business_id, name, categories, state, city, review_count
FROM business_data
""")
print ("record count:", df_business.count() )
df_business.show(truncate=22)
df_business.createOrReplaceTempView("business")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2a: Load the Metro Area Definitions into DBFS
# MAGIC Based on mapping the data in Tableau, we identified the 11 metro areas in the data and which businesses were in which metro areas.
# MAGIC
# MAGIC **NOTE:** We need to first load the metro areas.csv file into DBFS and move it to our `/yelp` directory.
# MAGIC
# MAGIC **The process:**
# MAGIC * Download the `metro_areas.csv` file from Canvas
# MAGIC * Upload it to DBFS from the Data tab in Databricks. By default, it will be uploaded to the directory `/FileStore/tables` in DBFS.
# MAGIC * Using dbutils, move the `metro_areas.csv` file from the `/FileStore/tables` directory to the `/yelp` directory.
# MAGIC
# MAGIC ####The following cell is Step 2a
# MAGIC

# COMMAND ----------

# Add your code to move the file after uploading it to /FileStore/tables
dbutils.fs.mv("/FileStore/tables/metro_areas.csv","/yelp/metro_areas.csv")

# COMMAND ----------

# MAGIC %md ### Step 2b: Loading the Metro Area Data into a DataFrame
# MAGIC
# MAGIC This step assumes there is a CSV file named `metro_areas.csv` in the `/yelp` directory on DBFS that contains the following two fields:
# MAGIC * `Business Id` (to match the business_id in the business data)
# MAGIC * `Metro Area` - containing the name of the metro area (there should be 11 metro areas)
# MAGIC
# MAGIC As long as the file contains these two fields, additional fields are fine.
# MAGIC
# MAGIC In the following code we used the PySpark method `withColumnRenamed` to rename the columns for the Business Id and Metro Area columns. In Tableau, spaces in our variable names did not matter, and made the titles more readable in our charts. However, in Spark we don't want spaces in our variable names and we want them to be lower case.  Keep in mind that reading the file in created a DataFrame.  That DataFrame was passed to `withColumnRenamed("Business Id","business_id")`, which generated a ***new*** DataFrame with the column name changed, and that DataFrame was passed to `withColumnRenamed("Metro Area","metro_area")`, which also generated a ***new*** DataFrame, and that DataFrame was assigned to the variable `df_metro_areas`.
# MAGIC
# MAGIC **NOTE:** If we were going to actually use the `State` column in the data being loaded, we might also want to rename that column to be lower case.
# MAGIC
# MAGIC #### The following cell is Step 2b:

# COMMAND ----------

df_metro_areas = spark.read.option("header","true").\
                            option("inferSchema","true").\
csv("/yelp/metro_areas.csv").\
withColumnRenamed("Business Id","business_id").\
withColumnRenamed("Metro Area","metro_area")

print("Number of records:", df_metro_areas.count() )
df_metro_areas.show(truncate=False)
df_metro_areas.printSchema()
df_metro_areas.createOrReplaceTempView("metro_areas")

# COMMAND ----------

# MAGIC %md ### Step 3: Identifying the metro area for each business
# MAGIC
# MAGIC In the following cell we match the business data with the metro area data to identify which of the 11 metro areas each business is in.
# MAGIC
# MAGIC **Keep in mind:**
# MAGIC
# MAGIC * We originally loaded the Yelp business file into Tableau (the same file we loaded in an earlier step in this notebook)
# MAGIC * Each business is included on a separate line in the metro_areas.csv file we loaded, and each line has the business's business_id
# MAGIC
# MAGIC #### What type of JOIN do We need?
# MAGIC
# MAGIC #### Which fields do we need from each data source?
# MAGIC
# MAGIC #### The following cell is Step 3:

# COMMAND ----------

# Add the Query
df_metro_business = spark.sql("""
select B.*, metro_area
from business as B 
    INNER JOIN metro_areas as M 
    on B.business_id = M.business_id
""")
print("Number of records:", df_metro_business.count() )
df_metro_business.show()
df_metro_business.createOrReplaceTempView("metro_business")

# COMMAND ----------

# MAGIC %md # Identifying the Top-Level Categories Each Business Is In
# MAGIC
# MAGIC Businesses on Yelp can identify themselves as being in any of over 1,500 categories.  These categories form a hierarchy, with 22 top-level categories at the top of the hierarchy, and then these break down into multiple levels of detail.  If a business identifies itself as being in one of the detailed categories lower in the hierarchy, all of the categories on the path up to and including one of the 22 top-level categories will be included in the `categories` field in that business's data.
# MAGIC
# MAGIC For example, if a martial arts school identifies itself as being in the business of `Karate`, that category is in the following hierarchy:
# MAGIC
# MAGIC `Active Life > Fitness & Instruction > Martial Arts > Karate`
# MAGIC
# MAGIC In that case, all 4 of those categories are included in the `categories` field in the data for that business.  The `Active Life` category is the one we want to identify that business with, we are not going to concern ourselves with the detailed categories.  However, if that business also sold martial arts equipment, it may also classify itself as being in the category `Shopping > Fitness/Exercise Equipment`.  In that case, the business is identified as being in two top-level categories: `Active Life` and `Shopping`.
# MAGIC
# MAGIC ### What the Category Data Contains
# MAGIC
# MAGIC Following is a description of the fields in the categories data:
# MAGIC * **alias**:  This is the value used in the `parents` field to identify the parent-child relationships in the category hierarchy. 
# MAGIC * **country_blacklist**:  A category may have a white list or black list, but not both.  This field is either a list of 2-digit country codes or null. if this field contains a list of country codes, then the category can be used in any country except those in the list.  For example, the cuisine "Afghan" has the blacklist `[MX, TR]`, which means this category is not used in Mexico or Turkey.
# MAGIC * **country_whitelist**:  If there is a list in this field, the category only is used in those countries.  For example, the category "Absinthe Bars", which is a sub-category under "Bars" has a whitelist with one country `[CZ]` which is the Czech Republic.  This category should only be used to describe businesses in that country.
# MAGIC * **parents**: The categories form a hierarchy, so the broader parent category above each category is a list in this field.  Most categories have one parent category. If the list is empty `[ ]` for a category (such as `Active Life`), then the category is a top-level category and there is no parent category.
# MAGIC * **title**:  This is the human-readable label for the category and is also the label included in the list of categories for a business in the `categories` field in the Yelp business data.
# MAGIC
# MAGIC ### Step 4: Loading the Category Data
# MAGIC This is the category definitions JSON file we downloaded from the webpage for Yelp's fusion API at <a href="https://www.yelp.com/developers/documentation/v3/all_category_list" target="_blank">this URL</a>.
# MAGIC
# MAGIC We are going to load this data from a JSON file, so similar to loading the business data, but with one difference.  The Yelp data is JSON Lines, where each line of the file is a JSON object.  The categories data is a single JSON array of JSON objects, but each object is not on a single line, but is instead "pretty printed" like our human readable sample data.  When we are loading JSON where each object spans multiple lines in the file, we need to set an option to tell Spark our file is `multiline`.
# MAGIC
# MAGIC #### What if I get a "Path does not exist" error?
# MAGIC If that occurs, you did not successfully run Part 2 of the "Working with Files" notebook.  You will need to upload the categories file.
# MAGIC
# MAGIC #### The following cell is Step 4

# COMMAND ----------

df_categories = spark.read.option("multiLine",True).json("/yelp/categories.json")
print( "number of categories:", df_categories.count() )
df_categories.show()
df_categories.printSchema()
df_categories.createOrReplaceTempView("categories")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 5: How many top-level categories are there?
# MAGIC
# MAGIC
# MAGIC Take a look at your earlier code for showing the first 20 rows of the category data.  "Active Life" is one of the top-level categories (and the first category listed on <a href="https://www.yelp.com/developers/documentation/v3/category_list" target="_blank">this</a> page.  What's different about the list in the `parents` column for that category?
# MAGIC
# MAGIC In Spark there's a <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.size.html#pyspark-sql-functions-size" target="_blank">SIZE </a> function that will return the number of elements in an array.  The function takes one parameter, the name of the column containing the array.
# MAGIC
# MAGIC In the following query, use the `SIZE` function in the `WHERE` clause to get only those categories that have no parents.  We will use this DataFrame to find out what top-level category(s) each business is in.
# MAGIC
# MAGIC **NOTE:** Although we are using the `parents` field in the `WHERE` clause, we are not including it in the `SELECT` clause. All we care about is getting the titles of the top-level categories, so that's all we include in the `SELECT` clause.
# MAGIC
# MAGIC #### The following cell is Step 5

# COMMAND ----------

# Finish the WHERE clause
df_top_categories = spark.sql("""
SELECT title
FROM categories
WHERE SIZE(parents) = 0
""")
print(f"Number of top-level categories: {df_top_categories.count()}")
df_top_categories.show(30, truncate=False)
df_top_categories.createOrReplaceTempView("top_categories")

# COMMAND ----------

# MAGIC %md ### <span style="color:#0055A2;">Step 6: The metadata is out-of-date? (Team Assignment)</span>
# MAGIC You should have found that there are 22 top-level categories.  The total count agrees with <a href="https://blog.yelp.com/businesses/yelp_category_list" target="_blank">this Yelp blog post</a> that lists all of the top-level categories and allows you to drill down to lower levels (it was updated in Spring 2020). However, take a look at the *names* of the top 22 categories on that blog post and compare it to your DataFrame above.  "Bicycles" is listed as a top-level category in your DataFrame, but not in the blog listing.  The blog lists "Real Estate" as a top-level category, but not your DataFrame.  If you take a look at the metadata where we downloaded the catagory definitions (click <a href="https://www.yelp.com/developers/documentation/v3/all_category_list" target="_blank">here</a>), You will find that ***both*** "Bicycles" and "Real Estate" are shown as top-level categories.
# MAGIC
# MAGIC In the following cell, **<span style="color:red;">Work with your team - add a query to find out what category is the parent of Real Estate in your data</span>**.
# MAGIC
# MAGIC #### <span style="color:#0055A2;">The following cell is Step 6</span>

# COMMAND ----------

# Add a query to find the row for Real Estate
df_real_estate = spark.sql("""
SELECT *
FROM categories
WHERE title = "Real Estate"
""")

df_real_estate.show()
df_real_estate.createOrReplaceTempView("real_estate")



# COMMAND ----------

# MAGIC %md ### Step 7: Identifying the categories a business is in
# MAGIC
# MAGIC If we want to join the business data with the `df_top_categories` temporary view we created earlier, there is a Spark function named `array_contains` that allows us to see if a value is in our array (list), but the business categories field is not an array, it's a comma-separated list (scroll up to where you showed this earlier).
# MAGIC
# MAGIC #### Splitting a string to generate an array
# MAGIC Spark has another function we can use to first transform our data; the <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.split.html#pyspark-sql-functions-split" target="_blank">SPLIT function</a>.  This is a string function that will take the value in a string column and split it on some delimiter
# MAGIC (in our case, a comma) and return a list of the values.  The delimiter is a string pattern, so it should be enclosed in single quote marks. We have another issue however, since the string in business categories has spaces after 
# MAGIC the commas, we need to trim off the spaces.
# MAGIC
# MAGIC #### Regular expressions
# MAGIC The `SPLIT` function's pattern can be what is known as a regular expression, and these are often used when matching text.  What we want is to split on a comma surrounded by any amount of leading or trailing blank space (so the blanks don't end up in the values we get back).  To do this, the pattern `\s` means a blank space, and we can use the wildcard `*` to mean "zero or more", so the pattern `\s*,\s*` means a comma surrounded by any amount of blank space.
# MAGIC
# MAGIC The `\s*` is really important - this means that when we "throw away" the commas, we are also throwing away any blank spaces that would also cause us problems.
# MAGIC
# MAGIC Since we are using SQL, we have one other issue.  The `\` is known as an "escape" character and has a special meaning, and in SQL, to say we **really** want a `\`, because it's part of our pattern `\s`, we need to "escape" the `\`, and in SQL, you "escape" a character by preceding it with two backslashes `\\`, so instead of using `\s` in our regular expression pattern, we need to use `\\\s` (three backslashes instead of one). 
# MAGIC
# MAGIC In the following cell we have selected all of the columns from the business data except for the categories column.  <span style="color:red">Add a formula to the `SELECT` clause that uses the `SPLIT` function to split on the commas and possible whitespaces</span>. **Alias the column's formula as categories**.
# MAGIC
# MAGIC #### The following cell is Step 7

# COMMAND ----------

# Add the formula for the categories column based on the SPLIT function
df_business_categories = spark.sql("""
SELECT business_id, name, state, city, metro_area, review_count, 
SPLIT(categories,'\\\s*,\\\s') AS categories
FROM metro_business
""")
df_business_categories.show()
df_business_categories.select("business_id", "name","categories").show(truncate=False)
df_business_categories.printSchema()
df_business_categories.createOrReplaceTempView("business_categories")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7a: Reviewing the category arrays
# MAGIC
# MAGIC Since the category arrays get truncated when we use the `show` method, in the following cell we are only showing the `business_id` and the `categories` array.

# COMMAND ----------

df_business_categories.select("business_id","categories").show(truncate=False)


# COMMAND ----------

# MAGIC %md ### Step 8 Identifying businesses in top-level categories using ARRAY_CONTAINS
# MAGIC
# MAGIC The `business_categories` temporary view has all of the businesses in the 11 metro areas.  The categories column (unless null) has all of the categories a business is in, using the same labels and format as the `title` field in the `top_categories` to the extent those categories are top-level categories.
# MAGIC
# MAGIC **What We Want to Do:** We want each top-level category a business is in to match with the corresponding title in `top_categories`
# MAGIC
# MAGIC **What We CANNOT Do:** We can't say, `business_categories.categories = top_categories.title`, because we can't compare a string (`title`) to an array of strings (`categories`)
# MAGIC
# MAGIC **What We *CAN* Do:*** Spark has an <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.array_contains.html#pyspark-sql-functions-array-contains" target="_blank">ARRAY_CONTAINS() function</a> that takes two parameters:
# MAGIC * the name of the column with the array
# MAGIC * the value we want to check for
# MAGIC
# MAGIC The function returns a Boolean value: True / False 
# MAGIC
# MAGIC Instead of using `=` in the `ON` condition for our join, we can use a formula based on the `ARRAY_CONTAINS` function that returns a Boolean, because that's what our `ON` condition needs.
# MAGIC
# MAGIC ### What to Do:
# MAGIC * We need to set the title field to "Unknown" if a business has no categories (or no top-level categories)
# MAGIC * Determine what type of join is needed
# MAGIC * Determine the condition for the join
# MAGIC * Sort based on the number of categories a business is in (descending)
# MAGIC
# MAGIC #### The following cell is Step 8

# COMMAND ----------

# Complete the FROM (JOIN),  SELECT, and ORDER BY clauses
df_business_top_categories = spark.sql("""
SELECT business_id, name, state, city, metro_area, review_count, 
        COALESCE(title,'Unknown') AS category
FROM  business_categories AS B LEFT OUTER JOIN top_categories AS T
ON ARRAY_CONTAINS(categories,title) 
ORDER BY SIZE (categories) DESC
""")
print(f"number of businesses:{df_business_top_categories.count()}")
df_business_top_categories.show(100, truncate=30)
df_business_top_categories.createOrReplaceTempView("business_top_categories")


# COMMAND ----------

# MAGIC %md ### Step 9: Check if the number of businesses is the same
# MAGIC When we did our join in Step 8, if a business had 3 top categories, it now has 3 rows. If `categories` was null, then an INNER JOIN would not include those businesses, but an OUTER JOIN would.  
# MAGIC #### The following cell is Step 9

# COMMAND ----------

print(f"Total Businesses:{df_business_top_categories.select('business_id').distinct().count()}")
  
print("Businesses without categories:",df_business_top_categories.filter("category = 'Unknown'").count() )

# COMMAND ----------

# MAGIC %md #<span style="color:#0055A2;">Step 10: Determining how many top-level categories each business is in (Team Assignment)</span>
# MAGIC
# MAGIC Some businesses are in multiple top-level categories as we saw in Step 8, in this step your team will be determining how many top-level categories each business is in and adding that to the `df_business_top_category_count` DataFrame and then regenerating the table based on that DataFrame.
# MAGIC
# MAGIC The new column should be named `category_count`.
# MAGIC
# MAGIC ####Suggestions:
# MAGIC
# MAGIC There are 2 possible approaches:
# MAGIC 1. Create an additional query ***before*** the query generating `df_business_top_category_count`.  In that query get the top-level category count for each business, then in the query for `df_business_top_category_count`, do a join to add that data as another column.
# MAGIC 2. Use a window function.  We will cover these in a future week (so be sure your team has figured out solution 1 before tackling this).  The window function will allow you to calculate the count as part of the query for `df_business_top_category_count`. 
# MAGIC
# MAGIC **NOTE:** After your team adds the `category_count` column, Step 11 to write the table out should be re-run, but for that re-run the `delete` parameter in the call to the `process_or_create_table` function should be set to `True`. After the table has been rewritten, the `delete` parameter should be set back to `False`.

# COMMAND ----------

# Add a column with the count of the number of top-level categories a business is in
# REMOVE WINDOW FUNCTION FOR category_count AND ORDER BY CLAUSE
df_business_top_category_count = spark.sql("""
SELECT *, COUNT(category) OVER(PARTITION BY business_id) AS category_count          
FROM business_top_categories 
ORDER BY category_count DESC, business_id
""")
df_business_top_category_count.show(100, truncate=30)
df_business_top_category_count.createOrReplaceTempView("business_top_categories_count")

# COMMAND ----------

# MAGIC %md ### Step 11: Write the wrangled business data to a table
# MAGIC As a separate notebook, this is generating the data we need from the business, metro area, and category data and writing it out to a table named `business_category_table`.  Unless this needs to change, we will not need to regenerate this table, but just reload the table files from any notebook using the table.
# MAGIC
# MAGIC For writing out the table, functions from the `Process or Create Table` notebook are first embedded here.
# MAGIC
# MAGIC In the future, if we need to force the table to be rebuilt, the `delete` parameter in the call to the `process_or_create_table` function should be set to `True`.
# MAGIC
# MAGIC #### The following cell is Step 11a:

# COMMAND ----------

# MAGIC %run "./Process or Create Table"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 11b: Write the table

# COMMAND ----------

process_or_create_table("business_category_table", "df_business_top_category_count", summary=True, delete=False)

# COMMAND ----------

# MAGIC %md # <span style="color:#0055A2;">Step 12: Some Visual Profiling (Team Assignment)</span>
# MAGIC
# MAGIC Now that we have a row for each business for each top-level category it's in, add queries that calculate and visualize the following answers:
# MAGIC
# MAGIC * Step 10a: How many businesses are in each top-level category?  Show each top-level category and a count.  Some businesses will be in multiple categories  (this should be one query).  Use the DataFrame to then generate a bar chart using the `display()` function.
# MAGIC * Step 10b: Some businesses are in multiple top-level categories as your team calculated above in step 10.  Show the number of businesses in 1 - 22 top-level categories (none will actually be in all 22).   Use the DataFrame to then generate a bar chart using the `display()` function.
# MAGIC
# MAGIC #### <span style="color:#0055A2;">Add the query for 12a in the following cell</span>

# COMMAND ----------

# Step 12a: Add your query here to answer 12a
df_business_top_level_category_count = spark.sql("""
SELECT category, COUNT(business_id) as business_count
FROM business_category_table
GROUP BY category
ORDER BY business_count DESC
""")
df_business_top_level_category_count.show(100,truncate=30)


# COMMAND ----------

# MAGIC %md
# MAGIC #### <span style="color:#0055A2;">Add the call to the `display()` function for 12a in the following cell</span>

# COMMAND ----------

display(df_business_top_level_category_count)

# COMMAND ----------

# MAGIC %md
# MAGIC #### <span style="color:#0055A2;">Add the query for 12b in the following cell</span>

# COMMAND ----------

# Step 12b: Add your query here to answer 12b
df_business_category_count = spark.sql("""
SELECT category_count, COUNT(business_id) AS business_count
FROM business_category_table
GROUP BY category_count
ORDER BY category_count
""")
df_business_category_count.show(100,truncate=30)

# COMMAND ----------

# MAGIC %md
# MAGIC #### <span style="color:#0055A2;">Add the call to the `display()`function for 12b in the following cell</span>

# COMMAND ----------

display(df_business_category_count)

# COMMAND ----------

# MAGIC %md # Submitting your notebook
# MAGIC * Be sure the name of the notebook starts with your team name
# MAGIC * Be sure your queries are formatted well:
# MAGIC   * Clauses, functions, and keywords should be all uppercase
# MAGIC   * Field names should be lowercase
# MAGIC   * Each major clause should start on it's own line
# MAGIC   * There should not be excess blank lines in your queries
# MAGIC   * Variable names and column names should be meaingful