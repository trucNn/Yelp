# Databricks notebook source
# MAGIC %md 
# MAGIC VICTOR NGUYEN, SWINSON CAI, REX TANG, CHLOE LIM, RONALD MARTINEZ, TRUC NGUYEN
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC Copyright &copy; 2022 Scott Jensen, San Jose State University
# MAGIC
# MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-sa/4.0/88x31.png" /></a><br /><span xmlns:dct="http://purl.org/dc/terms/" property="dct:title">This notebook</span> by <span xmlns:cc="http://creativecommons.org/ns#" property="cc:attributionName">Scott Jensen,Ph.D.</span> is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/">Creative Commons Attribution-ShareAlike 4.0 International License</a>.

# COMMAND ----------

# MAGIC %md # Bringing All of Our Data Together - User and Gender
# MAGIC
# MAGIC #### The ***main*** question we are asking of the Yelp data is:<br/>
# MAGIC *<div style="margin-left:50px;max-width:700px;background-color:#39506B;"><div style="text-align:center; margin:20px;font-size:1.5em;color:#ffffff">Do women and men join Yelp for different reasons?</div></div>*
# MAGIC
# MAGIC ***Our hypothesis is:*** the first reviews users post indicate why they joined Yelp.  Do women and men join Yelp for different reasons?  Does one join to praise and the other to flame?  Does one join to review restaurants and the other joins to review bars?
# MAGIC
# MAGIC
# MAGIC In this notebook we are going to bring together the Yelp user data and name data from the Social Security Administration (SSA):
# MAGIC * This notebook is based on the user table created in the **Working with Files** exercise and the gender data from the data wrangling exercise. 
# MAGIC * The predominant gender of each name in the SSA data was determined in the wrangling notebook which was written as `ssa_gender_table`.
# MAGIC * The gender of Yelp users is predicted based on joining with the `ssa_gender_table`.
# MAGIC * In this notebook you also identify if users were ever elite, and if so, when they were first elite
# MAGIC   * During that process, a data error for the elite field needed to be fixed
# MAGIC * The result of this notebook is written out as `user_gender_table`
# MAGIC
# MAGIC **<span style="font-size:1.2em;">The connections between our data files are as follows:</span>**<br/>
# MAGIC <img src="https://www.sjsu.edu/people/scott.jensen/courses/BUS4_118D/user-gender.png"/>

# COMMAND ----------

# MAGIC %md # Step 1: Loading the User Data 
# MAGIC Although the focus of our question is on the first reviews users wrote and their gender, we may also want to consider whether the first reviews indicate 
# MAGIC whether men or women are more likely to become elite users or the number of years that elapse between joining and becoming elite.
# MAGIC
# MAGIC The gender of each user will be based on the gender ratios calculated in the data wrangling exercise based on the Social Security Administration (SSA) data.
# MAGIC
# MAGIC The following cell attempts to rebuild the table named `user_table` that was created in the **Working with Files** exercise.
# MAGIC
# MAGIC If that table was not previously generated, an error will occur and part 4 of that notebook will need to be run.
# MAGIC
# MAGIC Since the `friends` field can contain a lot of user IDs, that field was excluded in building the `user_table` and instead a count of the number of Yelp friends a user has was included.
# MAGIC
# MAGIC Step 1a first loads a notebook used to rebuild or create tables.  Depending on whether a table needs to be created or reloaded (in this case, reloaded), the Spark engine seems to optimize the code for the path taken (create or rebuild), so if used to create some tables and rebuild others, the notebook needs to be embedded again.  After running the step 1a, you may want to hide the output, but make sure it has completed running first.
# MAGIC
# MAGIC #### The following cell is Step 1a:
# MAGIC

# COMMAND ----------

# MAGIC %run "./Process or Create Table"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1b: Loading the `user_table`
# MAGIC
# MAGIC Since the table is assumed to have already been created, no DataFrame name is passed for the second parameter of the `process_or_create_table` function and the `delete` parameter must be `False` or it will delete your `user_table`.

# COMMAND ----------

process_or_create_table("user_table", None, summary=True, delete=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Fixing an Error in the User Data
# MAGIC Any dataset has some errors - the Yelp data is actually cleaner than most datasets you will encounter in your career, but it has some issues.
# MAGIC
# MAGIC The elite field is a string - if you are not sure, insert a new cell and run the following SQL query:
# MAGIC
# MAGIC <code>spark.sql("""<br/>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;DESCRIBE user_table<br/>
# MAGIC """).show()
# MAGIC </code>
# MAGIC
# MAGIC To see the issue, in the following cell select just the `user_id`, `name`, and `elite` fields from the `user_table`.  Show 100 rows, and be sure to set `truncate=False` in the call to the `show()` method at the end of the query.
# MAGIC
# MAGIC #### The following cell is Step 2a

# COMMAND ----------

# Add your query here
spark.sql("""
SELECT user_id, name, elite
FROM user_table
""").show(100,truncate=False)



# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2b: Fixing the data error
# MAGIC If the user table was created for multiple data analysts or data scientists to use, we would want to fix this in the code that created the `user_table`.  For our purposes we will fix it in this notebook.
# MAGIC
# MAGIC #### Did you see the error?
# MAGIC In case you did not see it, take a look at some of the elite users who were elite in 2020.  One of those was "Margie", who had the following value for the elite field: `2017,2018,2019,20,20,2021`.  Whoever dumped the data for the dataset appears to have parsed the years a user is elite by parsing on the pattern `20`, which would work until 2020 since Yelp started in 2004.
# MAGIC
# MAGIC #### Fixing the error:
# MAGIC Since the `elite` field is a string, we can fix it using regular expressions - which allow us to match patterns in a string.  
# MAGIC
# MAGIC * The function we will use is named REGEXP_REPLACE which takes three parameters:
# MAGIC   * The name of the column containing a `string` value we want to change
# MAGIC   * A string with the pattern we are looking for that we want to replace (the pattern can contain regular expressions, such as we used when splitting categories)
# MAGIC   * The string we want to replace the pattern
# MAGIC   
# MAGIC In the following cell use the `REGEXP_REPLACE` function to add a column that fixes the `elite` column.  Alias the new column as `elite`.
# MAGIC
# MAGIC #### The following cell is Step 2b: 

# COMMAND ----------

# Add the formula for the elite column
df_clean_users = spark.sql("""
SELECT user_id, name, yelping_since, review_count, friend_count,
      REGEXP_REPLACE(elite,'20,20','2020') AS elite 
FROM user_table
""")
print(f"record count: {df_clean_users.count()}")
df_clean_users.show(truncate=False)
df_clean_users.createOrReplaceTempView("clean_users")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Identifying the years a user was elite
# MAGIC
# MAGIC The user data contains a comma-separated string with the 4-digit years a user was elite.  By converting this to an array, we can identify whether a user was elite the year each review was written.
# MAGIC
# MAGIC Since it may be that the reviewing style of elite users is different, even before they were elite, we also set a boolean `is_elite` field we can use to identify if a review was written by a user that was ever elite (even if not in the year they wrote a review or not until a future year).  It could also be that when becoming elite, a user's reviewing style changes, so we also track the first year they became elite as `first_elite` (which will be null if they are never elite).
# MAGIC
# MAGIC #### Common Table Expressions (CTE):
# MAGIC Here we are using a common table expression (CTE). You may also see these referred to as a `WITH` clause (arguably a better name since they start with `WITH`)
# MAGIC
# MAGIC The CTE allows us to define one or more temporary tables within our query.  This is somewhat like using subqueries, but are generally easier to read and if the temporary result would be used more than once (within the same query), you only need to define it once.  In Spark, since each query generates a DataFrame, and we then need to create a new temporary view if we want to use the result in another query, the CTE will allow us to do these steps all in one query.
# MAGIC
# MAGIC **Format of a CTE:**<br/>
# MAGIC A CTE will contain at least one temporary result defined using `WITH` and then the main query.  The `WITH` can define multiple temporary results that are comma-separated and they can reference prior temporary results in that same query.  In the following example, there are two temporary results.  In your CTE's, the temporary results should have more meaningful names (like your DataFrames and temporary views in general).  Notice that just as in a SELECT clause, there is no comma after the last temporary result in the CTE.<br/>
# MAGIC <div style="padding:10px; border:solid 1px black;">
# MAGIC WITH temporary_name_a AS<br/>
# MAGIC &nbsp;&nbsp;(SELECT ...<br/>
# MAGIC &nbsp;&nbsp;&nbsp;FROM ...),<br/>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;temporary_name_b AS<br/>
# MAGIC &nbsp;&nbsp;(SELECT ...<br/>
# MAGIC &nbsp;&nbsp;&nbsp;FROM ...)<br/>
# MAGIC SELECT ...<br/>
# MAGIC FROM temporary_name_a AS A INNER JOIN temporary_name_b AS B<br/>
# MAGIC ON A.some_field = B.some_field
# MAGIC </div>
# MAGIC
# MAGIC In the above example we have two temporary results and we join them in the main query, but they could build on each other and there is no requirement that the main query do a join, it just needs to be built on at least one of the temporary results.
# MAGIC
# MAGIC ### Parsing the Elite Array:<br/>
# MAGIC In this step we will use a CTE to:<br/>
# MAGIC * Parse the elite field using the `SPLIT` function to convert the `elite` comma-separate string into an array
# MAGIC * Flatten the array using the `EXPLODE` function to create one line for each year a user is elite
# MAGIC * Convert the string values for each elite year into an integer using the `CAST` function
# MAGIC * Collapse the multiple elite years for each user back into an array of integers using the `COLLECT_LIST` function
# MAGIC
# MAGIC #### The following cell is Step 3:

# COMMAND ----------

# Build Your Query - First CTE to create an array
# SPLIT(elite,"\\\s*,\\\s*") AS elite_years__ always give an array 

df_users = spark.sql("""
WITH elite_array AS 
     (SELECT user_id, SPLIT(elite,"\\\s*,\\\s*") AS elite_years
      FROM clean_users),
     
    exploded_elite AS 
    (SELECT user_id, EXPLODE(elite_years) AS elite_years
    FROM elite_array),
    
    converted_elite AS
    (SELECT user_id, CAST(elite_years AS Integer)
    FROM exploded_elite),
     
     collected_elite AS
     (SELECT user_id, COLLECT_LIST(elite_years) AS elite_years
     FROM converted_elite
     GROUP BY user_id)
       
     
SELECT U.user_id, U.name, U.yelping_since, 
       U.review_count AS reviews_written, U.friend_count, 
       ARRAY_SORT(E.elite_years) AS elite_years,
       SIZE(E.elite_years)>0 AS is_elite,
       ARRAY_MIN(elite_years) AS first_elite
       
FROM clean_users AS U INNER JOIN collected_elite AS E
ON U.user_id = E.user_id
""")
print(f"record count: {df_users.count()}")
df_users.printSchema()
df_users.show(100, truncate=False)
df_users.createOrReplaceTempView("users")

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Step 4: Recreating the `ssa_gender_table`
# MAGIC
# MAGIC In the wrangling exercise, we started with the SSA data and assigned a gender (M or F) to each name based on whether there were more women or men in the data with that name.  Along with a gender ratio, that data was written out to the `ssa_gender_table`.
# MAGIC
# MAGIC Since you most likely did that exercise on a different cluster, the table definition was deleted when the cluster terminated, but the files generated when we created the `user_table` are still stored in user/hive/warehouse on DBFS, so we can recreate that table as we did for the user table.
# MAGIC
# MAGIC Since how the imported notebook executes depends on the Spark optimizer, we first need to re-run the `Process or Create Table` notebook used to define the `process_or_create_table` function.  Run step 4a to run that notebook.  You may want to hide the result for that cell after it completes.
# MAGIC
# MAGIC #### The following cell is Step 4a:

# COMMAND ----------

# MAGIC %run "./Process or Create Table"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4b: Recreate the `ssa_gender_table`
# MAGIC Run the following cell to rebuild the `ssa_gender_table` created in the wrangling exercise.  If the table files do not exist, you will need to first complete that earlier exercise.

# COMMAND ----------

process_or_create_table("ssa_gender_table", None, summary=True, delete=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Predicting the gender of Yelp users
# MAGIC Based on the SSA data, the names of Yelp users, to the extent they match with names in the SSA data will be identified as male or female names.
# MAGIC
# MAGIC When matching names, the case of the name in the Yelp user data is disregarded.
# MAGIC
# MAGIC If a user's name does not have a match in the SSA data, the gender will be set to "unknown" and the gender ratio will be set to 0 (it could reasonably be set to either 0 to indicate no gender data or to 1 to indicate we are 100% sure the gender is unknown).
# MAGIC
# MAGIC In the `ON` condition for our matching names from the user data and the SSA data (that's in the `ssa_gender_table`), we should user the `LOWER()` string function to make sure we are comparing the same names (e.g., "Trinity", "trinity", and "TRINITY" would not match otherwise).
# MAGIC
# MAGIC ### What type of JOIN should we use?
# MAGIC * Do we only want users who have a name in the Social Security data?
# MAGIC * Do we want names where there is no Yelp user with that name?
# MAGIC
# MAGIC ### What fields do we need to check for nulls?
# MAGIC
# MAGIC In the following cell:
# MAGIC * Add the join in the FROM clause and the ON condition for the join
# MAGIC * Make any necessary adjustments for nulls
# MAGIC
# MAGIC #### The following cell is Step 5:

# COMMAND ----------

# Add the FROM clause with the JOIN and the ON condition for the JOIN
# Adjust for null values as needed
df_user_gender = spark.sql("""
SELECT U.*, 
       COALESCE(gender,'unknow') AS gender, 
       COALESCE(gender_ratio,1.0) AS gender_ration
FROM users AS U LEFT OUTER JOIN ssa_gender_table AS G
ON LOWER(U.name) = LOWER(G.name)
 """)
print(f"Record count: {df_user_gender.count()}")
df_user_gender.show(truncate=False)
df_user_gender.createOrReplaceTempView("user_gender")

# COMMAND ----------

# MAGIC %md ### Step 6: Write the wrangled user and gender data to a table
# MAGIC So we can easily use the results of this notebook in other notebooks (without recalculating this notebook), we will write our DataFrame out to a table named `user_gender_table`.  Unless this needs to change, we will not need to regenerate this table, but just reload the table from the files stored in /user/hive/warehouse.
# MAGIC
# MAGIC In the future, if we need to force the table to be rebuilt, the `delete` parameter in Step 6b should be set to `True`.
# MAGIC
# MAGIC For writing out the table, functions from the `Process or Create Table` notebook are first embedded here.
# MAGIC
# MAGIC As discussed above, we need to again embed the notebook used to create or reload a table.
# MAGIC
# MAGIC #### The following cell is Step 6a:

# COMMAND ----------

# MAGIC %run "./Process or Create Table"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6b: Generate the `user_gender_table`

# COMMAND ----------

process_or_create_table("user_gender_table", "df_user_gender", summary=True, delete=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7: How did the SSA data work at predicting gender?
# MAGIC Since we just did a major transformation, we should be doing some profiling to see how it worked (and if we lost data unexpectedly).
# MAGIC
# MAGIC In the following cell, add a query to summarize the number of records based on the `gender` field
# MAGIC
# MAGIC **<span style="color:red; font-size:1.5em;">NOTE:</span>**  For this exercise we are addressing the case of the names in the Yelp data and the SSA data, but if you were doing this as your career, you would want to see how else you could reduce the number of users of "unknown" gender.  One issue is that if a name has any whitespace (e.g., "Peggy Sue", the SSA dataset takes out the whitespace as "Peggysue").
# MAGIC
# MAGIC #### The following cell is Step 7a:

# COMMAND ----------

# Add your query here
df_user_gender = spark.sql("""
SELECT gender, COUNT(gender) as user_count,
               ROUND(COUNT(first_elite)/COUNT(gender),4) as elite_percentage,
               SUM(reviews_written) as reviews_written
FROM user_gender
GROUP BY gender
""")
df_user_gender.show(truncate=False)


# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 7b: Visual Profiling
# MAGIC We only have 3 values (women, men, and unknown), but visual profiling still helps.
# MAGIC
# MAGIC In the next cell, use the `display()` function to display a grouped bar chart with the number of users and reviews for each gender value (including unknown).
# MAGIC
# MAGIC #### The following cell is step 7b:
# MAGIC

# COMMAND ----------

# Add your chart using the display function
display(df_user_gender)