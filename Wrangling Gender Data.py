# Databricks notebook source
# MAGIC %md
# MAGIC Truc Nguyen

# COMMAND ----------

# MAGIC %md
# MAGIC # Wrangling Gender Data
# MAGIC
# MAGIC In this notebook we are going to do some basic wrangling of our Yelp data - asking some profiling questions
# MAGIC and doing some transformations.  As you recall, Rattenbury et al. in *Principles of Data Wrangling* discussed
# MAGIC five broad categories of data wrangling, broken down into 2 types of profiling and 3 types of transformations:
# MAGIC * Individual value based profiling
# MAGIC * Set based profiling
# MAGIC * Structuring transformations
# MAGIC * Enriching transformations
# MAGIC * Cleaning transformations
# MAGIC
# MAGIC We will take a look at each of these using the Yelp data.
# MAGIC
# MAGIC ### Always Start with a Question ...
# MAGIC
# MAGIC We should always start with a question.  The question may evolve, but the question we will be looking at in this notebook is, "***Do women and men rate businesses differently?***"
# MAGIC
# MAGIC Yelp wants to create the best user experience possible and show authentic reviews.  They have a proprietary 
# MAGIC algorithm for ranking the reviews they show users (see this <a href="https://www.yelpblog.com/2010/03/yelp-review-filter-explained" target="_blank">video</a>).  The average star rating for a business is part of it,
# MAGIC but not the whole story.  The number of reviews is part of it, but not the whole story. 
# MAGIC
# MAGIC Since many users will not read more than a couple reviews, having a good algorithm for ordering the reviews to 
# MAGIC show them to a user is critical to Yelp's business.  They need to always be thinking of how to make the ranking algorithm better
# MAGIC in order to improve the customer experience.
# MAGIC
# MAGIC What if men and women review differently?  Is a 4-star rating from a man the same as a 4-star rating from a woman?
# MAGIC In other words, might men or women consistently rate businesses higher or lower?  Would this depend on the type
# MAGIC of business?  Are ratings by one gender more consistent than the other?  If so, could we be more certain of the validity
# MAGIC of the ranking of a business based on 5 reviews by women than we would by 5 reviews by men (or vice versa)?
# MAGIC
# MAGIC If there is a difference, should that be taken into account when Yelp ranks businesses based on their ratings?
# MAGIC
# MAGIC
# MAGIC # Step 0: Creating a DataFrame for the SSA data
# MAGIC
# MAGIC In the following cell we use `spark` to read the directory of csv files we previously downloaded and unzipped from the 
# MAGIC <a href="https://www.ssa.gov/oact/babynames/limits.html" target="_blank">Social Security Administration (SSA) website</a>. Previously when loading the Yelp data which uses the JSON Lines format, we used 
# MAGIC `spark.read.json` to load the data.  Here we are using `spark.read.csv` method since the SSA data is in a comma-separated value file format.  
# MAGIC
# MAGIC Unlike reading JSON files, where the schema is inferred automatically unless we specify a schema, when reading CSV files the default is to *not* infer the schema.  
# MAGIC For this reason we will include an optional parameter to tell Spark to infer the schema.  
# MAGIC
# MAGIC Since our data files do not contain a row with column headings, we need to be sure to include the option for headers and specify 
# MAGIC the value false.  Otherwise, the first name in our file would be treated as the column headings. 
# MAGIC
# MAGIC When we loaded the data into DBFS during the *Working With Files* exercise, we unzipped the data file we downloaded from the SSA into the directory `/ssa/data`
# MAGIC and there was a separate file for each year. Since there are multiple files in that directory, the path we pass to the `csv` method, is `/ssa/data*.txt` which says we want to load all of the files in that directory with the `txt` file extension. They will all be loaded into a single DataFrame.
# MAGIC
# MAGIC **As we always do when loading data**, we printed a record count for our DataFrame, show a few records (20 by default), and print the schema. 
# MAGIC
# MAGIC
# MAGIC #### What if I get a <span style="color:red;">"Path does not exist"</span> error?
# MAGIC If that occurs, you did not successfully run Part 3 of the "Working with Files" notebook.  You will need to load the SSA data.
# MAGIC
# MAGIC Since you don't have time to complete Working with Files now, do the following (**ONLY IF YOU GOT THIS ERROR MESSAGE!)**:
# MAGIC * Insert a new cell before the cell that creates the `df_ssa_data` DataFrame
# MAGIC * In that cell, post the code in the next line (including the quotation marks):<br/>`%run "./Load_SSA"`
# MAGIC * Run that cell, and when it finishes, continue with running the cell to build the `df_ssa_data` DataFrame
# MAGIC
# MAGIC #### The following cell is Step 0:
# MAGIC

# COMMAND ----------

df_ssa_data = spark.read.option("header","false").option("inferSchema","true").csv("/ssa/data/*.txt")
print(f"Number of records: {df_ssa_data.count() }")
df_ssa_data.show(truncate=False)
df_ssa_data.printSchema()
df_ssa_data.createOrReplaceTempView("ssa_data")


# COMMAND ----------

# MAGIC %md
# MAGIC # Step 1: Two Transformations:
# MAGIC <ul>
# MAGIC   <li style="font-size:1.5em;">Enriching Transformation - Adding the File Name as a Column</li>
# MAGIC   <li style="font-size:1.5em;">Structuring Transformation - Renaming Columns</li>
# MAGIC </ul>
# MAGIC
# MAGIC The files 
# MAGIC we downloaded have 3 columns, the name, the gender (M or F), and the number of people with that name who were born in 
# MAGIC the year represented by each file and applied for a social security card.  We should also note there were no column headers in the data so Spark generated column headings based on the column position in the data.
# MAGIC
# MAGIC Each line of data does not say what year the data is from, but we have that in the name of each file, so we will 
# MAGIC do an ***enriching transformation*** to insert the file name as metadata into a new column in the DataFrame.
# MAGIC
# MAGIC There is a spark function named `INPUT_FILE_NAME()` that we can use to get the name of the file that was the source of the data.  There are 140 data files (one per year of birth for SSA applicants), so the function will return the name of the file that each row of data came from. 
# MAGIC
# MAGIC Since we want more useful column headings, we will also do a ***structural transformation*** in renaming the three columns in our data
# MAGIC
# MAGIC Add the `SELECT` clause in the code below, setting aliases as follows:
# MAGIC * \_c0: name
# MAGIC * \_c1: gender
# MAGIC * \_c2: count
# MAGIC * Include the file name as the fourth column using the `INPUT_FILE_NAME()` function and the alias "filename"
# MAGIC
# MAGIC #### The following cell is Step 1

# COMMAND ----------

# Add the SELECT clause
df_ssa = spark.sql("""
SELECT _c0 AS name, _c1 AS gender, _c2 AS count,
    INPUT_FILE_NAME() AS filename
FROM ssa_data
""")
print(f"Number of records: {df_ssa.count() }")
df_ssa.show(truncate=False)
df_ssa.createOrReplaceTempView("ssa")

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 2: Structuring Transformation - Extract the 4-Digit Year from the File Name
# MAGIC
# MAGIC Our `filename` column contains the name of the file from which the data was loaded, but
# MAGIC what we really want is the year the data represents, such as `2008` instead of `dbfs:/ssa/data/yob2008.txt`.
# MAGIC
# MAGIC To fix this we are going to do a structuring transformation.  In the textbook they call this itrarecord structuring 
# MAGIC (although we are changing all of the records, each change only concerns the data in a single record, so it's 
# MAGIC "intra" record structuring).   
# MAGIC Within intrarecord structuring, they refer to this as extracting values - we are going to take the 4-digit year 
# MAGIC (based on position within the filename), and make it a new column.
# MAGIC
# MAGIC Keep in mind that DataFrames are immutable (you cannot
# MAGIC change an existing DataFrame), so we are going to `SELECT` the first three columns and then select a substring
# MAGIC out of the filename column and give it the alias `ssa_year`.
# MAGIC
# MAGIC ### About that SUBSTRING
# MAGIC For the first 3 columns, we are just selecting them based on the aliases we gave them in the prior step.
# MAGIC
# MAGIC For the filename column we will use the `SUBSTRING` function.  See the SQL Examples notebook for an example. ***If you have not
# MAGIC already loaded that notebook, what are you waiting for?***
# MAGIC
# MAGIC We need to specify three parameters: 
# MAGIC * The name of the column we are working with
# MAGIC * Where we want to start in the string values for that column
# MAGIC * How many characters we want
# MAGIC
# MAGIC The first parameter should be obvious.  Since each of our file names contains a four-digit year, the third parameter is self-explanatory.  The second parameter
# MAGIC should be -8 because if we specify a negative number, the position counts back to the left from the end of the string.  All of 
# MAGIC our filenames are "yobXXXX.txt" where the XXXX is the year.  
# MAGIC
# MAGIC Alias the formula for the year as `ssa_year`
# MAGIC
# MAGIC ### Casting the string result as a number
# MAGIC When we extract the 4-digit year from the filename, we want to be able to use it as a number  However, `SUBSTRING`, as it's name implies, gets part of a string out of string, so the result is .... a string!
# MAGIC
# MAGIC Since we want a number, we can use the function `CAST` to cast our string as an integer.  Keep in mind that our extracted string contains values that would be valid as numbers (we cannot convert the string "SJSU" to a number, even though *we* know that should be number 1).  Since the `SUBSTRING()` function will return a 4-digit string with a number in this case, we will "wrap" that function call inside `CAST` and say what type of data we want to cast it to (and `INTEGER` in our situation).
# MAGIC
# MAGIC If you want to understand data types better, see Chapter 6 of the *SQL Pocket Guide* in the Safari Online reading list for this class.
# MAGIC
# MAGIC ### Compared to Python
# MAGIC If you are familiar with slicing strings in Python, this would be written a bit differently such that if we set the filename string to a variable named filename, we could do the following:
# MAGIC
# MAGIC   filename = "dbfs:/ssa/data/yob2008.txt"<br/>
# MAGIC   filename[-8:-4]
# MAGIC   
# MAGIC ### Why did we name the DataFrame generated df_ssa2 instead of just using df_ssa again?
# MAGIC   In the partial code below, you will see that we named the DataFrame generated by our query as `df_ssa2` and the subsequent temporary view was named `ssa2` instead of reusing `ssa`.  You may be wondering why we did not reuse the `ssa` name for our temporary view.  What if we make a mistake in our code?  If we named our DataFrame and temporary view as df_ssa and ssa respectively, we could not just fix our code and rerun it - why is that?
# MAGIC   
# MAGIC #### The following cell is Step 2

# COMMAND ----------

df_ssa2 = spark.sql("""
SELECT name, gender, count, CAST( SUBSTRING(filename, -8, 4) AS INTEGER ) AS ssa_year
FROM ssa
""")
df_ssa2.show(truncate=False)
df_ssa2.printSchema()
df_ssa2.createOrReplaceTempView("ssa2")

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 3a: Individual Value Profiling - Is the Gender Always M or F?
# MAGIC
# MAGIC Individual value profiling is often done to look for errors in our data - either syntax errors, such as whether the gender in the SSA data is always M or F, or semantic errors such as the spelling of city names in the Yelp business data.
# MAGIC
# MAGIC Here you need to select those rows in our data where the gender is not an M or an F. These would be the rows with an erroneous value for gender. However, in Spring 2022, the SSA said they would start allowing people in Fall 2022 to submit applications for a new Social Security card with "X" as their gender.  Whether that will be reflected in the name data download we use is unknown.

# COMMAND ----------

#Add the WHERE clause to get the rows with any invalid gender values (based on the SSA data definitions)
spark.sql("""
SELECT *
FROM ssa2
WHERE gender NOT IN ('F','M')
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 3b: Set-Based Profiling - What are the Trends for Men and Women Each Year in the Data?
# MAGIC We have data going back to 1880, but is the number of women in the dataset similar for men and women?  Is it similar in recent decades, but 
# MAGIC not in earlier decades?  Possibly we could predict the gender of a Yelp user's name much better for one year than the other.
# MAGIC
# MAGIC In the following cell we are going to use set-based profiling to count the number of men and women in the data each year.  To do this, 
# MAGIC we are going to add a `GROUP BY` clause to aggregate the data, and an `ORDER BY` clause to sort the data by year in ascending order.

# COMMAND ----------

# Add the GROUP BY AND ORDER BY clauses
df_gender_by_year = spark.sql("""
SELECT gender, ssa_year, SUM(count) AS annual_count
FROM ssa2
GROUP BY gender,ssa_year
ORDER BY ssa_year, gender
""")
df_gender_by_year.show(100)

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 3c: Visual Profiling
# MAGIC In the above output of the `df_gender_by_year` DataFrame, we can see there is a growth trend in the number of men and women in the dataset, at least up through the 1920's, but other than a general idea, it's hard to interpret that trend from the tabular output.  Databricks also has a `display()` function that allows us to do some basic charts directly on a distributed DataFrame without first bringing the data back onto the driver node.  Compared to Tableau, the visualizations we can generate using the `display()` function are very limited, but they are excellent for doing some initial visual profiling.
# MAGIC
# MAGIC The parameter we pass to the display method is a DataFrame, and the output is initially tabular data (it looks like a spreadsheet), but we can then set the plot options as to the type of chart and how we want the data displayed.  Click on the chart icon at the bottom of the tabular output to display the initial chart (or select a chart from the drop-down list).
# MAGIC
# MAGIC Click the `Plot Options ...` button to set the display parameters for the chart:
# MAGIC * **Keys:** This is the field that will be displayed on the x-axis.
# MAGIC * **Series grouping:** This is the filed used for color-coding different line in a line chart or segments in a stacked bar or area chart.
# MAGIC * **Values:** This is the field that will be displayed on the y-axis and aggregated.
# MAGIC * **Aggregation:** How the "Values" are combined.  Your chart probably defaulted to `SUM`, but since we have one value per year for each gender, we are aggregating a single number for each data point in the chart. 
# MAGIC * **Display type:** This is the chart to be displayed.  Depending on the type of chart selected, additional options may appear.  For example, for the bar chart there is a selection between grouped, stacked, and 100% stacked bar charts.
# MAGIC

# COMMAND ----------

# Set the plot options for the displayed chart
display(df_gender_by_year)

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 4: Structuring Transformation (Interrecord) - Pivot on Gender
# MAGIC
# MAGIC Instead of two lines for the women and men born each year for a name, what we really want is one line
# MAGIC for with a column for the number of women and a column for the number of men.  What we want to do is ***pivot*** the data
# MAGIC based on the values in the gender field.  
# MAGIC
# MAGIC As an example, assume we had the following two lines in our `ssa2` temporary view for the 2020 data for the name Isabella:<br/>
# MAGIC
# MAGIC | name | gender | count | ssa_year |
# MAGIC | ---- | ------ | ----- | -------- |
# MAGIC |Isabella | F | 12066 |  2020 |
# MAGIC |Isabella | M | 9 |  2020 |
# MAGIC
# MAGIC What we want instead is one line:
# MAGIC
# MAGIC | ssa_year | name | women | men |
# MAGIC | ---- | ------ | ----- | -------- |
# MAGIC | 2020 | Isabella | 12066 | 9 |
# MAGIC
# MAGIC
# MAGIC Originally `pivot` was a method of grouped data in PySpark, but was later added to Spark SQL in a subsequent release. For documentation on the PySpark `pivot`
# MAGIC method, click <a href="https://spark.apache.org/docs/latest/api/python/_modules/pyspark/sql/group.html#GroupedData.pivot" target="_blank">here</a>).  We are going to use the Spark SQL approach.
# MAGIC
# MAGIC #### Syntax for PIVOT in spark SQL:
# MAGIC Compared to the usual format of a SQL query, using PIVOT is a bit different.  Although in PySpark `pivot` works on Grouped Data, in Spark SQL it is NOT part of the GROUP BY clause, but instead follows ***after*** a SQL query which returns the data to be pivoted: <br/>
# MAGIC SELECT ...<br/>
# MAGIC FROM ...<br/>
# MAGIC PIVOT( ...)<br/>
# MAGIC ORDER BY ...
# MAGIC
# MAGIC You will notice in the above format, we complete a SQL query that gets the data we want to work with.  Every column in that query's `SELECT` clause will either be 
# MAGIC The field we are pivoting on, a field we are aggregating, or a field we are grouping on.  To explain this, we need to dive into the `PIVOT` part of the query.  The syntax for PIVOT is as follows:<br/>
# MAGIC PIVOT( <br/>
# MAGIC  &nbsp;&nbsp;aggregation formulas <br/>
# MAGIC   &nbsp;&nbsp;FOR &lt; pivot columns &gt; IN ( &lt; pivot values &gt; )<br/>
# MAGIC )
# MAGIC
# MAGIC **For an excellent discussion and some examples of using PIVOT in Spark SQL, see <a href="https://databricks.com/blog/2018/11/01/sql-pivot-converting-rows-to-columns.html" target="_blank">this post in the Databricks Engineering Blog</a> by MaryAnn Xue**
# MAGIC
# MAGIC #### Applying PIVOT to the SSA data:
# MAGIC If we look at the example with Isabella above, we want to: 
# MAGIC
# MAGIC * Group our data by year and name (there should be a row for each year/name combination).
# MAGIC * Pivot on the `gender` column, so that each different row value becomes a column.   This is specified in the `FOR` clause inside `PIVOT`.  Our two row values are F and M, for women and men (we also want to alias them).  This means that when we pivot, each *value* in the pivot column becomes a *column* in the result.
# MAGIC * The possible values for the pivot columns are listed in the `IN` list in parenthesis (along with any aliases to be used).  Here our possible values are F and M, but since they are strings, we need to enclose them in quotation marks.  <br/>**NOTE:** If you were using PIVOT and did *not* know what all the possible values were (particularly if that was not consistent), you would need to use the PySpark implementation of the `pivot` method since it can be run without specifying the possible values.  For the Spark SQL version of PIVOT, the `IN` is not optional. 
# MAGIC * We need to include an aggregation on the `count` column (since we want the count values in our pivoted columns), so we will use SUM on `count`. For the 2020 row for Isabella in the result, we want to sum all of the rows in count for that year/name/gender combination.  In our case that will always be summing one row, but if we were instead wanting to get the grouping by name (aggregating all years), then it would be summing the rows for all years that have a name/gender combination.
# MAGIC
# MAGIC **Complete the PIVOT clause in the following cell**
# MAGIC
# MAGIC #### The following cell is Step 4

# COMMAND ----------

# Complete the PIVOT following the query for the data to pivot
df_ssa3 = spark.sql("""
SELECT *
FROM ssa2
PIVOT(
  SUM(count)
  FOR gender IN('F' AS women, 'M' AS men) 
)
ORDER BY name, ssa_year
""")
df_ssa3 .show()
df_ssa3.createOrReplaceTempView("ssa3")

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 5: Two Transformations
# MAGIC <ul>
# MAGIC   <li style="font-size:1.5em;">Structuring Transformation - Aggregating by name</li>
# MAGIC   <li style="font-size:1.5em;">Cleaning Transformation - Getting rid of nulls</li>
# MAGIC </ul> 
# MAGIC
# MAGIC We want to eventually be able to predict whether a Yelp user is a woman or a man, so we want to
# MAGIC get a total count by gender and indicate if the name is more often a man's or a woman's name, and 
# MAGIC what the percentage is.  For example, if from 1880 - 2020 there were 100,000 Isabella women named 
# MAGIC Isabella, and 100 men named Isabella, we would classify it as a women's name 99.9% of the time.
# MAGIC
# MAGIC We need to aggregate based on the `name` column, but in the `SELECT` clause we also need to be able to set the values in the `women` and `men` columns to 0 when they are null (no people in the data with that name for that gender).
# MAGIC
# MAGIC To clean up the null values we can use the `COALESCE` function.  In this function we provide a comma-separated list of columns and constants (only the last item in the list should be a constant), and the first value that's not null is the result for this function.  Here we will use the `COALESCE` function in both the `women` and `men` columns, but we only need tow parameters - the column name and the value 0. The `COALESCE` function will then return:<br/>
# MAGIC * The value in that column if it is not null
# MAGIC * 0 if the column is null (for that row)
# MAGIC
# MAGIC Since we are aggregating, and `COALESCE` is not an aggregate function, we need to decide how to combine the aggregation and `COALESCE` function.
# MAGIC
# MAGIC #### The following cell is Step 5

# COMMAND ----------

# Add the SELECT clause
df_ssa4 = spark.sql("""
SELECT name,
       SUM(COALESCE(women,0)) AS women,
       SUM(COALESCE(men,0)) AS men
       
FROM ssa3
GROUP BY name
""")
df_ssa4.show()
df_ssa4.createOrReplaceTempView("ssa4")

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 6: Enriching Transformation - Identifying Gender for a Name
# MAGIC ### Which gender has a higher count?
# MAGIC
# MAGIC We now want to add two columns (in addition to the name, women, and men columns):<br/>
# MAGIC * The total number of people with a name, aliased as `total`.  We will use this column in the next step to calculate a percentage for the gender.
# MAGIC * a column named `gender` that has the value 'M' if there are more men with that name, or 'F' if there are not (here we have classified a tie as women)
# MAGIC
# MAGIC To calculate the `gender` field, we are going to use the SQL `IF` function.  If you have used Excel, this function should look *very* familiar since it is the same syntax as the IF function in Excel with the same 3 parameters:<br/>
# MAGIC IF( &lt; condition &gt; ,  &lt; then value &gt; ,  &lt; else value &gt; )
# MAGIC
# MAGIC The three parameters are as follows:<br/>
# MAGIC * &lt; condition &gt; is a formula or column we are checking, and it should result in a Boolean value; True or False.  
# MAGIC * If the formula in the condition results in True, the function's value is the  &lt; then value &gt;
# MAGIC * If the formula in the condition results in False, the function's value is the  &lt; else value &gt;
# MAGIC
# MAGIC **NOTE:** The angle brackets  &lt;  &gt; are not part of the condition or values and not included in the function.
# MAGIC
# MAGIC #### The following cell is Step 6

# COMMAND ----------

# Add the SELECT clause
df_ssa5 = spark.sql("""
SELECT name, women, men, 
      (men+women) AS total,
      IF(men>women,'M','F') AS gender
FROM ssa4
""")
df_ssa5.show(truncate=False)
df_ssa5.createOrReplaceTempView("ssa5")

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 7: Enriching Transformation - Calculating the Percentage for a Name
# MAGIC
# MAGIC For predicting the gender of Yelp users, we have classified names in the SSA data as male or female, but if names are frequently more gender neutral, we would want to take that into account.
# MAGIC
# MAGIC To do this, we will compare the count for the gender assigned to each name to the total for that name.  In this query, we use the `IF` function again to calculate a new `gender_ratio` column:
# MAGIC * If the `gender` field is 'F', the ratio is the women's total over the total for the name
# MAGIC * Otherwise, it is calculated as the men's total over the total for the name
# MAGIC
# MAGIC #### The following cell is Step 7

# COMMAND ----------

# Add the SELECT clause for all of the fields in the ssa temporary view and the gender ratio
df_ssa_gender = spark.sql("""
SELECT name, women, men, total, gender,
       IF(gender= 'F', women/total, men/total)AS gender_ratio
FROM ssa5
""")
df_ssa_gender.show()
df_ssa_gender.createOrReplaceTempView("ssa_gender")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7a: Histogram
# MAGIC In the following cell, set the plot options for a histogram based on the gender_ratio field.

# COMMAND ----------

# Set the plot options
display(df_ssa_gender.select("gender_ratio"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 8: Saving the Gender Data to a Table
# MAGIC In the following cell we run the `Process or Create Table` notebook in this same directory - that will allow us to save a 
# MAGIC DataFrame as a table and then in the next cell we call the `process_or_create_table` function within 
# MAGIC that notebook to create the `ssa_gender_table` which we will later use to predict the gender of Yelp users.

# COMMAND ----------

# MAGIC %run "./Process or Create Table"

# COMMAND ----------

TBL_NAME = "ssa_gender_table"
DF_NAME = "df_ssa_gender"

process_or_create_table(TBL_NAME, DF_NAME, summary=True, delete=False)
df_ssa_gender.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC # Steps 9 and 10: Your Mission
# MAGIC In looking at 20 rows, it appears that most names (to the extent we can identify a gender), are predominately one gender or the other.  
# MAGIC However, with over 100,000 names in the SSA data, There could still be common names that are gender neutral or which have drifted from one gender to the other over time.
# MAGIC
# MAGIC In Step 9 you are going to look for these names, particularly those with the most people.  In Step 10 you will build on that to look at the percentage of people in the data who were female for the most common gender-neutral names.
# MAGIC
# MAGIC ### Step 9: Most gender-neutral popular names
# MAGIC In the following cell: add a query to find the top 5 names (based on the number of people), where the difference in the gender ratio was 5% or less (e.g., One gender was 47.5% of the people and the other gender was 52.5% of the people).  Sort the results in order of the number of people with that name (with the most common name listed first).  
# MAGIC
# MAGIC Your results should include the name, gender, total number of people (for that name), and the gender ratio. 
# MAGIC
# MAGIC **NOTE:** Adding a `LIMIT` to your result<br/>
# MAGIC At the end of your query, you can add a `LIMIT` clause.  Unlike the `show()` method which only shows the number of rows specified, but does not change the actual resulting DataFrame, the `LIMIT` clause is included at the end of your SQL query and will
# MAGIC start at the first row of the query result and include only the number of rows specified in the resulting DataFrame. The format is:<br/>
# MAGIC `LIMIT x` where "x" is some positive integer value.
# MAGIC
# MAGIC **Hint:** Build the query using `ssa_gender_table`
# MAGIC
# MAGIC #### The following cell is Step 9:

# COMMAND ----------

# Add your query here
df_gender_ratio = spark.sql("""
SELECT name, gender, total, gender_ratio
FROM ssa_gender_table
WHERE ABS((women/total)- (men/total))<= 0.05
ORDER BY total DESC 
LIMIT 5
""")
df_gender_ratio.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 10: Visualizing gender ratios over time
# MAGIC In Step 9 you found the 5 most popular names where the difference in the gender ratio for men and women was 5% or less.  In this step you are going to generate a line chart
# MAGIC that has a line for each of those five names and shows the trend in the female gender ratio for each name.
# MAGIC
# MAGIC You will need to build a query to generate a DataFrame and then feed that DataFrame to the `display()` function.
# MAGIC
# MAGIC HINT: You will most likely want to start with the temporary view `ssa3` since it has the counts by name and by year for each gender, but you will need to incorporate the `COALESCE` function used in Step 5 as well as calculating the gender ratio for women.  You can do this as multiple queries in multiple cells, a nested query, or multiple queries within a "common table expression" (CTE).  If you are not already familiar with nested queries or CTE's in SQL, multiple queries as separate steps are fine.
# MAGIC
# MAGIC #### The following cell is Step 10:

# COMMAND ----------

#Add your query here and the call to the display function below.  Insert as many cells as needed

df_ratio = spark.sql("""
SELECT name,ssa_year, 
       SUM(COALESCE(women,0))AS women,
       SUM(COALESCE(men,0)) AS men
FROM ssa3
WHERE name IN (
            SELECT name
            FROM ssa_gender_table
            WHERE ABS((women/total)- (men/total))<= 0.05
            ORDER BY total DESC 
            LIMIT 5
)
GROUP BY ssa_year,name
""")
df_ratio.show()
df_ratio.createOrReplaceTempView("ratio")



# COMMAND ----------

df_female_ratio = spark.sql("""
SELECT name,ssa_year, 
       (women/(men+women))AS female_ratio
FROM ratio
Group BY name,ssa_year,women, men
""")
df_female_ratio.show()
df_female_ratio.createOrReplaceTempView("female_ratio")

# COMMAND ----------

# Add your call to the display function here
display(df_female_ratio)