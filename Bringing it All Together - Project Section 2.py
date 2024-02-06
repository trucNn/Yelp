# Databricks notebook source
# MAGIC %md 
# MAGIC Team Dreamers 
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC Copyright &copy; 2022 Scott Jensen, San Jose State University
# MAGIC
# MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-sa/4.0/88x31.png" /></a><br /><span xmlns:dct="http://purl.org/dc/terms/" property="dct:title">This notebook</span> by <span xmlns:cc="http://creativecommons.org/ns#" property="cc:attributionName">Scott Jensen,Ph.D.</span> is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/">Creative Commons Attribution-ShareAlike 4.0 International License</a>.

# COMMAND ----------

# MAGIC %md # Bringing All of Our Data Together
# MAGIC
# MAGIC #### The ***main*** question we are asking of the Yelp data is:<br/>
# MAGIC *<div style="margin-left:50px;max-width:700px;background-color:#39506B;"><div style="text-align:center; margin:20px;font-size:1.5em;color:#ffffff">Do women and men join Yelp for different reasons?</div></div>*
# MAGIC
# MAGIC ***Our hypothesis is:*** the first reviews users post indicate why they joined Yelp.  Do women and men join Yelp for different reasons?  Does one join to praise and the other to flame?  Does one join to review restaurants and the other joins to review bars?
# MAGIC
# MAGIC
# MAGIC In class we have done a number of data wrangling exercises looking at the different data files relevant to our question and bringing them together.
# MAGIC For the question we are asking, we need the following data sources:
# MAGIC * Yelp categories (not part of the Yelp dataset, but avalable **<a href="https://www.yelp.com/developers/documentation/v3/all_category_list" target="_blank">here</a>**).
# MAGIC * Yelp business data - all of the businesses in the 8 metro areas included in the dataset.
# MAGIC * Metro Areas - this is a file you created from Tableau
# MAGIC * Yelp reviews - the reviews written on all of the businesses in the business data (to the extent that Yelp considers them to be legitimate reviews).
# MAGIC * Yelp user data - The data describing those Yelp users who wrote the reviews included in the Yelp review data (we do NOT have all of the users' revirews - only those written about the 8 metro areas in the dataset).
# MAGIC * Social Security Administration (SSA) data - This data ia available at this SSA page titled **<a href="https://www.ssa.gov/oact/babynames/limits.html" target="_blank">Beyond the Top 1000 Names</a>**. We will use it to predict the gender of Yelp users.
# MAGIC
# MAGIC **<span style="font-size:1.2em;">The connections between our data files are as follows:</span>**<br/>
# MAGIC <img src="https://www.sjsu.edu/people/scott.jensen/courses/Yelp%20Project%20Data-all%20tables.png"/>
# MAGIC
# MAGIC **Notes about the combined notebooks:**<br/>
# MAGIC * The category definitions, metro areas, and Yelp business data is combined and wrangled in the Business and Categories notebook
# MAGIC   * That data is written out to the table `business_category_table`
# MAGIC * The Yelp user data and the SSA name data are wrangled and combined to predict each Yelp user's gender in the User and Gender notebook
# MAGIC   * That data is written out to the table `user_gender_table`
# MAGIC * The review data was wrangled in the Intro to Window Functions notebook and written out as `ordered_reviews_table` 
# MAGIC * This notebook brings the tables from the earlier notebooks together with the review data

# COMMAND ----------

# MAGIC %md # Step 1: Load the prior tables
# MAGIC This step attempts to rebuild the following tables:
# MAGIC
# MAGIC * business_category_table
# MAGIC * ordered_reviews_table
# MAGIC * user_gender_table
# MAGIC
# MAGIC If any of those tables were not previously generated, an error will occur and the prior notebook that created the table will need to be completed.
# MAGIC
# MAGIC
# MAGIC Step 1a first loads a notebook we will use to rebuild the tables.  After running step 1a, you may want to hide the output, but make sure it has completed running first.
# MAGIC
# MAGIC #### The following cell is step 1a:

# COMMAND ----------

# MAGIC %run "./Rebuild Project Tables"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1b: Rebuild the tables 

# COMMAND ----------

table_list = ['business_category_table', 'ordered_reviews_table', 'user_gender_table']
process_tables(table_list)

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 2: Combining the review and business data
# MAGIC In the `Business and Categories` notebook we generated the table named `business_category_table` which has business level data, including the metro area.
# MAGIC
# MAGIC Each business has as many rows as top-level categories it is categorized as operating in.
# MAGIC
# MAGIC Here we bring the review data together with the business data and apportion each review to included a weighted field that is 1 divided by the number of categories.
# MAGIC
# MAGIC #### The following cell is Step 2:

# COMMAND ----------

df_weighted_reviews = spark.sql("""
SELECT  R.*,
        B.name, B.state, B.city, B.metro_area, B.review_count, B.category, B.category_count, 
        (1/B.category_count) AS weighted_review
FROM ordered_reviews_table AS R INNER JOIN business_category_table AS B 
ON R.business_id = B.business_id
""")
print("Record Count:", df_weighted_reviews.count())
df_weighted_reviews.show(truncate=22)
df_weighted_reviews.createOrReplaceTempView("weighted_reviews")

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 3: Combining the weighted reviews and user data
# MAGIC
# MAGIC In the `User and Gender` notebook we generated the table named `user_gender_table` which has the user data for each review writer, with gender data from the SSA.
# MAGIC
# MAGIC Here we bring the weighted review data together with the user data.
# MAGIC
# MAGIC #### The following cell is Step 3:

# COMMAND ----------

df_user_reviews = spark.sql("""
SELECT R.*,
       U.name AS user_name, U.yelping_since, U.reviews_written, U.friend_count, U.is_elite, U.first_elite, U.gender, U.gender_ration
FROM weighted_reviews AS R INNER JOIN user_gender_table AS U
ON R.user_id = U.user_id
""").cache()
print("Record count:", df_user_reviews.count() )
df_user_reviews.show()

# COMMAND ----------

# MAGIC %md # Step 4: Write the combined project data to a table
# MAGIC So we can access the  results of this notebook from Tableau (without recalculating this notebook), we will write our DataFrame out to a table named `project_table`.  Unless this needs to change, we will not need to regenerate this table, but just reload the table from the files stored in /user/hive/warehouse.
# MAGIC
# MAGIC In the future, if we need to force the table to be rebuilt, the `delete` parameter in the next cell should be set to `True`.
# MAGIC
# MAGIC For writing out the table, functions from the `Process or Create Table` notebook are first embedded here.
# MAGIC
# MAGIC **<span style="color:red; font-size:1.5em;">NOTE:</span>**  After the table is built, if you want to connect to the project table from Tableau, only Steps 4a and 4b need to be re-run.
# MAGIC
# MAGIC #### The following cell is Step 4a:

# COMMAND ----------

# MAGIC %run "./Process or Create Table"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4b: Generate the `project_table`

# COMMAND ----------

process_or_create_table("project_table", "df_user_reviews", summary=True, delete=False)

# COMMAND ----------

# look at the field of table 
spark.sql("""
describe project_table
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC project_reviews DataFrame and Tables

# COMMAND ----------

df_project_reviews = spark.sql("""
SELECT review_id,
       FIRST(user_id) AS user_id,
       FIRST(date) AS date,
       FIRST(stars) AS stars,
       FIRST(review_order) AS review_order,
       FIRST(name) AS name,
       FIRST(gender) AS gender,
       FIRST(metro_area) AS metro_area,
       FIRST(review_count) AS review_count,
       FIRST(category_count) AS category_count,
       FIRST(user_name) AS user_name,
       FIRST(yelping_since) AS yelping_since,
       FIRST(reviews_written) AS reviews_written,
       COLLECT_LIST(category) AS categories
FROM project_table
GROUP BY review_id
ORDER BY review_id
""")
df_project_reviews.show(truncate=22)

# COMMAND ----------

process_or_create_table("project_reviews_table", "df_project_reviews", summary=True, delete=False)