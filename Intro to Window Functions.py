# Databricks notebook source
# MAGIC %md 
# MAGIC Copyright &copy; 2022 Scott Jensen, San Jose State University
# MAGIC
# MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-sa/4.0/88x31.png" /></a><br /><span xmlns:dct="http://purl.org/dc/terms/" property="dct:title">This notebook</span> by <span xmlns:cc="http://creativecommons.org/ns#" property="cc:attributionName">Scott Jensen,Ph.D.</span> is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/">Creative Commons Attribution-ShareAlike 4.0 International License</a>.

# COMMAND ----------

# MAGIC %md # Introducing SQL Window Functions
# MAGIC
# MAGIC #### The ***main*** question we are asking of the Yelp data is:<br/>
# MAGIC *<div style="margin-left:50px;max-width:700px;background-color:#39506B;"><div style="text-align:center; margin:20px;font-size:1.5em;color:#ffffff">Do women and men join Yelp for different reasons?</div></div>*
# MAGIC
# MAGIC ***Our hypothesis is:*** the first reviews users post indicate why they joined Yelp.  Do women and men join Yelp for different reasons?  Does one join to praise and the other to flame?  Does one join to review restaurants and the other joins to review bars?
# MAGIC
# MAGIC #### What do we need to know?
# MAGIC * To answer our question, we need to find the first review for users in the dataset, however ...
# MAGIC   * We have the user information for each review in the 11 metro areas in the dataset, we ***do NOT*** have every review for those users
# MAGIC   * Reviews are not marked as "first review", so we need to figure out if we have a user's first review (either with certainty, or very likely)
# MAGIC   * The first step is figuring out which reviews in the dataset ***could possibly*** be the first review for those users
# MAGIC * We have the date and time each review was posted, so the first review a user has in the dataset, is the review that ***could*** be their first review
# MAGIC * First, we will use the `ROW_NUMBER` window function to order the reviews in the dataset for each user 

# COMMAND ----------

# MAGIC %md # Step 1: Load the Review Data
# MAGIC The following cell attempts to rebuild the table named `reviews_without_text_table` that was created in the **Working with Files** exercise.
# MAGIC
# MAGIC If that table was not previously generated, an error will occur and that prior notebook will need to be run.
# MAGIC
# MAGIC Since the reviews data is large, we excluded the text field (this is the actual text of the review).
# MAGIC
# MAGIC Step 1a first loads a notebook used to rebuild or create tables.  Depending on whether a table needs to be created or reloaded (in this case, reloaded), the Spark engine seems to optimize the code for the path taken (create or rebuild), so if used to create some tables and rebuild others, the notebook needs to be embedded again.  After running the step 1a, you may want to hide the output, but make sure it has completed running first.
# MAGIC
# MAGIC #### The following cell is step 1a:

# COMMAND ----------

# MAGIC %run "./Process or Create Table"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1b: Load `reviews_without_text_table` 
# MAGIC Since we are not providing a DataFrame name (the second parameter is None), the `delete` parameter must always be False.

# COMMAND ----------

process_or_create_table("reviews_without_text_table", None, summary=True, delete=False)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 2: Assigning the order of the reviews for each user
# MAGIC
# MAGIC **Keep in mind:** The first review we have in the dataset might not be the user's first review.
# MAGIC
# MAGIC In the following query on the review data, we are going to partition the data based on the `user_id` field, so each user is its own partition.
# MAGIC
# MAGIC We want to number each review with the order in which the review was posted by that user (for the 11 metro areas in our data). The first review we have in the dataset for a user would be identified as 1 (separately for every user), and the second review in our dataset by that user would be review 2, and so on.  Since we want to number the reviews in the order posted, we will sort the reviews within the partition based on the `date` field.
# MAGIC
# MAGIC To number the reviews, we will use the `ROW_NUMBER` windows function.  Since the row number is not calculated based on any column, but is just based on the sort order, the parenthesis after `ROW_NUMBER` are empty. We will alias the whole function as `review_order`, which will be the name of the column this function adds to the result.
# MAGIC
# MAGIC #### The following cell is Step 2:

# COMMAND ----------

# Add the ROW_NUMBER window function to the SELECT clausehttps://community.cloud.databricks.com/?o=1014402195071173#
df_ordered_reviews = spark.sql("""
SELECT *, 
        ROW_NUMBER() OVER (PARTITION BY User_id ORDER BY date) AS review_order
       
FROM reviews_without_text_table 
ORDER BY user_id, review_order
""")
print(f"review count:{df_ordered_reviews.count()}")
df_ordered_reviews.show(200, truncate=False)
df_ordered_reviews.createOrReplaceTempView("ordered_reviews")

# COMMAND ----------

# MAGIC %md # <span style="color:#0055A2;">Step 3: Ranking Functions (Team Assignment)</span>
# MAGIC
# MAGIC In addition to `ROW_NUMBER` which would arbitrarily order ties (if a user posted two or more reviews at the same second of the same minute of the same hour of the same day in the same year), There are two other ranking functions that handle ties differently:
# MAGIC
# MAGIC * RANK will return the same order for duplicates and then have a gap.  
# MAGIC * DENSE_RANK returns the same order for duplicates (same as `RANK`), except has no gaps
# MAGIC * PERCENT_RANK returns what percentile the record is in in the data
# MAGIC * NTILE(X) based on X, breaks the data in the partition into percentiles. The "X" parameter tells Spark how many groups to break the data into.  For example, a box plot uses quartiles (0% - 25%, 25% - 50%, 50% - 75%, 75% - 100%).  If we wanted quartiles, we would set the "X" parameter to 4.
# MAGIC
# MAGIC We could rank the reviews a user wrote in the 11 metro areas based on the number of useful votes they received.
# MAGIC
# MAGIC #### In the following cell: 
# MAGIC
# MAGIC * Add a query to SELECT all of the columns from the review data
# MAGIC * Calculate the ROW_NUMBER as done above
# MAGIC * Add four columns for the four functions listed above, based on the useful votes (the ORDER BY condition for the partition)
# MAGIC * For the NTILES function, use quartiles.  
# MAGIC * Sort the result based on the `user_id` and `useful`
# MAGIC * Show 100 rows
# MAGIC * Be sure your result is not truncated when you show it
# MAGIC * If you copy the query, be sure **NOT** to save it to the same DataFrame 
# MAGIC * If you copy the query, be sure **NOT** to  cache the result.
# MAGIC
# MAGIC The quartiles may be out of order.  **Why is that?**
# MAGIC
# MAGIC #### The following cell is Step 3:  

# COMMAND ----------

df_ordered_reviews_ranking = spark.sql("""
SELECT *, 
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY date) AS review_order,
       RANK()OVER (PARTITION BY user_id  ORDER BY useful) AS rank,
       DENSE_RANK()OVER (PARTITION BY user_id ORDER BY useful) AS dense_rank,
       PERCENT_RANK()OVER (PARTITION BY user_id ORDER BY useful) AS percent_rank,
       NTILE(4)OVER (PARTITION BY user_id ORDER BY useful) AS quartile
FROM reviews_without_text_table 
ORDER BY user_id, useful,review_order, rank, dense_rank,percent_rank, quartile
""")
df_ordered_reviews_ranking.show(100)
df_ordered_reviews.createOrReplaceTempView("ordered_reviews_ranking")

# COMMAND ----------

# MAGIC %md # Step 4: Comparing Reviews to the Average at That Business
# MAGIC
# MAGIC Possibly first reviews are more towards the extreme - that would not be too surprising, since ***something*** about a business prompted the user to go from being a passive reader to a content generator (which is advantageous to Yelp).
# MAGIC
# MAGIC ### Can we compare each review to the average for the business being reviewed?  
# MAGIC
# MAGIC To do this we are going to need an aggregate window function instead of a ranking window function.  Although the function names are the same as what you have used before as aggregate functions, most often along with the GROUP BY clause in a query, remember that the aggregate functions here are applied to each partition separately.
# MAGIC
# MAGIC ### Step 4a: In the next cell:
# MAGIC * Start with the query used to build df_ordered_reviews in Step 2
# MAGIC * Add a column to the SELECT to calculate the average at the business being reviewed in each review
# MAGIC * Name the new column `business_avg`
# MAGIC
# MAGIC <div style="border: solid 1px black;">
# MAGIC   <table style="border-collapse: collapse; border: none;">
# MAGIC     <tr style="border: none;">
# MAGIC       <td style="padding-right:5px; border:none;"><strong>NOTE:</strong></td>
# MAGIC       <td style="border:none; padding-bottom:20px;">If you want to check your results (assuming you named the DataFrame <code>df_ordered_reviews</code> as you did in Step 2), you can run the following PySpark in a subsequent cell:<br/>
# MAGIC         <code>df_ordered_reviews.orderBy('business_id').show(200,truncate=False)</code>
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC   </table>
# MAGIC </div>
# MAGIC
# MAGIC #### The next cell is Step 4a

# COMMAND ----------

# Add your query here (Start with Step 2)
df_ordered_reviews = spark.sql("""
SELECT *, 
        ROW_NUMBER() OVER (PARTITION BY User_id ORDER BY date) AS review_order,
        AVG(stars) OVER (PaRTITION BY business_id) AS business_avg
FROM reviews_without_text_table 
ORDER BY user_id, review_order
""")
print(f"review count:{df_ordered_reviews.count()}")
df_ordered_reviews.show(200, truncate=False)
df_ordered_reviews.createOrReplaceTempView("ordered_reviews")


# COMMAND ----------

# MAGIC %md ### Step 4b: Average of Prior Reviews (at the same business)
# MAGIC
# MAGIC What we ***really*** want to know is the average at the time the user wrote the review, which is the average of all preceding reviews at this business, so those from the first review at the business to the review immediately before the current review. That was the average when the user wrote the review.
# MAGIC
# MAGIC To do this, we need a "frame" which is specified in the following format: **ROWS BETWEEN <span style="color:red;">start</span> AND <span style="color:green;">end</span>**
# MAGIC
# MAGIC ### Step 4b: In the next cell:
# MAGIC * Start with the query used to build df_ordered_reviews in Step 4a
# MAGIC * Modify the partition for the `business_avg` field:
# MAGIC   * Add an ORDER BY to the partition.  We want to get the average of all of the reviews (within the partition) that were posted prior to the current review.  To do this, the reviews for the business need to be sorted by the `date` field.
# MAGIC   * Add a "frame" using ROWS to the partition
# MAGIC   * The **<span style="color:red;">start</span>** of the range is `UNBOUNDED PRECEDING` which means at the start of the partition
# MAGIC   * The **<span style="color:green;">end</span>** of the range is `1 PRECEDING` which means the review immediately before the current review (for this business)
# MAGIC  
# MAGIC <div style="border: solid 1px black;">
# MAGIC   <table style="border-collapse: collapse; border: none;">
# MAGIC     <tr style="border: none;">
# MAGIC       <td style="padding-right:5px; border:none;"><strong>NOTE:</strong></td>
# MAGIC       <td style="border:none; padding-bottom:20px;">If you want to check your results (assuming you named the DataFrame <code>df_ordered_reviews</code>), you can run the following PySpark in a subsequent cell:<br/>
# MAGIC         <code>df_ordered_reviews.orderBy(['business_id','date']).show(200,truncate=False)</code>
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC   </table>
# MAGIC </div>
# MAGIC
# MAGIC #### The next cell is Step 4b:

# COMMAND ----------

# Add your query here (Start with Step 4a)
df_ordered_reviews = spark.sql("""
SELECT *, 
        ROW_NUMBER() OVER (PARTITION BY User_id ORDER BY date) AS review_order,
        AVG(stars) OVER (PaRTITION BY business_id ORDER BY date 
                       ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) 
                       AS business_avg
FROM reviews_without_text_table 
ORDER BY user_id, review_order
""")
print(f"review count:{df_ordered_reviews.count()}")
df_ordered_reviews.show(200, truncate=False)
df_ordered_reviews.createOrReplaceTempView("ordered_reviews")


# COMMAND ----------

# MAGIC %md ### <span style="color:#0055A2;">Step 4c: Counting the Number of Prior Reviews at the Business (Team Assignment)</span>
# MAGIC
# MAGIC Possibly we would not want to consider the average if the business did not have many prior reviews (if the review is the first review for a business, the `business_avg` is null).
# MAGIC
# MAGIC In this step your team is adding another column to the SELECT clause.  This column should count the number of reviews in the partition prior to the current review, in other words, the number of reviews the `business_avg` is based on. The count for the first review would be 0 (no preceding reviews).
# MAGIC
# MAGIC ### Step 4c: in the next cell:
# MAGIC * Copy the query from Step 4b
# MAGIC * Add a field in the SELECT clause to do the count described above
# MAGIC * Name your new field `prior_reviews`
# MAGIC * This should be using the COUNT aggregate window function
# MAGIC
# MAGIC **NOTE:** To check your query you can use the same code as was used to check Step 4b 
# MAGIC
# MAGIC #### The next cell is Step 4c:

# COMMAND ----------

# Work with your team to add the query 
df_ordered_reviews = spark.sql("""
SELECT *, 
        ROW_NUMBER() OVER (PARTITION BY User_id ORDER BY date) AS review_order,
        AVG(stars) OVER (PaRTITION BY business_id ORDER BY date 
                       ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) 
                       AS business_avg, 
       COUNT(AVG(stars) OVER (PaRTITION BY business_id ORDER BY date 
                       ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) OVER (PaRTITION BY business_id ORDER BY date) AS prior_reviews
FROM reviews_without_text_table 
ORDER BY user_id, review_order
""")
print(f"review count:{df_ordered_reviews.count()}")
df_ordered_reviews.show(200, truncate=False)
df_ordered_reviews.createOrReplaceTempView("ordered_reviews")



# COMMAND ----------

# MAGIC %md # Step 5: Write the DataFrame to a Table
# MAGIC
# MAGIC After completing all of the steps, and adding the `prior_reviews` column to the `df_ordered_reviews` DataFrame, write that DataFrame out as `ordered_reviews_table`.
# MAGIC
# MAGIC For writing out the table, functions from the `Process or Create Table` notebook are first embedded here.
# MAGIC
# MAGIC In the future, if we need to force the table to be rebuilt, the `delete` parameter in the call to the `process_or_create_table` function should be set to `True`.
# MAGIC
# MAGIC #### The following cell is Step 5a:

# COMMAND ----------

# MAGIC %run "./Process or Create Table"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5b: Write the table

# COMMAND ----------

process_or_create_table("ordered_reviews_table", "df_ordered_reviews", summary=True, delete=False)