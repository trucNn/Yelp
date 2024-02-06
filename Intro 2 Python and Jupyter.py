# Databricks notebook source
# MAGIC %md 
# MAGIC # Truc Nguyen

# COMMAND ----------

# MAGIC %md 
# MAGIC # This is a Markdown Cell
# MAGIC
# MAGIC When you first load this notebook, this cell will have automatically been calculated to generate HTML.  This is the beauty of markdown.  Although this is a very basic markdown cell, 
# MAGIC markdown allows a wide range of formatting capabilities including **bolding**, *italics*, headings, bullet point lists, numbered lists, tables, indented blocks, code blocks, and even math formulas.
# MAGIC Since markdown is a superset of HTML (meaning it includes HTML and more), you can also embed HTML directly if you know it within a markdown cell.
# MAGIC
# MAGIC To see the original code written to generate this markdown cell, double-click anywhere in this cell.
# MAGIC
# MAGIC The first line above uses the "magic" cell starting with a percent (%) sign to indicate this is a markdown cell (%md).
# MAGIC
# MAGIC When editing a markdown cell, it is automatically compiled into HTML when you exit the cell (in other Jupyter interfaces 
# MAGIC you may need to run the cell to get it to compile, but it's fast either way).
# MAGIC
# MAGIC The second line of this cell has a "#" sign followed by a space to indicate the rest of the line is a Heading 1 line (what it becomes in HTML - a large header).
# MAGIC If instead we had 2 "#" signs, it would be a header 2 line - slightly smaller header than a Header 1, and so on as you add more "#" signs. The notebook allows up to 6 levels, as in HTML.
# MAGIC
# MAGIC Although a markdown cell is for documentation, one quirk you will quickly notice is there is no spell checking - make sure to review 
# MAGIC your work or it will lok sloopey an unprofessianole (to your teammates, instructor, and any recruiter you show it to). One way to review the text is to copy it into Word or a similar editor.
# MAGIC
# MAGIC There are links in Canvas to guides on using markdown, but one of the most often referenced guides on the Internet is <a href="https://daringfireball.net/projects/markdown/syntax" target="_blank">Daring Fireball</a>.
# MAGIC
# MAGIC However, when you want to add a link such as the above link to Daring Fireball, don't use the markdown approach of a description in square brackets followed by a link in quotes:
# MAGIC
# MAGIC `[Daring Fireball](https://daringfireball.net/projects/markdown/syntax)`
# MAGIC
# MAGIC If you do that, it will launch you out of your notebook instead of opening it up in another tab. Instead, add your links using embedded HTML.  Since markdown is a superset of HTML, 
# MAGIC meaning it includes HTML, you can embed any HTML in your markdown.  The above link to Daring Fireball uses the following HTML (double-click on this markdown to see it in the markdown):
# MAGIC
# MAGIC `<a href="https://daringfireball.net/projects/markdown/syntax" target="_blank">Daring Fireball</a>`
# MAGIC
# MAGIC Since this embedded HTML includes `target="_blank"` in the opening anchor tag, it opens the Daring Fireball web page in a new tab - the same as if that HTML was on any regular web page.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now for Some Simple Python ![Python_Powered](https://www.python.org/static/community_logos/python-powered-w-200x80.png)
# MAGIC
# MAGIC To get us started, we are going to do some simple Python code.  First we need to insert a new cell, so hover your mouse between this cell and the next cell.  A "plus" sign should appear as follows between the two cells:
# MAGIC
# MAGIC ![Insert_Notebook_Cell](http://www.sjsu.edu/people/scott.jensen/courses/add_notebook_cell.png)
# MAGIC
# MAGIC Click on that plus sign to add a new cell.  Since we did not add `%md` at the start of this cell, it's a code cell and not a markdown cell.  If you are using other Jupyter notebook environments (such as in the Anaconda desktop environment), you specify the cell type from a drop-down list.  In that environment you also need to explicitly run a markdown cell for it to compile into HTML.  In Databricks you just leave the markdown cell and it automatically compiles.
# MAGIC
# MAGIC ### Step 1: Creating some variables
# MAGIC In your new cell, add the following code:
# MAGIC
# MAGIC `my_school = "SJSU"`
# MAGIC
# MAGIC That will define a new variable named `mySchool` and it will contain the string variable SJSU.  We surrounded SJSU in double-quotes because all strings are surrounded in quotes.  We could also have surrounded it in single quotes, Python does not care, just that the starting and ending quote must (if we start with a double-quote, we must end with a double-quote).  This can be handy if your string includes an apostrophe (which is also a single quote on your keyboard).
# MAGIC
# MAGIC To assign the string SJSU to the variable `mySchool`, run the cell below by clicking the play button that appears on the right-hand side of the cell when you hover your mouse over that code cell.  After you run that cell, there will be a message below the cell saying when it was run, but there is no output - all you did was assign a value to a variable.  Before we move on, let's define one more variable.  in the same cell, add a new line with the following code and re-run the cell:
# MAGIC
# MAGIC `school_rank = 1`
# MAGIC
# MAGIC <div style="text-align:center;"><strong><span style="color:red;">Your New Cell Should Be Directly Below</span></strong></div>

# COMMAND ----------

my_school = "SJSU"
school_rank = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Using Our Variables and looking at "State" of the Notebook
# MAGIC
# MAGIC In Step 1 above you defined two variables, `my_school` and `school_rank`, but you have not done anything with them.  In the next bit of code you write, you will be 
# MAGIC using the `print` function to print out a message about your school and it's ranking.  However, we are also going to learn a bit about "state", which is the current situation in your notebook.  While you can always clear the state while you are in a session, the longest that the state will last is until your session ends and your cluster shuts down.  When the cluster shuts down, the notebook's state disappears.  When you come back into the notebook, there is no state until you start calculating cells again - the code will be there (and any output previously displayed will also be visible). Of course, and data we stored in DBFS will also be there.
# MAGIC
# MAGIC To continue, insert a new cell ***above*** the cell where you defined the `my_school` and `school_rank` variables.
# MAGIC
# MAGIC In that cell, we are going to use the Python `print` function to print a message in the output.  Type the following in that cell:
# MAGIC
# MAGIC `print("I think", my_school, "is #", school_rank)`
# MAGIC
# MAGIC Run that cell. No errors should occur (if typed correctly), and you should get a message printed as the output.
# MAGIC
# MAGIC If you have programed before (not in a notebook), this may seem odd.  Once you ran the cell in which you defined your two variables, they were part of the notebook's "state" (it's current situation).  Although you added your new cell ***above*** where you defined the variables you used, the calculation is based on the state.  For this reason, avoid reusing variable names in your notebook.
# MAGIC
# MAGIC #### Clearing the State:
# MAGIC
# MAGIC In the menu at the top, click on the dropdown list for `Clear` and select `Clear State & Run All`
# MAGIC
# MAGIC What Happened?
# MAGIC
# MAGIC You can change the order of your cells by dragging them (along the left edge).

# COMMAND ----------

print("I think", my_school, "is #", school_rank)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using formatted strings (also known as f-strings)
# MAGIC
# MAGIC When printing the string above, the school name and rank are part of the comma-separated list, but alternately, if you are familiar with Python f-strings, you could
# MAGIC use the following instead:
# MAGIC
# MAGIC `print (f"I think {my_school} is #{school_rank}")`
# MAGIC
# MAGIC Here the sentence is a single string, and the variables (or calculations) are enclosed in curly braces `{ }`.  The string has a lower-case `f` before the quotation marks (You could also use a capital `F`, but that's not commonly done).
# MAGIC
# MAGIC For all of the times you use the `print` function in this class, either approach is fine. 

# COMMAND ----------

# Not required, but try using f-strings
print (f"I think {my_school} is #{school_rank}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Checking the data type of our variables
# MAGIC The data type of our variables can be confusing.  For example, although `school_rank` is an `int` (integer), we could assign it the value "1" (including the quotes), and it would now be a string, but if we then ran our simple code cell with the `print` function, it would work ***and the output would still look the same***.  Fortunately, Python has another function that will help us check the data type of any variable.
# MAGIC
# MAGIC #### Variable Types:
# MAGIC Above we defined a new variable named `school_rank` and we are assigning the numeric value 1.  Since we assigned the value `1`, and not the value `1.0`, Python determined that our variable's type is an integer (a number without a decimal point).  If instead we had assigned the value `1.0`, Python would have determined our variable type is a float.  If you worked in other computing languages that use what is known as "static typing",
# MAGIC this might strike you as sort of weird. In some languages you must explicitly say the data type for each new variable you define.  Having to define the type when creating a variable is known as "static typing".
# MAGIC Instead, Python uses "dynamic typing", which means the type of a variable is determined when the program is running and the data type of the variable can even change based on the value assigned.
# MAGIC
# MAGIC #### Python Functions:
# MAGIC When we imported the Yelp data in another notebook, we defined a number of functions of our own.  These functions were short blocks of code that we could "call", meaning we could run them from other places in our code. Usually you will pass variables to a function (these are known as the "parameters" of the function).  We say *generally* since a function could have no parameters.  The function generally then takes some action - such as printing to the screen (which you may hear referred to as the standard output) or it returns a value or multiple values. A function can also both take some action and return a value.
# MAGIC
# MAGIC Python has a number of functions built in other than the `print()` function you used above.  One such function that can be handy is the `type()` function, which returns the data type of the variable passed as a parameter. When we assigned the value `SJSU` to the variable named `my_school`, we enclosed it in quotation marks, so Python knew this was a string value. Since the value assigned to the `school_rank` variable was **not** enclosed in quotation marks, it was not a string.  If instead we had written the following:<br/>
# MAGIC `school_rank = "1"`<br/>
# MAGIC The variable `school_rank` would have *looked* like an integer, but it would have been a string (since it's in quotation marks).
# MAGIC
# MAGIC Add a new cell below this markdown cell and use the `type` function to print the type for both the `my_school` and `school_rank` variables.
# MAGIC
# MAGIC <div style="text-align:center;"><strong><span style="color:red;">Your New Cell Should Be Directly Below</span></strong></div>

# COMMAND ----------

print("The data type for my_school is:", type(my_school),"the data type for school_rank is:", type(school_rank))

# COMMAND ----------

# MAGIC %md ## Your Mission: apply your Python skills

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generating Our Python Lists
# MAGIC In future notebooks we will dive into Spark DataFrames, view, tables, and Spark SQL.
# MAGIC
# MAGIC You do **NOT** need to understand the code in this cell.  We are only using it to build some data.
# MAGIC In the following cell we are querying one of the years of Social Security data you previously loaded and then building a Python dictionary
# MAGIC of the top 10 names that year for men and women (since it's the year they were born, actually boys and girls).
# MAGIC
# MAGIC You don't need to understand this code, but if you already understand Python well, you can play with it (but ideally not in the notebook you turn in or your results could be wrong).
# MAGIC
# MAGIC **Run the following cell, then skip down to the bottom of the notebook where it says "Working With Dictionaries".**
# MAGIC
# MAGIC The code:
# MAGIC 1. Loads the data from a CSV file into a DataFrame
# MAGIC 2. Adds column headers (since our CSV files do NOT have headers)
# MAGIC 3. Uses PySpark to create separate DataFrames for the men and women
# MAGIC 4. For each of the DataFrames we call a function we defined named `df_as_dict` to turn the small DataFrame into the Python dictionaries we want.
# MAGIC
# MAGIC The function:
# MAGIC 1. Sorts the DataFrame in descending order
# MAGIC 2. Gets rid of the gender column
# MAGIC 3. Keeps only the top 10 rows
# MAGIC 4. Orders the top 10 rows by name
# MAGIC 5. If we want to bring a small DataFrame out, the PySpark `collect` method can be used to return a list of rows, so that's what we do.
# MAGIC 6. For each row in the list, we create a dictionary entry with the name and count of people with that name in that year's data
# MAGIC 7. That map is then printed (so you can check your work later)
# MAGIC
# MAGIC We now have two Python lists named `men` and `women` respectively.

# COMMAND ----------

# UPDATE THE YEAR IN THE SELECT
def df_as_dict(df):
  df_gender = df.orderBy("cnt", ascending=False).drop("gender").\
              limit(10).orderBy("name")
  rows = df_gender.select("name","cnt").collect()
  map = {}
  for row in rows:
    name = row.name
    cnt = row.cnt
    map[name] = cnt
  print(map)
  return(map)


df_ssa = spark.read.option("header","false").option("inferSchema","true").csv("/ssa/data/yob2021.txt")
df_ssa2 = df_ssa.toDF("name","gender","cnt")
df_men = df_ssa2.filter("gender = 'M'")
print("Following is the 'men' dictionary:")
men = df_as_dict(df_men)

df_women = df_ssa2.filter("gender = 'F'")
print("\nFollowing is the 'women' dictionary:")
women = df_as_dict(df_women)

# COMMAND ----------

# MAGIC %md #Working With Dictionaries
# MAGIC
# MAGIC In the above cell we generated two Python dictionaries you will use to complete this exercise
# MAGIC
# MAGIC ### The following cell is Step 4a:

# COMMAND ----------

# The dictionaries are provided here (run the cell)
# The above cell will not work until we run the "Working With Files" notebook to load the Social Security Administration data
men = {'Benjamin': 11791, 'Elijah': 12708, 'Henry': 11307, 'James': 12367, 'Liam': 20272, 'Lucas': 11501, 'Noah': 18739, 'Oliver': 14616, 'Theodore': 9535, 'William': 12088}
women = {'Amelia': 12952, 'Ava': 12759, 'Charlotte': 13285, 'Emma': 15433, 'Evelyn': 9434, 'Harper': 8388, 'Isabella': 11201, 'Mia': 11096, 'Olivia': 17728, 'Sophia': 12496}

# COMMAND ----------

# MAGIC %md ### Step 4b: Most female applicants?
# MAGIC
# MAGIC Add your code in the cell below to calculate the women's name that had the most applicants (from the 10 names in the dictionary above).  Your answer should include:
# MAGIC
# MAGIC * The name 
# MAGIC * The number of applicants
# MAGIC * Some text (not just a number and name, but a human readable sentence)
# MAGIC
# MAGIC The result notebook shows what is expected - but your code **must** actually calculate the result, don't just print what's in the result notebook.

# COMMAND ----------

# Add your code here
women_key = max(women,key=women.get)
women_value=women.values()
max_value=max(women_value)
print("The female name with the most applicants",max_value,"is",women_key)

# COMMAND ----------

# MAGIC %md ### Step 4c: Were there more men or women?
# MAGIC
# MAGIC In the following cell, add your code to calculate the following using the above dictionaries:
# MAGIC
# MAGIC * How many male applcants were in the top 10 names for men
# MAGIC * How many female applicants were in the top 10 names for women
# MAGIC * Include in the output whether there were more men or women (see the result notebook for an example).
# MAGIC
# MAGIC The result notebook shows what is expected - but your code **must** actually calculate the result, don't just print what's in the result notebook.

# COMMAND ----------

# Add your code here
men_app=sum(men.values())
women_app=sum(women.values())
if men_app>women_app:
  print("more men",men_app,"than women",women_app)
else:
  print("more women",women_app,"than men",men_app)

# COMMAND ----------

# MAGIC %md 
# MAGIC #Deliverable:
# MAGIC 1. Complete both the in-class portion and the additional work with dictionaries
# MAGIC 2. Check your work against the results notebook posted in the assignment
# MAGIC 3. Add a ***New*** Markdown cell with a header at the start of the notebook that contains your full name
# MAGIC 4. Publish the notebook
# MAGIC 5. Submit the URL