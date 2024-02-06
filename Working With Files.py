# Databricks notebook source
# MAGIC %md 
# MAGIC Truc Nguyen
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC Copyright 2022, Scott Jensen, San Jose State University
# MAGIC
# MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-sa/4.0/88x31.png" /></a><br /><span xmlns:dct="http://purl.org/dc/terms/" property="dct:title">This notebook</span> by <span xmlns:cc="http://creativecommons.org/ns#" property="cc:attributionName">Scott Jensen, Ph.D.</span> is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/">Creative Commons Attribution-ShareAlike 4.0 International License</a>.

# COMMAND ----------

# MAGIC %md 
# MAGIC # Part 1: Loading The Yelp Data
# MAGIC
# MAGIC Although the Yelp datset is not "Big" by commercial standards, for an academic dataset it's large in that unzipped it's approximately 10GB. Fortunately, Spark can work directly with compressed data in certain formats, and we will be loading zipped data using the bzip2 format (WinZip files will not work - don't try it).
# MAGIC
# MAGIC Some of the data files, particularly the reviews and the user data files, are nearly 2GB even when compressed, so loading them from a home Internet connection is not possible for many students (keep in mind that if your ISP is a cable company, data download speeds are usually much faster than data upload speeds, and you would need to do both).  If you are curious about your Internet speed, see the <a href="https://www.att.com/support/speedtest/" target="_blank">AT&T speedtest</a> (there's also a link in Canvas) - you would have roughly 900 Mbps both directions when using a wired Ethernet (not Wi-Fi) connection on campus.
# MAGIC
# MAGIC In this notebook you will be loading the data directly to your Databricks account from <a href="https://www.kaggle.com/datasets/yelp-dataset/yelp-dataset" target="_blank">Kaggle's</a> website (you will need a free Kaggle account and in step 3 you will be loading your Kaggle credentials). Yelp has staged the data on Kaggle in addition to putting it on the <a href="https://www.yelp.com/dataset" target="_blank">Yelp website</a>.
# MAGIC
# MAGIC We will walk through this notebook in class. Since the review and user files are rather large (almost 2GB each when compressed), you will need to complete **Part 4** of this exercise to create tables for the review and user data files. We will use these tables instead of the JSON data when working with the review and user data in future exercises.
# MAGIC
# MAGIC <strong><span style="font-size:1.2em;">NOTE:</span></strong> By downloading the dataset from Kaggle, you are bound by Yelp's `Dataset User Agreement` which you can download from the following dataset page at Kaggle: <a href="https://www.kaggle.com/datasets/yelp-dataset/yelp-dataset" target="_blank">https://www.kaggle.com/datasets/yelp-dataset/yelp-dataset</a>.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Spinning up a cluster
# MAGIC To be able to calculate any cells in your notebook, you will need to be attached to a cluster.  If you completed the exercise in the *Intro to Jupyter and Python* video lecture, this is the same process.  From the toolbar on the
# MAGIC left-hand side, click on the `Compute` icon (it looks like a cloud and used to be amed Clusters).  You won't have any clusters to start with, so click on the `+Create Cluster` button.
# MAGIC The defaults in that screen are fine, but you will need to enter a name for your cluster.  Once your cluster's status is 
# MAGIC running (and has a solid green circle by it), come back to your notebook and from the drop-down list in the top-left corner, attach 
# MAGIC your notebook to the cluster listed with the solid green circle.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 2: Setting  up the Kaggle API
# MAGIC The Python code used to download data using Kaggle's API (application programming interface) is in a separate Python module, so we need to install it on the driver node.
# MAGIC
# MAGIC Run the cell below to install the module.  You may notice that the first line of the output says "Python interpreter will be restarted".  That's why this cell needs to be the first code cell run - it's
# MAGIC going to restart python and all of the settings and values in memory will be gone.
# MAGIC
# MAGIC Be sure to wait for it to complete before continuing.  When a code cell is finished, it will say at the end of the output how long it took and that you ran it.

# COMMAND ----------

pip install kaggle

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 3: Setting your Kaggle credentials
# MAGIC
# MAGIC The Yelp dataset we will be using is at the following Kaggle page: <a href="https://www.kaggle.com/datasets/yelp-dataset/yelp-dataset" target="_blank">https://www.kaggle.com/datasets/yelp-dataset/yelp-dataset</a>.  If you have not already created an account, you will need to do so before you can download the data. In the upper right-hand corner of the webpage, there will be a `Sign In` link and a `Register` button. If you don't have an account already, click on the `Register` button and create an account.  If you have an account, click the `Sign In` link. If you are already signed in, or once you sign into an existing account or register an account, you will see an icon with what appears to be the head of a goose - that's the link to your personal profile - click it (if you hover your mouse over the icon, a pop-up tooltip will display your name).
# MAGIC
# MAGIC After you click on the duck icon, from the menu that appears, click on the `Account` option that has a gear icon.
# MAGIC
# MAGIC On your account page you will see a section titled **API** that contains a button titled `Create New API Token` and a button titled `Expire API token`.  Asuming you have not already created an API token, click on the `Create New API Token` button. This will cause Kaggle to generate a very tiny file named `kaggle.json` that contains a json object with your user name and key for the API token.  Depending on your browser settings, you will either be prompted to download the file or it will automatically be downloaded to the Downloads folder on your laptop.  Make sure you can find this file.
# MAGIC
# MAGIC #### Upload your credentials file
# MAGIC In ***another tab***, open the data option from the Databricks toolbar on the left-hand side of the screen (the black ribbon with icons).  The easiest way to open another tab in Databricks is to right-click on the Databricks icon at the top of that toolbar and select to open it in another tab (and then go to that tab). When you click on the Data menu option, it will expand and there will be a `Create Table` button - click it (we are not creating a table - it's just a quirky way to get to the interface for uploading files).  The **Create New Table** screen looks as shown below, and the `Upload File` menu option will be selected by default.  Leave the `DBFS Target Directory` input blank and just drag your `kaggle.json` file to the area circled in red (which says to drag files to upload) or you can use the browse link to go find it in the Downloads folder on your laptop. 
# MAGIC
# MAGIC <img src="https://www.sjsu.edu/people/scott.jensen/courses/upload_file_image.png" width="600px"/>
# MAGIC
# MAGIC The file will load quickly and a green checkmark will be displayed.
# MAGIC
# MAGIC #### Setting the environment variable for your Kaggle token
# MAGIC Since we don't want your credentials just sitting around in a web-accessible directory, we will copy them to the driver node and set an environment variable to point to them.  
# MAGIC The token file will be deleted when your cluster shuts down, so if you want to upload the files again, you will need to load your token file again.
# MAGIC
# MAGIC If you encounter a `java.io.FileNotFoundException` error message when running the following cell, then you did not upload your `kaggle.json` token file.  If you uploaded it correctly as described above, it will be in the `/FileStore/tables` directory in the Databricks file system (DBFS) for your account.
# MAGIC
# MAGIC If you want to see what files are in that directory, insert a new cell in this notebook and run the following command:
# MAGIC
# MAGIC `dbutils.fs.ls("/FileStore/tables")`
# MAGIC
# MAGIC #### The following cell is Step 3

# COMMAND ----------

import os

# Create a directory on the driver node and move the token file there
# The token will be deleted automatically when the cluster terminates
dbutils.fs.mkdirs("file:/kaggle")
dbutils.fs.mv("/FileStore/tables/kaggle.json","file:/kaggle/kaggle.json")
os.chmod("/kaggle/kaggle.json",0o600)
print( dbutils.fs.ls("file:/kaggle") )

os.environ["KAGGLE_CONFIG_DIR"] = "/kaggle"

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 3a: Optional step - confirm your credential was set
# MAGIC
# MAGIC After we moved our `kaggle.json` file with our credential to the local file system, we set a property named `KAGGLE_CONFIG_DIR` on the operating system
# MAGIC that says where our token can be found.  If you run the following cell it should tell you that the token is at `/kaggle`.  If an error is generated, then the property was not set.

# COMMAND ----------

token_path = os.getenv("KAGGLE_CONFIG_DIR")
print("Our token is loaded at:", token_path)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 4: Loading the Yelp file manifest
# MAGIC We will be loading the dataset posted on Kaggle by Yelp.  As they update versions of the dataset, the dataset owner or dataset name could change, 
# MAGIC or some of the files in the dataset could be named differently (it's happened in the past), or new files could be added (that also happened).
# MAGIC By having a small JSON file containing these parameters, and the expected size of each file, we can check that the correct version is being downloaded.
# MAGIC
# MAGIC As you did for the kaggle.json file containing your Kaggle token, upload the yelp_manifest.json file you downloaded from Canvas.
# MAGIC
# MAGIC **Be Sure to Complete This Step As Described Here Before Continuing!**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Importing the Yelp data
# MAGIC
# MAGIC If you uploaded a new manifest file in step 4 above, the code here in step 5 will use that to reload all of the data from Kaggle.  If there are other files than the dataset in the directory containing the yelp data, they will not be deleted, but any existing files from the Yelp dataset will be deleted.  If you did not upload a new manifest file, this step will check if there is an existing manifest file in the directory with the Yelp data.  If the yelp data directory does not exist, or does not contain a manifest file, this step will fail.  
# MAGIC
# MAGIC **If the output is a message that no manifest was loaded, return to step 4 and follow those instructions before continuing with this step**
# MAGIC
# MAGIC In the following code we are loading the files from the Yelp dataset on Kaggle and putting them in the DBFS (Databricks File System) for your account.  If you do another class project or your own side project using Kaggle data, you can use the same approach (but your manifest file would need to list the Kaggle dataset and fiels you want to load).  If you want some more details on using the Kaggle API for accessing datasets, see their documentation <strong><a href="https://www.kaggle.com/docs/api" target="_blank">here</a></strong> and <strong><a href="https://github.com/Kaggle/kaggle-api#datasets" target="_blank">here</a></strong>.  Also, since the documentation could be better, you may also want to take a look at <strong><a href="https://www.kaggle.com/code/donkeys/kaggle-python-api/notebook" target="_blank">this Kaggle notebook</a></strong> (not a Databricks notebook) that another Kaggle user created.  It explains the API calls in more detail.
# MAGIC
# MAGIC Spark can read data files in specific compressed file formats.  You may be familiar with the `zip` file format often used to compress files on Windows computers (also referred to as WinZip). When we download fiels using Kaggle's API, it automatically zips the files (so it takes less bandwidth to download).  Unfortunately, Spark cannot read a zip file, but it can read a bzip2 compressed file which by convention have a `.bz` file extension.  In the following code, we will:
# MAGIC
# MAGIC * Download each of the Yelp files listed in the manifest you previously uploaded (the files are downloaded to the *local* drive of the driver node for your cluster on AWS)
# MAGIC * Unzip the file
# MAGIC * Calculate an MD5 hash (we check this against the hash in the manifest file to make sure it downloaded correctly)
# MAGIC * Compress the file using the bzip2 file format
# MAGIC * Move the bzip2 version of the file into a folder named `Yelp` on the DBFS for your account on Databricks
# MAGIC
# MAGIC When downloading the data, we cannot work directly in DBFS, so we download and unzip the data using the driver node and then movng the data to DBFS.  Keep in mind that where we download the files
# MAGIC on the file system local to the driver node of the cluster disappears when the cluster terminates, but the files we move to DBFS are permanent and will be there again when you start a new cluster.
# MAGIC
# MAGIC ***Let's Go!*** (<span style="color:red;">this could take 30 minutes to run - be sure it completed successfully before you continue</span>)
# MAGIC
# MAGIC #### The following cell is Step 5

# COMMAND ----------

# import pyspark.sql.functions as f
# import re
# import tarfile
import zipfile
import json
import hashlib
import kaggle

MANIFEST_NAME = "yelp_manifest.json"
DEFAULT_DBFS_DIR = "/FileStore/tables/"
TEMP_DIR = "/yelptemp/" # Each function assumes it can delete this on the local driver node
DATA_DIR = "/yelp"


def get_manifest():
  ''' If the manifest is in the default upload directory on DBFS (/FileStore/tables)
      then that file is copied to the Yelp data directory and returned.  If there is
      not a manifest in the upload directory, then this function checks if there is 
      one in the Yelp data directory (if it exists).  If no manifest can be found,
      the function returns None.
  '''
  manifest = None # This will be set to the dictionary for the manifest if it exists
  upload_manifest_path = DEFAULT_DBFS_DIR + MANIFEST_NAME
  data_manifest_path = DATA_DIR + "/" + MANIFEST_NAME
  # We cannot use Python to open a file on DBFS and read the manifest, so 
  # we need to create the temp directory on the driver node and then copy the file 
  # to that temp directory to read it. Any files in the temp directory
  # are deleted.
  tempdir_path = "file:" + TEMP_DIR
  dbutils.fs.rm(tempdir_path, recurse=True)
  dbutils.fs.mkdirs(tempdir_path)
  # copy the manifest to the temporary directory
  temp_manifest_path = tempdir_path + MANIFEST_NAME
  # We don't know if the manifest exists in the default upload directory
  # so we need to check for an exception when copying it.
  copied = True
  try:
    dbutils.fs.cp(upload_manifest_path, temp_manifest_path)
    # Make sure the Yelp data directory exists on DBFS
    # if it already exists, mkdirs has no effect
    dbutils.fs.mkdirs(DATA_DIR)
    dbutils.fs.cp(upload_manifest_path, data_manifest_path)
  except Exception:
    copied = False
  # If the manifest was not in the uploads directory, check the data directory
  if copied == False:
    copied = True
    try:
      dbutils.fs.cp(data_manifest_path, temp_manifest_path)
    except Exception:
      copied = False
  # If the manifest exists, try to open it as a JSON file to a Python dictionary
  if copied:
    try:
      temp_manifest = TEMP_DIR + MANIFEST_NAME # without the file: needed for dbutils
      with open(temp_manifest, 'r', encoding="utf-8") as manifest_file:
        manifest = json.load(manifest_file)
        # The manifest should have a dataset element and a down_Load list element
        if "dataset" in manifest == False:
          raise Exception("The manifest does not contain a dataset element.")
        if "download_list" in manifest == False:
          raise Exception("The manifest does not contain a download_list.")
    except json.JSONDecodeError as err:
      manifest = None
      raise Exception("The manifest is not a valid JSON document.", err)
    except Exception as manifest_err:
      manifest = None
      raise Exception("An error occurred in reading the manifest file", manifest_err)
  return(manifest)  


def load_data(manifest):
  ''' This function processes each file in the manifest by:
      (1) Deleting it if it exists in the data directory, 
      (2) Downloading, unzipping, and comparing the MD5 sum to the manifest,
      (3) If it matches the manifest's MD5 sum, the file is zipped using bzip2 and moved to the data directory
      If not all of the files in the manifest were processed successfully, an exception is raised that says
      file caused the error and how many were processed.
  '''
  kaggle_dataset = manifest["dataset"]
  manifest_list = manifest["download_list"]
  # Create the data directory on DBFS
  dbutils.fs.mkdirs(DATA_DIR) # has no effect if it exists
  # Remove the items in the manifest from the data directory
  for data_source in manifest["download_list"]:
    name = data_source["name"]
    file_path = DATA_DIR + "/" + name
    try:
      dbutils.fs.rm(file_path)
    except Exception:
      pass
  # Process each item in the manifest
  files2process = len(manifest["download_list"])
  processed_count = 0 # how many were completed
  tempdir_path = "file:" + TEMP_DIR
  for data_source in manifest["download_list"]:
    dbutils.fs.rm(tempdir_path,recurse=True)
    dbutils.fs.mkdirs(tempdir_path)
    filename = data_source["raw_name"]
    md5 = data_source["md5"]
    bz2_name = data_source["name"]
    try:
      print("processing:", filename)
      # Download the file from kaggle
      kaggle.api.dataset_download_file(dataset=kaggle_dataset, file_name=filename, path=TEMP_DIR)
      # Unzip the file
      zipped = TEMP_DIR + filename + '.zip'
      unzipped = TEMP_DIR + filename
      print("unzipping:", zipped)
      with zipfile.ZipFile(zipped,"r") as zip_ref:
        zip_ref.extractall(TEMP_DIR)
      # Delete the zipped version
      zipped_path = 'file:' + zipped
      dbutils.fs.rm(zipped_path)
      # Calculate the MD5 sum and compare to the manifest
      with open(unzipped, 'rb') as data_file:
        md5sum = hashlib.md5( data_file.read() ).hexdigest()
      if md5sum.lower() != md5.lower():
        error_msg = f"For the file: {filename} the MD5 sum ({md5sum}) does not match the MD5 sum specified in the manifest({md5})."
        raise Exception(error_msg)
      # Re-compress the file in the bzip2 format
      print("recompressing:", filename)
      compress_path = TEMP_DIR + bz2_name
      tempZip = zipfile.ZipFile(str(compress_path), mode='x', compression=zipfile.ZIP_BZIP2)
      tempZip.write(unzipped, arcname=filename)
      tempZip.close()
      # Move the file to the data folder
      source = "file:" + compress_path
      destination = DATA_DIR + "/" + bz2_name
      dbutils.fs.mv(source, destination)
      print("Completed:", bz2_name)
      # Cleanup
      dbutils.fs.rm(tempdir_path,recurse=True)
    except Exception as err:
      print(f"Processed only {processed_count} of {files2process} files.")
      raise Exception(f"There was an error processing {filename}.", err)
    processed_count += 1
    # Done processing a file
  print(f"Processed {processed_count} files.")
  
# *******************************************  
# Run the Actual Routine to Load the Data
# This code uses the above defined functions
# *******************************************
manifest_dict = get_manifest()
load_data(manifest_dict)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 6a: Getting familiar with the Databricks File System (DBFS)
# MAGIC
# MAGIC When you create a cluster, the cluster has a "driver node", and if you were paying for the resources, you could have as many "worker nodes" as you are willing to pay for.  We are not paying for the cluster, so we have a driver node.  This is a virtual computer, but you can think of it as being like your laptop with a multi-core processor and local hard disk space. DBFS is where we store our data, and it's separate from the cluster, but accessible from the cluster.  As an analogy, you can store data on Google Drive, and you can access that data from your laptop. When you shut your laptop down, the data on Google Drive is still out there on the cloud and accessible to you from any other computer.  The difference is that once your cluster shuts down, your driver node is deleted.  This would be similar to if you borrow a laptop from the library, use it to load data onto your Google Drive account, and then shutdown and return the laptop. If you went back a week later and borrowed a laptop again, it woud likely be a different laptop, and even if it was the same one, the library may have deleted any files you created on it locally. However, you could still access your data you previously uploaded to Google Drive.  Similarly, when we start a new cluster, we have a different driver node (and just like two different laptops, the new cluster has a different "local" hard disk), but the data we stored on DBFS is still there in our account.
# MAGIC
# MAGIC Above you ran code to bring the Yelp data over from Kaggle to your account on Databricks.  As discussed previously, your data is stored in the Databricks File System, which is referred to as DBFS.  To be able to store your data in DBFS, we first brought it over to the local drive on the driver node and then moved it to a new folder named `/yelp` that we created in your account on DBFS.
# MAGIC
# MAGIC The commands for working with DBFS are very similar to Linux, but if you have not used Linux, don't panic, the number of commands we will be using can be counted on one hand (unless you are ET - he only had 2 fingers, but we are assuming you are an Earthling).  If you are a Mac user and have played around at the command line on your laptop, these commands will look familiar since the operating system for a Mac is a variant of Unix (and so is Linux).  All of the commands for working with DBFS are in the dbutils library (which Databricks has conveniently already installed when you spun up a cluster).  Although we will be using only a few commands, you don't need to memorize them, just run the following help command in a blank cell and it will show you all of the available commands:
# MAGIC
# MAGIC `dbutils.fs.help()`
# MAGIC
# MAGIC This will display each file system method along with the syntax and a brief description.
# MAGIC
# MAGIC Here we are using the Python dot notation.  The `dbutils` module contains functions for handling a number of tasks in Databricks (the name is short for Databricks utilities).  At the moment, we are only interested in the file system utilities for working with DBFS, so we use the dot notation to get the fileystem commands which are in `fs`, and then within the file system commands, we use the dot notation again to say we want to call the `help` method.
# MAGIC
# MAGIC The `dbutils.fs` methods are divided into file system utilities (fsutils) and mount methods.  You won't be mounting other drives, so you will only be using the fsutils methods.  If you have not done so, run the command shown above in the code cell below.

# COMMAND ----------

# Run the help function as described above to see the file system functions
dbutils.fs.help()

# COMMAND ----------

# MAGIC %md ### Step 6b: Listing the files loaded
# MAGIC
# MAGIC In the following cell, add a line of code to list the `/yelp` directory.  You should see the five Yelp files listed.
# MAGIC
# MAGIC If we wanted to *use* the list returned by the `ls` method in other code, we could assign it to a variable name. 
# MAGIC
# MAGIC #### The following cell is Step 6b:

# COMMAND ----------

dbutils.fs.ls("/yelp")

# COMMAND ----------

# MAGIC %md 
# MAGIC # Part 2: Getting the Category Definitions
# MAGIC
# MAGIC Above you loaded the Yelp data as zipped files in the bzip2 format to save space.  In the following cell we will take a slightly different approach using the `urllib` module to read a JSON data file from a page on Yelp's website.  
# MAGIC
# MAGIC In the dataset's business file, most businesses have a `categories` field which is a comma-separated list of the categories in which a business operates.  Some categories are at a high level (such as "Restaurants"), but the categories form a hierarchy with increasing levels of detail, so there are more specific categories too, such as "Dim Sum" which is within the "Chinese" category, which in turn is within the "Restaurants" category.  There are over 1500 categories (and growing).  As part of their "Fusion API", Yelp makes this list available to web developers who are creating apps that use Yelp data (and drive traffic to Yelp).  The page documenting the controlled vocabulary for categories can be found **<a href="https://www.yelp.com/developers/documentation/v3/all_category_list" target="_blank">here</a>**.
# MAGIC
# MAGIC On that site there's a link to a JSON file defining the hierarchy for this controlled vocabulary.  Download that file.  Although there are a lot of categories, as a JSON file this file is tiny compared to the Yelp data, so we don't need to compress it.
# MAGIC
# MAGIC <img src="https://www.sjsu.edu/people/scott.jensen/courses/Categories.PNG" width="800"/>
# MAGIC
# MAGIC ### The categories.json file is a small file, so we can load it through the GUI interface
# MAGIC
# MAGIC To load the categories.json through the GUI:
# MAGIC 1. Download the categories.json file from the link to the website.  You should now have a file named categories.json
# MAGIC 2. Through the GUI on the Data option (click the Create Table button, then load the file, but don't create a table)
# MAGIC
# MAGIC When the file has loaded, a green check mark will appear next to the file in the upload screen.
# MAGIC
# MAGIC You now have a file on the path: /FileStore/tables/categories.json
# MAGIC
# MAGIC You want to move that file to the path: /yelp/categories.json
# MAGIC
# MAGIC ### Moving the file in DBFS
# MAGIC
# MAGIC In the output for the help command when you ran it earlier, you will see a function named `mv` which is described as "Moves a file or directory, possibly across FileSystems"
# MAGIC
# MAGIC The file you loaded is on the path: /FileStore/tables/categories.json
# MAGIC
# MAGIC You want to move that file to the path: /yelp/categories.json
# MAGIC
# MAGIC The `mv` has two parameters you must enter, the "from" path and the "to" path.  There is a third parameter named "recurse" that has the default boolean value `False`.  The recurse parameter is only needed when we are moving directories, so we won't use it here. If we were moving a directory, we would want to set it to `True` so any subdirectories are also moved.
# MAGIC
# MAGIC We know the path where our file currently is and where we want to move it to, so use the `dbutils.fs.mv` function in the next cell and include the "from" and "to" paths, seperated by a comma.  Keep in mind that the paths are string variables (probably obvious, but it also tells you that in the help output), so the paths must be enclosed within quotation marks.
# MAGIC
# MAGIC #### Step 1: Use the `mv` method from file system methods in dbutils to move the file

# COMMAND ----------

# Add your code to move the file in this cell
dbutils.fs.mv("/FileStore/tables/categories.json","/yelp/categories.json")

# COMMAND ----------

# MAGIC %md #### **<span style="color:#22b922">Riddle me this</span>**: Why does the output have the value `True`?  
# MAGIC
# MAGIC The `mv` function has a Boolean return value, so instead we could have run the following code:
# MAGIC
# MAGIC `result = dbutils.fs.mv("/FileStore/tables/categories.json", "/yelp/categories.json")`
# MAGIC
# MAGIC That would have created a new variable named `result` and assigned the value returned by `mv` to that variable.  The value assigned to `result` would have been the Boolean value `True` (assuming the file was moved successfully)
# MAGIC and then we could have printed the value of `result` with the following Python code:
# MAGIC
# MAGIC `print(result)`
# MAGIC
# MAGIC In the code we actually ran, we did not assign the value returned by the `mv` method to a variable, so Jupyter printed out the value returned by the function call.
# MAGIC
# MAGIC **PLEASE NOTE:** Although Jupyter printed out the returned value of our call to `mv`, this is only because it's the last line in our code cell.  If we had additional code in that cell that wrote out or generated other values, the `True` returned by the call to `mv` would not be shown.

# COMMAND ----------

# MAGIC %md #### Step 1b: List the files in /yelp
# MAGIC
# MAGIC You have now moved the categories.json file into the same directory where you loaded the Yelp data above in Part 1. In the following cell, list the files again and you will now see the Yelp data and the categories.json file.

# COMMAND ----------

# Add your code to list the directory in this cell
dbutils.fs.ls("/yelp")

# COMMAND ----------

# MAGIC %md 
# MAGIC # Part 3: Finding Gender Data
# MAGIC
# MAGIC Yelp wants to create the best user experience possible and show authentic reviews.  They have a proprietary 
# MAGIC algorithm for ranking the reviews they show users.  The average star rating for a business is part of it,
# MAGIC but not the whole story.  The number of reviews is part of it, but not the whole story.  
# MAGIC Since many users will not read more than a couple reviews, having a good algorithm when ordering the reviews to 
# MAGIC show them to a user is critical to Yelp's business.  They need to always be thinking of how to make the ranking better
# MAGIC in order to improve the customer experience.
# MAGIC
# MAGIC What if men and women review differently?  Is a 4-star rating from a man the same as a 4-star rating from a woman?
# MAGIC In other words, might men or women consistently rate businesses higher or lower?  Would this depend on the type
# MAGIC of business?  Are ratings by one gender more consistent than the other?  If so, could we be more certain of the validity
# MAGIC of the ranking of a business based on 5 reviews by women than we would by 5 reviews by men (or vice versa)?
# MAGIC
# MAGIC If there is a difference, should that be taken into account when Yelp ranks businesses based on their ratings?

# COMMAND ----------

# MAGIC %md ### Using First Name as a Proxy for Gender
# MAGIC
# MAGIC We have a problem.  We don't have information about the user's gender.  However, we are curious and persistent.  Is there a proxy
# MAGIC we could use?  A proxy is a stand-in or substitute for something else.  Could a user's first name be a proxy for their gender?  What issues would we have if we use name
# MAGIC as a proxy for gender?
# MAGIC
# MAGIC First, we need some data to associate user names with genders.  We can start searching on the web.  Possibly lists of baby names?
# MAGIC If you were to search for a while, you would find the Social Security Administration's (SSA) website with the 1000 most popular baby names
# MAGIC for girls and boys (at least in the U.S.), but we want more than the most popular names, we want to tie as many names as possible to a gender.
# MAGIC If you dig a little further, you'll find the SSA page titled <a href="https://www.ssa.gov/oact/babynames/limits.html" target="_blank">Beyond the Top 1000 Names</a>.
# MAGIC
# MAGIC Read that page - they have a zip file there with national data as to every first name used to apply for a social security account and a count
# MAGIC of the number of men and women applying with that name, based on their date of birth.  Hover your mouse over that link, the file can be downloaded 
# MAGIC from the following URL:  <a href="https://www.ssa.gov/oact/babynames/names.zip" target="_blank">https://www.ssa.gov/oact/babynames/names.zip</a>
# MAGIC
# MAGIC We could download that file and unzip it (and you may want to do that after class), but what the zip file contains is a file for each year-of-birth, so 
# MAGIC for those little girls and boys born in 2017 who applied for a social security card, the file is named `yob2017.txt` (a copy is in this week's module in Canvas) and it contains data in the following
# MAGIC format:
# MAGIC
# MAGIC Emma,F,19738 <br/>
# MAGIC Isabella,F,15100 <br/>
# MAGIC Sophia,F,14831 <br/>
# MAGIC Mia,F,13437 <br/>
# MAGIC Liam,M,18728 <br/>
# MAGIC Logan,M,13974 <br/>
# MAGIC Benjamin,M,13733
# MAGIC
# MAGIC So if you just had a baby boy or girl and named her Isabella or named him Liam because you thought it would be unique, apparently so did everybody else.
# MAGIC
# MAGIC As you can see, the file has 3 columns, the name, the gender (M or F), and the number of people with that name who were born in 2017 and applied for a social security card.  You should also note there are no headers.
# MAGIC
# MAGIC It does not say what year the data is from, but the year is in the name of each file, so in a later exercise we will do an *enriching transformation* to insert
# MAGIC that metadata into a new column in the DataFrame.
# MAGIC
# MAGIC ### Step 1: Downloading the data
# MAGIC
# MAGIC We are going to use the `urllib` module. As before with the Yelp categories data, for the download, all of the necessary code is already included below.
# MAGIC
# MAGIC Using `dbutils.fs`, we will create a new directory named `ssa` where we are going to download the zip file from the SSA: `names.zip`.  However, we use the `file:` prefix for the path to say we want to create the directory local to the driver node for our cluster.  We need to do this because we cannot treat DBFS as a local file system.  Later we will move the zipped and unzipped data to DBFS.

# COMMAND ----------

import requests
import urllib

SSA_URL = "https://www.ssa.gov/oact/babynames/names.zip"
SSA_DIR ="/ssa/"
SSA_FILENAME = "names.zip"

# If the ssa directory exists, remove it
dbutils.fs.rm("file:"+SSA_DIR, recurse=True)
dbutils.fs.mkdirs("file:"+SSA_DIR)

urllib.request.urlretrieve(SSA_URL, SSA_DIR+SSA_FILENAME)

# COMMAND ----------

# MAGIC %md ###  Step 2: Check if your file was written
# MAGIC
# MAGIC Did the file end up in our `ssa` directory?  In the cell below, add code that uses the `ls` method from  `dbutils.fs` to list 
# MAGIC the contents of the `file:/ssa` directory which is a directory local to the driver node and ***not*** part of the DBFS.  We cannot store
# MAGIC files on the local system long-term since the local file system is on the driver node and it is gone when the cluster terminates.  Any
# MAGIC files moved to DBFS will still be there after the cluster terminates so=ince DBFS is accessible from the driver node, but not onthe driver node.

# COMMAND ----------

# Add your code here to list the /ssa directory (and run the code)
dbutils.fs.ls("file:/ssa")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Unzipping the data
# MAGIC When we loaded our Yelp data we loaded bzip2 files because Spark does not read zip files, but here we
# MAGIC are still using Python (not Spark), to unzip the file, so that's not a problem.
# MAGIC
# MAGIC We will need to import another Python module that has the functions to unzip a file, and we will
# MAGIC use the Databricks `dbutils` functions to create a subdirectory we are going to unzip the data files into. 
# MAGIC
# MAGIC **Keep in mind that when we are running the zip commands, we are just using Python and not Spark**.  Why does that matter?  The paths we provide
# MAGIC are not pointing to paths on DBFS (since the plain old Python does not "know" about DBFS).  When we use the path "/ssa", the zip command assumes
# MAGIC we are talking about the `ssa` directory off the root of the driver node.  If we use the path "/ssa" in a dbutils command, it assumes we are
# MAGIC referring to a DBFS path since we did not prefix the path with `file:`.
# MAGIC
# MAGIC Once we unzip the data, we must move the directory to DBFS.  While data on DBFS will remain when our cluster shuts down, the files 
# MAGIC local to the driver will not exist once the cluster is shutdown.
# MAGIC
# MAGIC **After running the following cell, in Step 4 be sure to use the `ls` method to see the listing of all of the SSA files for each year**

# COMMAND ----------

import zipfile

SSA_SUBDIR = "data"
ssaDataDir = "file:" + SSA_DIR + SSA_SUBDIR
dbutils.fs.mkdirs(ssaDataDir) 

ssaNamesZip = SSA_DIR + SSA_FILENAME

with zipfile.ZipFile(ssaNamesZip,"r") as zip_ref:
    zip_ref.extractall(SSA_DIR + SSA_SUBDIR)
    
# Move the files to DBFS
dbutils.fs.rm("dbfs:"+SSA_DIR, recurse=True)
dbutils.fs.mv("file:"+SSA_DIR, "dbfs:"+SSA_DIR, recurse=True)

# COMMAND ----------

# MAGIC %md ###  Step 4: Check if your data was unzipped properly
# MAGIC
# MAGIC Did the files for each year's data end up in our `data` subdirectory under `ssa`?  Use the `ls` method from  `dbutils.fs` to list 
# MAGIC the contents of the `/ssa/data` directory.

# COMMAND ----------

# Add your code here to list the contents of the /ssa/data directory (and run it)
dbutils.fs.ls("/ssa/data")

# COMMAND ----------

# MAGIC %md ### Step 5: What does one of these data files look like?
# MAGIC
# MAGIC Using the `head` method in `dbutils.fs` we can take a peek at the first "X" bytes.  We will use the default of 64K.

# COMMAND ----------

dbutils.fs.head("dbfs:/ssa/data/yob1880.txt")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 6: Making it more human-readable
# MAGIC In the above command, the `head` function is just getting bytes.  For a more human-readable view of the head of the file, we can enclose the call to `dbustils.fs.head()` inside a call 
# MAGIC th the Python print function and the `\r\n` which represent line feeds in the file will now show each name on a separate line.
# MAGIC
# MAGIC **In the following cell**, enclose the call to the `head` function from the cell above in a call to the python `print` function.

# COMMAND ----------

# In this cell, add a call to the print function around the call to the dbutils.fs.head method
print(dbutils.fs.head("dbfs:/ssa/data/yob1880.txt"))

# COMMAND ----------

# MAGIC %md # Part 4: Creating Tables for Reviews and Users
# MAGIC Loading the review and user data from the JSON files is tedious, so we will create tables to load the data and strips out the text of the reviews.
# MAGIC
# MAGIC There is a separate notebook named "Building Review and User Tables Version 2" that will build the tables, and when you imported the Databricks archive (dbc) file containing this notebook, that other notebook was loaded into the same folder. You will need to run it (it takes about 25 minutes).  
# MAGIC
# MAGIC Once you have run it, come back here and 
# MAGIC run the following cells (you *won't* need to rerun the code above), which will rebuild the tables and print out the tables that are defined.  You do not even need to run on the same cluster, so if your cluster had shut down, just start a new one.  After you run this cell, you can hide the results.

# COMMAND ----------

# MAGIC %run "./Building Review and User Tables Version 3"

# COMMAND ----------

# MAGIC %md ### Show the Tables
# MAGIC The above cell re-ran the *Building Review and User Tables Version 2* notebook.  If that notebook was not connected to the current cluster, it did not matter.  The above cell **MUST** finish before running the following cell.
# MAGIC
# MAGIC That other notebook recreated the tables for the user and review data, so in the following cell we can show (list) those tables.

# COMMAND ----------

spark.sql("SHOW TABLES").show(truncate=False)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Assignment Deliverable
# MAGIC
# MAGIC * Make sure you have added and run code for those steps where you were supposed to list or print the results.
# MAGIC * Make sure you have successfully run the code in part 4 to load the review and user tables
# MAGIC * Publish your notebook as you have done before
# MAGIC * Submit the published URL as the deliverable for this assignment