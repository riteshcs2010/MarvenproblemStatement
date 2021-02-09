# Databricks notebook source
# MAGIC %md
# MAGIC Problem Statement
# MAGIC Description: 
# MAGIC Develop a solution that seeks to join related data together into document-based data structure, saving this to a suitable format (e.g. JSON, AVRO, Parquet) and load into a database of your choice (SQL/NoSQL/SparkSQL/Presto) hosted on Docker. 

# COMMAND ----------

# MAGIC %md
# MAGIC Unmounted the data if its existed already

# COMMAND ----------

def sub_unmount(str_path):
    if any(mount.mountPoint == str_path for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(str_path)

sub_unmount('/mnt/marveldc')

# COMMAND ----------

# MAGIC %md
# MAGIC Mounted the Azure Blob data into Spark

# COMMAND ----------

# MAGIC %scala
# MAGIC val configs = Map("fs.azure.account.key.sastreamritesh.blob.core.windows.net" -> "V1L0SBhAzcE2VLBDrUtZ8lgWVYaFdCWn2KudgkPAH+iKAvlInHjl0waxU94DL4UU82hc1JD57ZLottLBz7Rulg=="
# MAGIC )
# MAGIC 
# MAGIC dbutils.fs.mount(
# MAGIC source = "wasbs://marveldc@sastreamritesh.blob.core.windows.net",
# MAGIC mountPoint = "/mnt/marveldc",
# MAGIC extraConfigs =configs
# MAGIC )

# COMMAND ----------

# MAGIC %fs ls /mnt/marveldc

# COMMAND ----------

# MAGIC %md
# MAGIC Load the data into a dataframe

# COMMAND ----------

file_type="csv"
df = spark.read.format(file_type).option("inferSchema", "true").option("header", "true").load("dbfs:/mnt/marveldc/marvel_dc_characters.csv")
display(df) 

# COMMAND ----------

df.repartition(1).write.mode('overwrite').parquet('mnt/marveldcoutput')

# COMMAND ----------

# MAGIC %fs ls /mnt/marveldcoutput

# COMMAND ----------

df1 = spark.read.parquet('/mnt/marveldcoutput')
df1.show()

# COMMAND ----------

# toJSON() turns each row of the DataFrame into a JSON string
# calling first() on the result will fetch the first row.
import json
results = json.loads(df.toJSON().first())
print(results)
df.repartition(1).write.mode('overwrite').json("mnt/marveldcJson/x.json")

# COMMAND ----------

df2 = spark.read.json('/mnt/marveldcJson/x.json')
df2.show()

# COMMAND ----------

#import required modules
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
 
#Create spark configuration object
conf = SparkConf()
conf.setMaster("local").setAppName("My app")
 
#Create spark context and sparksession
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)
 
#read csv file in a dataframe
#df = spark.read.csv(path="C:\\sample-spark-sql.csv", header = True, sep = ",")
 
#set variable to be used to connect the database
database = "riteshSQL"
table = "dbo.tbl_spark_df"
user = "riteshsql"
password  = "Chandra@2010"
 
#write the dataframe into a sql table
df.write.mode("overwrite") \
    .format("jdbc") \
    .option("url", f"jdbc:sqlserver://riteshsqltesting.database.windows.net:1433;databaseName={database};") \
    .option("dbtable", table) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .save()

# COMMAND ----------

# MAGIC %fs ls /mnt/marveldcJson

# COMMAND ----------

