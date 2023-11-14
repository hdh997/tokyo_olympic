# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/hdhtokyoolympicdata/tokyo-olumpic-data/raw-data"

# COMMAND ----------

athletes = spark.read.format("csv").option("header",True).load("/mnt/hdhtokyoolympicdata/tokyo-olumpic-data/raw-data/athletes.csv")
coaches = spark.read.format("csv").option("header",True).load("/mnt/hdhtokyoolympicdata/tokyo-olumpic-data/raw-data/coaches_2023-11-14.csv")
entries_gender = spark.read.format("csv").option("header",True).load("/mnt/hdhtokyoolympicdata/tokyo-olumpic-data/raw-data/entriesGender_2023-11-14.csv")
medals = spark.read.format("csv").option("header",True).option("inferSchema", True).load("/mnt/hdhtokyoolympicdata/tokyo-olumpic-data/raw-data/medals.csv")
teams = spark.read.format("csv").option("header",True).option("inferSchema", True).load("/mnt/hdhtokyoolympicdata/tokyo-olumpic-data/raw-data/teams.csv")


# COMMAND ----------

athletes.show()

# COMMAND ----------

athletes.printSchema()

# COMMAND ----------

display(coaches)

# COMMAND ----------

coaches.printSchema()

# COMMAND ----------

entries_gender.show()

# COMMAND ----------

entries_gender.printSchema()

# COMMAND ----------

medals.show()

# COMMAND ----------

medals.printSchema()

# COMMAND ----------

teams.show()

# COMMAND ----------

teams.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform data type for Entries Gender

# COMMAND ----------

entries_gender = entries_gender.withColumn("Female", col("Female").cast(IntegerType()))\
                                .withColumn("Male", col("Male").cast(IntegerType()))\
                                .withColumn("Total", col("Total"). cast(IntegerType()))

# COMMAND ----------

entries_gender.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Change columns name
# MAGIC

# COMMAND ----------

athletes = athletes.withColumnRenamed("PersonName", "Person_Name")

# COMMAND ----------

medals = medals.withColumnRenamed("Rank by Total", "Rank_by_Total")

# COMMAND ----------

teams = teams.withColumnRenamed("TeamName", "Team_Name")

# COMMAND ----------

display(athletes)

# COMMAND ----------

display(medals)

# COMMAND ----------

display(teams)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load data into ADLS

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/hdhtokyoolympicdata/tokyo-olumpic-data/"

# COMMAND ----------

athletes.repartition(2).write.mode("overwrite").option("header", True).csv("/mnt/hdhtokyoolympicdata/tokyo-olumpic-data/transformed-data/athletes")
entries_gender.write.mode("overwrite").option("header", True).csv("/mnt/hdhtokyoolympicdata/tokyo-olumpic-data/transformed-data/entries_gender")
coaches.write.mode("overwrite").option("header", True).csv("/mnt/hdhtokyoolympicdata/tokyo-olumpic-data/transformed-data/coaches")
medals.write.mode("overwrite").option("header", True).csv("/mnt/hdhtokyoolympicdata/tokyo-olumpic-data/transformed-data/medals")
teams.write.mode("overwrite").option("header", True).csv("/mnt/hdhtokyoolympicdata/tokyo-olumpic-data/transformed-data/teams")

