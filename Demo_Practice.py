# Databricks notebook source
# MAGIC %sql
# MAGIC select * from table1

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/Table1-2.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", True) \
  .option("sep", delimiter) \
  .load(file_location)

df.write.format("orc").mode("overwrite").saveAsTable("table1")

# COMMAND ----------

from pyspark.sql import functions as  f
from pyspark.sql import types as t
import time
df = spark.sql("select * from table1")
#df.select((col("Issue Date").cast('double')).cast("timestamp")).show()
#df = df.withColumn("Issue_Date",(col("Issue Date").cast('double')).cast("timestamp"))
#df = df.withColumn("Issue_Date",f.to_date(col("Issue Date").cast(dataType=t.TimestampType()),"yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("Issue_Date",f.to_timestamp(col("Issue Date").cast('long')))
df = df.withColumn("Country",f.when(df.Country == "null", "").otherwise(df.Country))
df.display()
df.write.format("orc").mode("overwrite").saveAsTable("table1_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table2

# COMMAND ----------

df1 = spark.sql("select * from table2_3_csv")
from pyspark.sql import functions as f

def changecase(str):
    return ''.join(['_'+i.lower() if i.isupper() else i for i in str]).lstrip('_')

df2 = df1.select([f.col(c).alias(changecase(c)) for c in df1.columns])
df2.display()
df2.write.format("orc").mode("overwrite").saveAsTable("table2_new")


# COMMAND ----------

dft = spark.sql("select * from table2_new")
from pyspark.sql import functions as f

dft = dft.withColumn("start_time_ms", (f.col('start_time').cast('double') * 1000).cast('long') )
dft.display()

# COMMAND ----------

df1 = spark.sql("select * from table2_new")
#df1.select(df1.product _number)
df2 = spark.sql("select * from table1_new")
from pyspark.sql import functions as f

dff = df1.join(df2, df1["product _number"] == df2["Product number"])
dff = dff.withColumn("Country",lit("EN"))
dff.display()


# COMMAND ----------

table2_3_csv%sql
select * from table2_3_csv

# COMMAND ----------


