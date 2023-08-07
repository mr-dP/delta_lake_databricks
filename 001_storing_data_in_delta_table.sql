-- Databricks notebook source
-- There are 2 ways to do that

--   1.  You can directly store it in Data Lake
--       - The files are stored in Parquet format
--       - The transaction log is stored as well
--       - No metadata is registered with Hive metastore, which is available in Databricks
--       - To query it you must read the data directly from Data Lake

--   2.  You can store the data as Spark Table (Delta Table)
--       - The files are stored in Parquet format in Data Lake
--       - The transaction log is stored as well
--       - The schema of the data is registered with Hive metastore
--       - To query it you can either read the data from Data Lake or use the table name to access the data. This makes it easier to work with the data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### (A) Read data from Data Lake

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.types import *
-- MAGIC
-- MAGIC yellowTaxiSchema = (
-- MAGIC     StructType()
-- MAGIC     .add("RideId", IntegerType())
-- MAGIC     .add("VendorId", IntegerType())
-- MAGIC     .add("PickupTime", TimestampType())
-- MAGIC     .add("DropTime", TimestampType())
-- MAGIC     .add("PickupLocationId", IntegerType())
-- MAGIC     .add("DropLocationId", IntegerType())
-- MAGIC     .add("CabNumber", StringType())
-- MAGIC     .add("DriverLicenseNumber", StringType())
-- MAGIC     .add("PassengerCount", IntegerType())
-- MAGIC     .add("TripDistance", DoubleType())
-- MAGIC     .add("RatecodeId", IntegerType())
-- MAGIC     .add("PaymentType", IntegerType())
-- MAGIC     .add("TotalAmount", DoubleType())
-- MAGIC     .add("FareAmount", DoubleType())
-- MAGIC     .add("Extra", DoubleType())
-- MAGIC     .add("MtaTax", DoubleType())
-- MAGIC     .add("TipAmount", DoubleType())
-- MAGIC     .add("ToolsAmount", DoubleType())
-- MAGIC     .add("ImprovementSurcharge", DoubleType())
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC yellowTaxiDF = (spark.read.option("header", True).schema(yellowTaxiSchema)).csv(
-- MAGIC     "dbfs:/mnt/optimizingdatabricks/delta_lake_demos/YellowTaxis/YellowTaxis/YellowTaxis1.csv"
-- MAGIC )
-- MAGIC
-- MAGIC yellowTaxiDF.count()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC display(yellowTaxiDF)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### (B) Write data in Parquet format

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC (
-- MAGIC     yellowTaxiDF.write.mode("overwrite")
-- MAGIC     .partitionBy("VendorId")
-- MAGIC     .format("parquet")
-- MAGIC     .save("dbfs:/mnt/optimizingdatabricks/delta_lake_demos/Output/YellowTaxis.parquet")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### (C) Write data in Delta Format

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC (
-- MAGIC     yellowTaxiDF.write.mode("overwrite")
-- MAGIC     .partitionBy("VendorId")
-- MAGIC     .format("delta")
-- MAGIC     .save("dbfs:/mnt/optimizingdatabricks/delta_lake_demos/Output/YellowTaxis.delta")
-- MAGIC )

-- COMMAND ----------

-- Transaction Log Entry  - _delta_log/00000000000000000000.json

--    commitInfo  - commit information  - It contains time at which commit was made and who made the commit. what operation was performed. It also contains operation metrics, like, how many files, bytes and output rows are written

-- {"commitInfo":{"timestamp":1691407156739,"userId":"###############","userName":"##########@###.###","operation":"WRITE","operationParameters":{"mode":"Overwrite","partitionBy":"[\"VendorId\"]"},"notebook":{"notebookId":"################"},"clusterId":"####-#######-########","isolationLevel":"WriteSerializable","isBlindAppend":false,"operationMetrics":{"numFiles":"23","numOutputRows":"9999995","numOutputBytes":"247916659"},"engineInfo":"Databricks-Runtime/12.2.x-scala2.12","txnId":"########-####-####-####-############"}}


--    metaData  - This includes the schema like column name, its data type, if it is nullable and other properties

-- {"metaData":{"id":"1da967f5-f4a0-412e-81ec-edbb3ccdca29","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"RideId\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"VendorId\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"PickupTime\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"DropTime\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"PickupLocationId\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"DropLocationId\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"CabNumber\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"DriverLicenseNumber\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"PassengerCount\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"TripDistance\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"RatecodeId\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"PaymentType\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"TotalAmount\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"FareAmount\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"Extra\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"MtaTax\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"TipAmount\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ToolsAmount\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ImprovementSurcharge\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["VendorId"],"configuration":{},"createdTime":1691407112424}}


--    Finally, it contains file operations like which files were added removed, etc, and the path of the file is stored

-- {"add":{"path":"VendorId=1/part-00000-622f8613-f359-4109-aa32-85d3826d43e8.c000.snappy.parquet","partitionValues":{"VendorId":"1"},"size":11029171,"modificationTime":1691407126000,"dataChange":true,"stats":"{\"numRecords\":433622,\"minValues\":{\"RideId\":1,\"PickupTime\":\"2022-03-01T00:00:00.000Z\",\"DropTime\":\"2022-03-01T00:01:32.000Z\",\"PickupLocationId\":1,\"DropLocationId\":1,\"CabNumber\":\"109ELITE\",\"DriverLicenseNumber\":\"110654\",\"PassengerCount\":0,\"TripDistance\":0.0,\"RatecodeId\":1,\"PaymentType\":1,\"TotalAmount\":0.0,\"FareAmount\":0.0,\"Extra\":0.0,\"MtaTax\":0.0,\"TipAmount\":0.0,\"ToolsAmount\":0.0,\"ImprovementSurcharge\":0.0},\"maxValues\":{\"RideId\":1085497,\"PickupTime\":\"2022-03-04T21:10:01.000Z\",\"DropTime\":\"2022-03-04T22:11:55.000Z\",\"PickupLocationId\":265,\"DropLocationId\":265,\"CabNumber\":\"ZRQ\",\"DriverLicenseNumber\":\"93280\",\"PassengerCount\":8,\"TripDistance\":84.8,\"RatecodeId\":99,\"PaymentType\":4,\"TotalAmount\":921.8,\"FareAmount\":700.0,\"Extra\":84.0,\"MtaTax\":65.0,\"TipAmount\":200.0,\"ToolsAmount\":825.0,\"ImprovementSurcharge\":0.3},\"nullCount\":{\"RideId\":0,\"PickupTime\":0,\"DropTime\":0,\"PickupLocationId\":0,\"DropLocationId\":0,\"CabNumber\":0,\"DriverLicenseNumber\":0,\"PassengerCount\":0,\"TripDistance\":0,\"RatecodeId\":0,\"PaymentType\":0,\"TotalAmount\":0,\"FareAmount\":0,\"Extra\":0,\"MtaTax\":0,\"TipAmount\":0,\"ToolsAmount\":0,\"ImprovementSurcharge\":0}}","tags":{"INSERTION_TIME":"1691407126000000","MIN_INSERTION_TIME":"1691407126000000","MAX_INSERTION_TIME":"1691407126000000","OPTIMIZE_TARGET_SIZE":"268435456"}}}
-- {"add":{"path":"VendorId=2/part-00000-cc4c60bd-ebda-4ff8-b403-ce179b5e9352.c000.snappy.parquet","partitionValues":{"VendorId":"2"},"size":15976289,"modificationTime":1691407129000,"dataChange":true,"stats":"{\"numRecords\":635467,\"minValues\":{\"RideId\":4,\"PickupTime\":\"2022-03-01T00:00:00.000Z\",\"DropTime\":\"2022-03-01T00:00:00.000Z\",\"PickupLocationId\":1,\"DropLocationId\":1,\"CabNumber\":\"1023BOR\",\"DriverLicenseNumber\":\"126875\",\"PassengerCount\":0,\"TripDistance\":0.0,\"RatecodeId\":1,\"PaymentType\":1,\"TotalAmount\":-210.3,\"FareAmount\":-210.0,\"Extra\":-4.5,\"MtaTax\":-0.5,\"TipAmount\":-80.88,\"ToolsAmount\":-31.32,\"ImprovementSurcharge\":-0.3},\"maxValues\":{\"RideId\":1085498,\"PickupTime\":\"2022-03-04T21:10:01.000Z\",\"DropTime\":\"2022-03-05T21:04:40.000Z\",\"PickupLocationId\":265,\"DropLocationId\":265,\"CabNumber\":\"ZRQ\",\"DriverLicenseNumber\":\"93280\",\"PassengerCount\":9,\"TripDistance\":117.25,\"RatecodeId\":99,\"PaymentType\":4,\"TotalAmount\":1302.8,\"FareAmount\":1302.0,\"Extra\":4.5,\"MtaTax\":0.5,\"TipAmount\":371.37,\"ToolsAmount\":64.26,\"ImprovementSurcharge\":0.3},\"nullCount\":{\"RideId\":0,\"PickupTime\":0,\"DropTime\":0,\"PickupLocationId\":0,\"DropLocationId\":0,\"CabNumber\":0,\"DriverLicenseNumber\":0,\"PassengerCount\":0,\"TripDistance\":0,\"RatecodeId\":0,\"PaymentType\":0,\"TotalAmount\":0,\"FareAmount\":0,\"Extra\":0,\"MtaTax\":0,\"TipAmount\":0,\"ToolsAmount\":0,\"ImprovementSurcharge\":0}}","tags":{"INSERTION_TIME":"1691407126000001","MIN_INSERTION_TIME":"1691407126000001","MAX_INSERTION_TIME":"1691407126000001","OPTIMIZE_TARGET_SIZE":"268435456"}}}

