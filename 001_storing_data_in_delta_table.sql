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
-- MAGIC     .add("RideId", "integer")
-- MAGIC     .add("VendorId", "integer")
-- MAGIC     .add("PickupTime", "timestamp")
-- MAGIC     .add("DropTime", "timestamp")
-- MAGIC     .add("PickupLocationId", "integer")
-- MAGIC     .add("DropLocationId", "integer")
-- MAGIC     .add("CabNumber", "string")
-- MAGIC     .add("DriverLicenseNumber", "string")
-- MAGIC     .add("PassengerCount", "integer")
-- MAGIC     .add("TripDistance", "double")
-- MAGIC     .add("RatecodeId", "integer")
-- MAGIC     .add("PaymentType", "integer")
-- MAGIC     .add("TotalAmount", "double")
-- MAGIC     .add("FareAmount", "double")
-- MAGIC     .add("Extra", "double")
-- MAGIC     .add("MtaTax", "double")
-- MAGIC     .add("TipAmount", "double")
-- MAGIC     .add("TollsAmount", "double")
-- MAGIC     .add("ImprovementSurcharge", "double")
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


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### (D) Options to create Delta Tables

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS TaxisDB;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Option 1: Reference Data Location in Data Lake

-- COMMAND ----------

-- Create table based on Parquet data
CREATE TABLE TaxisDB.YellowTaxisParquet USING PARQUET LOCATION "dbfs:/mnt/optimizingdatabricks/delta_lake_demos/Output/YellowTaxis.parquet";

-- COMMAND ----------

-- Create table based on Delta data
CREATE TABLE TaxisDB.YellowTaxis USING DELTA LOCATION "dbfs:/mnt/optimizingdatabricks/delta_lake_demos/Output/YellowTaxis.delta";

-- COMMAND ----------

SELECT COUNT(*) FROM TaxisDB.YellowTaxis;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### (E) 'DESCRIBE TABLE' Commands

-- COMMAND ----------

DESCRIBE TABLE EXTENDED TaxisDB.YellowTaxis;

-- COMMAND ----------

 DESCRIBE DETAIL TaxisDB.YellowTaxis;

-- COMMAND ----------

DESCRIBE TABLE EXTENDED TaxisDB.YellowTaxisParquet;

-- COMMAND ----------

DESCRIBE DETAIL TaxisDB.YellowTaxisParquet;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### (F) Audit History of Delta Table
-- MAGIC
-- MAGIC This shows transaction log of Delta Table

-- COMMAND ----------

 DESCRIBE HISTORY TaxisDB.YellowTaxis;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### (G) More options to create Delta Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Option 2: Save DataFrame as a Table

-- COMMAND ----------

DROP TABLE TaxisDB.YellowTaxis;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.fs.rm(
-- MAGIC     "dbfs:/mnt/optimizingdatabricks/delta_lake_demos/Output/YellowTaxis.delta", True
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Save DataFrame as Delta Table
-- MAGIC
-- MAGIC (
-- MAGIC     yellowTaxiDF.write.mode("overwrite")
-- MAGIC     .partitionBy("VendorId")
-- MAGIC     .format("delta")
-- MAGIC     .option(
-- MAGIC         "path",
-- MAGIC         "dbfs:/mnt/optimizingdatabricks/delta_lake_demos/Output/YellowTaxis.delta",
-- MAGIC     )
-- MAGIC     .saveAsTable("TaxisDB.YellowTaxis")
-- MAGIC )
-- MAGIC
-- MAGIC # This not just save the files in DataLake but also registers the table

-- COMMAND ----------

DESCRIBE HISTORY TaxisDB.YellowTaxis;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Save DataFrame as Delta Table
-- MAGIC
-- MAGIC (
-- MAGIC     yellowTaxiDF.write.mode("overwrite")
-- MAGIC     .partitionBy("VendorId")
-- MAGIC     .format("delta")
-- MAGIC     .option(
-- MAGIC         "path",
-- MAGIC         "dbfs:/mnt/optimizingdatabricks/delta_lake_demos/Output/YellowTaxis.delta",
-- MAGIC     )
-- MAGIC     .saveAsTable("TaxisDB.YellowTaxis")
-- MAGIC )

-- COMMAND ----------

DESCRIBE HISTORY TaxisDB.YellowTaxis;

-- COMMAND ----------

-- Running the code will not replace the files
-- In case of an Overwrite operation on Delta Table, the existing part files are not deleted. It first adds  new part files and then add an entry into transaction log

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Option 3: Create Table Definition & Add Data

-- COMMAND ----------

DROP TABLE TaxisDB.YellowTaxis;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.fs.rm(
-- MAGIC     "dbfs:/mnt/optimizingdatabricks/delta_lake_demos/Output/YellowTaxis.delta", True
-- MAGIC )

-- COMMAND ----------

CREATE TABLE TaxisDB.YellowTaxis (
  RideId INT COMMENT 'This is the primary key column',
  VendorId INT,
  PickupTime TIMESTAMP,
  DropTime TIMESTAMP,
  PickupLocationId INT,
  DropLocationId INT,
  CabNumber STRING,
  DriverLicenseNumber STRING,
  PassengerCount INT,
  TripDistance DOUBLE,
  RatecodeId INT,
  PaymentType INT,
  TotalAmount DOUBLE,
  FareAmount DOUBLE,
  Extra DOUBLE,
  MtaTax DOUBLE,
  TipAmount DOUBLE,
  TollsAmount DOUBLE,
  ImprovementSurcharge DOUBLE,
  PickupYear INT GENERATED ALWAYS AS (YEAR(PickupTime)) COMMENT 'Auto-generated year from PickupTime',
  PickupMonth INT GENERATED ALWAYS AS (MONTH(PickupTime)) COMMENT 'Auto-generated month from PickupTime',
  PickupDay INT GENERATED ALWAYS AS (DAY(PickupTime)) COMMENT 'Auto-generated day from PickupTime'
) 
USING DELTA -- Default in Databricks isDelta
LOCATION "dbfs:/mnt/optimizingdatabricks/delta_lake_demos/Output/YellowTaxis.delta"
PARTITIONED BY (VendorId) -- optional
COMMENT 'This table stores ride information for Yellow Taxis';

-- This creates an empty table

-- COMMAND ----------

DESC TABLE EXTENDED TaxisDB.YellowTaxis;

-- COMMAND ----------

DESC HISTORY TaxisDB.YellowTaxis;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Options to Add Data to Delta Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Option 1: 'INSERT'
-- MAGIC  command

-- COMMAND ----------

INSERT INTO
  TaxisDB.YellowTaxis (
    RideId,
    VendorId,
    PickupTime,
    DropTime,
    PickupLocationId,
    DropLocationId,
    CabNumber,
    DriverLicenseNumber,
    PassengerCount,
    TripDistance,
    RatecodeId,
    PaymentType,
    TotalAmount,
    FareAmount,
    Extra,
    MtaTax,
    TipAmount,
    TollsAmount,
    ImprovementSurcharge
  )
VALUES
  (
    9999996,
    3,
    '2021-12-01T00:00:00.000Z',
    '2021-12-01T00:15:34.000Z',
    170,
    140,
    'TAC399',
    '5131685',
    1,
    2.9,
    1,
    1,
    15.3,
    13.0,
    0.5,
    0.5,
    1.0,
    0.0,
    0.3
  );

-- COMMAND ----------

SELECT
  *
FROM
  TaxisDB.YellowTaxis;

-- COMMAND ----------

DESCRIBE HISTORY TaxisDB.YellowTaxis;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Option 2: Append a DataFrame

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Extract new records from Data Lake
-- MAGIC yellowTaxisAppendDF = (
-- MAGIC     spark.read.option("header", "true")
-- MAGIC     .schema(yellowTaxiSchema)
-- MAGIC     .csv(
-- MAGIC         "dbfs:/mnt/optimizingdatabricks/delta_lake_demos/YellowTaxis_Additional/YellowTaxis_append.csv"
-- MAGIC     )
-- MAGIC )
-- MAGIC
-- MAGIC display(yellowTaxisAppendDF)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # append to data lake in DELTA format
-- MAGIC
-- MAGIC (
-- MAGIC     yellowTaxisAppendDF.write.mode("APPEND")
-- MAGIC     .partitionBy("VendorId")
-- MAGIC     .format("DELTA")
-- MAGIC     .save("dbfs:/mnt/optimizingdatabricks/delta_lake_demos/Output/YellowTaxis.delta")
-- MAGIC )

-- COMMAND ----------

SELECT
  *
FROM
  TaxisDB.YellowTaxis;

-- COMMAND ----------

DESCRIBE HISTORY TaxisDB.YellowTaxis;
-- Using INSERT command or appending a DataFrame, both are WRITE operations

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Option 3: COPY Command

-- COMMAND ----------

COPY INTO TaxisDB.YellowTaxis
FROM
  'dbfs:/mnt/optimizingdatabricks/delta_lake_demos/YellowTaxis/YellowTaxis/YellowTaxis1.csv'
FILEFORMAT = CSV          --  Other options: JSON, PARQUET, AVRO, ORC, TEXT, BINARYFILE
VALIDATE ALL              --  Other option: VALIDATE 10 ROWS
FORMAT_OPTIONS ('header' = 'true');

-- If folder is provided in FROM clause, files can be specified
-- Example: FILES = ('f1.csv', 'f2.csv', 'f3.csv')

-- VALIDATE clause will only check to see if file schema matches with the table and data can be loaded or not

-- Error in SQL statement: AnalysisException: Failed to merge fields 'RideId' and 'RideId'. Failed to merge incompatible data types IntegerType and StringType
--    Since we are reading CSV file, all data types are considered of STRING type

-- COMMAND ----------

COPY INTO TaxisDB.YellowTaxis
FROM
  (
    SELECT
      RideId :: Int,
      VendorId :: Int,
      PickupTime :: Timestamp,
      DropTime :: Timestamp,
      PickupLocationId :: Int,
      DropLocationId :: Int,
      CabNumber :: String,
      DriverLicenseNumber :: String,
      PassengerCount :: Int,
      TripDistance :: Double,
      RateCodeId :: Int,
      PaymentType :: Int,
      TotalAmount :: Double,
      FareAmount :: Double,
      Extra :: Double,
      MtaTax :: Double,
      TipAmount :: Double,
      TollsAmount :: Double,
      ImprovementSurcharge :: Double
    FROM
      'dbfs:/mnt/optimizingdatabricks/delta_lake_demos/YellowTaxis/YellowTaxis/YellowTaxis1.csv'
  )
FILEFORMAT = CSV
-- VALIDATE ALL
FORMAT_OPTIONS ('header' = 'true');

-- COMMAND ----------

DESCRIBE HISTORY TaxisDB.YellowTaxis;

-- COMMAND ----------

COPY INTO TaxisDB.YellowTaxis
FROM
  (
    SELECT
      RideId :: Int,
      VendorId :: Int,
      PickupTime :: Timestamp,
      DropTime :: Timestamp,
      PickupLocationId :: Int,
      DropLocationId :: Int,
      CabNumber :: String,
      DriverLicenseNumber :: String,
      PassengerCount :: Int,
      TripDistance :: Double,
      RateCodeId :: Int,
      PaymentType :: Int,
      TotalAmount :: Double,
      FareAmount :: Double,
      Extra :: Double,
      MtaTax :: Double,
      TipAmount :: Double,
      TollsAmount :: Double,
      ImprovementSurcharge :: Double
    FROM
      'dbfs:/mnt/optimizingdatabricks/delta_lake_demos/YellowTaxis/YellowTaxis/YellowTaxis1.csv'
  )
FILEFORMAT = CSV
-- VALIDATE ALL
FORMAT_OPTIONS ('header' = 'true');

-- COPY command keeps track of all the loaded files. If you try to reload the file, it will simply ignore it

-- COMMAND ----------

DESCRIBE HISTORY TaxisDB.YellowTaxis;

-- COMMAND ----------

-- This will load the file again even if it has been loaded before

COPY INTO TaxisDB.YellowTaxis
FROM
  (
    SELECT
      RideId :: Int,
      VendorId :: Int,
      PickupTime :: Timestamp,
      DropTime :: Timestamp,
      PickupLocationId :: Int,
      DropLocationId :: Int,
      CabNumber :: String,
      DriverLicenseNumber :: String,
      PassengerCount :: Int,
      TripDistance :: Double,
      RateCodeId :: Int,
      PaymentType :: Int,
      TotalAmount :: Double,
      FareAmount :: Double,
      Extra :: Double,
      MtaTax :: Double,
      TipAmount :: Double,
      TollsAmount :: Double,
      ImprovementSurcharge :: Double
    FROM
      'dbfs:/mnt/optimizingdatabricks/delta_lake_demos/YellowTaxis/YellowTaxis/YellowTaxis1.csv'
  )
FILEFORMAT = CSV
-- VALIDATE ALL
FORMAT_OPTIONS ('header' = 'true')
COPY_OPTIONS ('force' = 'true');

-- COMMAND ----------


