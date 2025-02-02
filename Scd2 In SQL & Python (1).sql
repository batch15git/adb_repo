-- Databricks notebook source
set spark.databricks.delta.commitValidation.enabled = False

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm('dbfs:/user/hive/warehouse/customers09' , True)
-- MAGIC dbutils.fs.rm('dbfs:/user/hive/warehouse/newcustomers09' , True)

-- COMMAND ----------

delete from  Customers09

-- COMMAND ----------

delete from NewCustomers09

-- COMMAND ----------

-- Create Customers table
CREATE or Replace TABLE Customers09 (
    CustomerID INT ,
    CustomerName VARCHAR(255),
    EffectiveStartDate DATE,
    EffectiveEndDate DATE,
    IsCurrent int
) using delta;

-- Create NewCustomers table
CREATE or Replace TABLE NewCustomers09 (
    CustomerID INT ,
    CustomerName VARCHAR(255)
) using delta;



-- COMMAND ----------

select *from Customers09

-- COMMAND ----------

-- Sample data for Customers table
INSERT INTO Customers09 (CustomerID, CustomerName, EffectiveStartDate, EffectiveEndDate, IsCurrent)
VALUES (1, 'John Doe', '2022-01-01', '9999-12-31', 1),
       (2, 'Jane Smith', '2022-01-01', '9999-12-31', 1),
       (3, 'Bob Johnson', '2022-01-01', '9999-12-31', 1);


-- COMMAND ----------


-- Sample data for NewCustomers table (including updates and inserts)
INSERT INTO NewCustomers09 (CustomerID, CustomerName)
VALUES (1, 'John Smith'),    -- Update existing record
       (4, 'Alice Brown');    -- Insert new record


-- COMMAND ----------

select *from Customers09

-- COMMAND ----------

select *from NewCustomers09

-- COMMAND ----------

select *from NewCustomers

-- COMMAND ----------

select *from Customersp order by customerId

-- COMMAND ----------

select *from NewCustomersp

-- COMMAND ----------

select *,'siva' as test1 from customers09

-- COMMAND ----------

SELECT  
        CustomerID as mergekey,
        CustomerID,
        CustomerName,
        GETDATE() AS EffectiveStartDate,
        '9999-12-31' AS EffectiveEndDate,
        1 AS IsCurrent
    FROM NewCustomers09

-- COMMAND ----------

SELECT
        Null as mergekey,
        CustomerID,
        CustomerName,
        GETDATE() AS EffectiveStartDate,
        '9999-12-31' AS EffectiveEndDate,
        1 AS IsCurrent
    FROM NewCustomers09 where exists (select * from Customers09 where Customers09.CustomerID = NewCustomers09.CustomerID )


-- COMMAND ----------

SELECT  
        CustomerID as mergekey,
        CustomerID,
        CustomerName,
        GETDATE() AS EffectiveStartDate,
        '9999-12-31' AS EffectiveEndDate,
        1 AS IsCurrent
    FROM NewCustomers09 
UNION
SELECT
        Null as mergekey,
        CustomerID,
        CustomerName,
        GETDATE() AS EffectiveStartDate,
        '9999-12-31' AS EffectiveEndDate,
        1 AS IsCurrent
    FROM NewCustomers09 where exists (select * from customers09 where customers09.CustomerID = NewCustomers09.CustomerID )

-- COMMAND ----------

select * , 'sankar' as test from customers09

-- COMMAND ----------

exists.....

-- COMMAND ----------

select * , 'sankar' as test from customers09
union 
select * , 'siva' as test from customers09

-- COMMAND ----------

select * from customers09


-- COMMAND ----------

select *from customers09

-- COMMAND ----------

SELECT  
        CustomerID as mergekey,
        CustomerID,
        CustomerName,
        GETDATE() AS EffectiveStartDate,
        '9999-12-31' AS EffectiveEndDate,
        1 AS IsCurrent
    FROM NewCustomers09 
UNION
SELECT
        Null as mergekey,
        CustomerID,
        CustomerName,
        GETDATE() AS EffectiveStartDate,
        '9999-12-31' AS EffectiveEndDate,
        1 AS IsCurrent
    FROM NewCustomers09 where exists (select * from customers09 where customers09.CustomerID = NewCustomers09.CustomerID )


    target.CustomerID = source.mergekey AND target.IsCurrent = 1
    1=1 and 1=1-----

    true===update
    false==insert---------------- 4 



-- COMMAND ----------

merge-----update, insert,delete----perform---

merge into customers09 as target 
using (SELECT  
        CustomerID as mergekey,
        CustomerID,
        CustomerName,
        GETDATE() AS EffectiveStartDate,
        '9999-12-31' AS EffectiveEndDate,
        1 AS IsCurrent
    FROM NewCustomers09 
UNION
SELECT
        Null as mergekey,
        CustomerID,
        CustomerName,
        GETDATE() AS EffectiveStartDate,
        '9999-12-31' AS EffectiveEndDate,
        1 AS IsCurrent
    FROM NewCustomers09 where exists (select * from customers09 where customers09.CustomerID = NewCustomers09.CustomerID )
    ) as source

-- COMMAND ----------

merge into customers09 as target
using ( SELECT  
        CustomerID as mergekey,
        CustomerID,
        CustomerName,
        GETDATE() AS EffectiveStartDate,
        '9999-12-31' AS EffectiveEndDate,
        1 AS IsCurrent
    FROM NewCustomers09 
UNION
SELECT
        Null as mergekey,
        CustomerID,
        CustomerName,
        GETDATE() AS EffectiveStartDate,
        '9999-12-31' AS EffectiveEndDate,
        1 AS IsCurrent
    FROM NewCustomers09 where exists (select * from customers09 where customers09.CustomerID = NewCustomers09.CustomerID )) as source
    target.CustomerID = source.mergekey AND target.IsCurrent = 1


-- COMMAND ----------

merge into customers09 as target
using () source
condtion
when mathced
when not matched

-- COMMAND ----------

-- Merge statement to handle SCD2 updates
MERGE INTO Customers09 AS target
USING ( SELECT  
        CustomerID as mergekey,
        CustomerID,
        CustomerName,
        GETDATE() AS EffectiveStartDate,
        '9999-12-31' AS EffectiveEndDate,
        1 AS IsCurrent
    FROM NewCustomers09 
UNION
SELECT
        Null as mergekey,
        CustomerID,
        CustomerName,
        GETDATE() AS EffectiveStartDate,
        '9999-12-31' AS EffectiveEndDate,
        1 AS IsCurrent
    FROM NewCustomers09 where exists (select * from customers09 where customers09.CustomerID = NewCustomers09.CustomerID )
    ) AS source
ON target.CustomerID = source.mergekey AND target.IsCurrent = 1
WHEN MATCHED THEN
    UPDATE SET target.IsCurrent = 0, target.EffectiveEndDate = GETDATE()
WHEN NOT MATCHED THEN
    INSERT (CustomerID, CustomerName, EffectiveStartDate, EffectiveEndDate, IsCurrent)
    VALUES (source.CustomerID, source.CustomerName, source.EffectiveStartDate, source.EffectiveEndDate, source.IsCurrent);

-- COMMAND ----------

SELECT  
        CustomerID as mergekey,
        CustomerID,
        CustomerName,
        GETDATE() AS EffectiveStartDate,
        '9999-12-31' AS EffectiveEndDate,
        1 AS IsCurrent
    FROM NewCustomers09 
UNION
SELECT
        Null as mergekey,
        CustomerID,
        CustomerName,
        GETDATE() AS EffectiveStartDate,
        '9999-12-31' AS EffectiveEndDate,
        1 AS IsCurrent
    FROM NewCustomers09 where exists (select * from customers09 where customers09.CustomerID = NewCustomers09.CustomerID )

-- COMMAND ----------

select *from Customers09

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from delta.tables import *
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.types import *

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.types import *
-- MAGIC from delta.tables import *

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Define the Delta table schema
-- MAGIC columns = [
-- MAGIC     StructField("CustomerID", IntegerType(), True),
-- MAGIC     StructField("CustomerName", StringType(), True),
-- MAGIC     StructField("EffectiveStartDate", DateType(), True),
-- MAGIC     StructField("EffectiveEndDate", DateType(), True),
-- MAGIC     StructField("IsCurrent", IntegerType(), True)
-- MAGIC ]
-- MAGIC
-- MAGIC # Create the Delta table
-- MAGIC schema = StructType(columns)
-- MAGIC DeltaTable.createIfNotExists(spark).tableName("Customers091").addColumns(schema).execute()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Define the Delta table schema
-- MAGIC columns = [
-- MAGIC     StructField("CustomerID", IntegerType(), True),
-- MAGIC     StructField("CustomerName", StringType(), True)
-- MAGIC    ]
-- MAGIC
-- MAGIC # Create the Delta table
-- MAGIC schema = StructType(columns)
-- MAGIC DeltaTable.createIfNotExists(spark).tableName("NewCustomers091").addColumns(schema).execute()

-- COMMAND ----------

-- Sample data for Customers table
INSERT INTO Customers091 (CustomerID, CustomerName, EffectiveStartDate, EffectiveEndDate, IsCurrent)
VALUES (1, 'John Doe', '2022-01-01', '9999-12-31', 1),
       (2, 'Jane Smith', '2022-01-01', '9999-12-31', 1),
       (3, 'Bob Johnson', '2022-01-01', '2022-02-15', 0);

-- Sample data for NewCustomers table (including updates and inserts)
INSERT INTO NewCustomers091 (CustomerID, CustomerName)
VALUES (1, 'John Smith'),    -- Update existing record
       (4, 'Alice Brown');    -- Insert new record


-- COMMAND ----------

-- MAGIC %python
-- MAGIC d1 = DeltaTable.forName(spark, "customers091") #------target should be table
-- MAGIC
-- MAGIC #-----dataframe
-- MAGIC
-- MAGIC
-- MAGIC df2 = DeltaTable.forName(spark, "NewCustomers091").toDF()
-- MAGIC df_3 = DeltaTable.forName(spark, "customers091").toDF()
-- MAGIC

-- COMMAND ----------

SELECT  
        CustomerID as mergekey,
        CustomerID,
        CustomerName,
        GETDATE() AS EffectiveStartDate,
        '9999-12-31' AS EffectiveEndDate,
        1 AS IsCurrent
    FROM NewCustomers09 


-- COMMAND ----------

-- MAGIC %python
-- MAGIC df3 = (
-- MAGIC     df2.withColumn("mergeKey", col("CustomerID"))
-- MAGIC     .withColumn("EffectiveStartDate", col("current_date"))
-- MAGIC     .withColumn("EffectiveEndDate", lit("9999-12-31"))
-- MAGIC     .withColumn("IsCurrent" , lit(1) )
-- MAGIC )

-- COMMAND ----------


SELECT
        Null as mergekey,
        CustomerID,
        CustomerName,
        GETDATE() AS EffectiveStartDate,
        '9999-12-31' AS EffectiveEndDate,
        1 AS IsCurrent
    FROM NewCustomers09 where exists (select * from customers09 where customers09.CustomerID = NewCustomers09.CustomerID )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df4 = df3.join(df_3 , ["CustomerID"] , 'leftsemi' ).withColumn('mergeKey' , lit(None) )   #----step2

-- COMMAND ----------

-- MAGIC %python
-- MAGIC d2 = df3.union(df4)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC d2.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC d2.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC d2.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC d1.alias("target").merge(
-- MAGIC     source=d2.alias("source"),
-- MAGIC     condition="target.CustomerID = source.mergekey AND target.IsCurrent = 1 ",
-- MAGIC ).whenMatchedUpdate(
-- MAGIC     set={"target.IsCurrent": "0", "target.EffectiveEndDate": "GETDATE()"}
-- MAGIC ).whenNotMatchedInsert(
-- MAGIC     values={
-- MAGIC         "CustomerID": "source.CustomerID",
-- MAGIC         "CustomerName": "source.CustomerName",
-- MAGIC         "EffectiveStartDate": "source.EffectiveStartDate",
-- MAGIC         "EffectiveEndDate": "source.EffectiveEndDate",
-- MAGIC         "IsCurrent": "source.IsCurrent"
-- MAGIC     }
-- MAGIC ).execute()

-- COMMAND ----------

select *from CustomersP order by customerId
