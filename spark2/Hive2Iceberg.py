#****************************************************************************
# (C) Cloudera, Inc. 2020-2022
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Paul de Fusco
#***************************************************************************/

# NB: THIS SCRIPT REQUIRES A SPARK 3 CLUSTER

#---------------------------------------------------
#               CREATE SPARK SESSION
#---------------------------------------------------

from pyspark.sql import SparkSession
from datetime import datetime
import sys

s3BucketName = "s3a://goes-se-sandbox01/cnelson2/cde-workshop"
prefix = 'cnelson2'

spark = SparkSession \
    .builder \
    .appName("Iceberg Load") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")\
    .config("spark.sql.catalog.spark_catalog.type", "hive")\
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
    .config("spark.yarn.access.hadoopFileSystems", s3BucketName)\
    .getOrCreate()

#---------------------------------------------------
#               MIGRATE HIVE TABLE TO ICEBERG TABLE
#---------------------------------------------------
query_1=f"""CALL spark_catalog.system.migrate('{prefix}_CAR_DATA.CAR_SALES')"""
print(query_1)
spark.sql(query_1)

query_2 = f"""CALL spark_catalog.system.snapshot('{prefix}_CAR_DATA.CUSTOMER_DATA', '{prefix}_CAR_DATA.CUSTOMER_DATA_ICE')"""
print(query_2)
spark.sql(query_2)

#---------------------------------------------------
#               SHOW ICEBERG TABLE SNAPSHOTS
#---------------------------------------------------

spark.read.format("iceberg").load(f"spark_catalog.{prefix}_CAR_DATA.CAR_SALES.history").show(20, False)

# SAVE TIMESTAMP BEFORE INSERTS
now = datetime.now()

timestamp = datetime.timestamp(now)
print("PRE-INSERT TIMESTAMP: ", timestamp)

#---------------------------------------------------
#               INSERT DATA
#---------------------------------------------------

# PRE-INSERT COUNT
print("PRE-INSERT COUNT")
spark.sql(f"SELECT COUNT(*) FROM spark_catalog.{prefix}_CAR_DATA.CAR_SALES").show()

# INSERT DATA APPROACH 1 - APPEND FROM DATAFRAME
temp_df = spark.sql(f"SELECT * FROM spark_catalog.{prefix}_CAR_DATA.CAR_SALES").sample(fraction=0.3, seed=3)
temp_df.writeTo(f"spark_catalog.{prefix}_CAR_DATA.CAR_SALES").append()

# INSERT DATA APPROACH 2 - INSERT VIA SQL
spark.sql(f"DROP TABLE IF EXISTS spark_catalog.{prefix}_CAR_DATA.CAR_SALES_SAMPLE")
temp_df.writeTo(f"spark_catalog.{prefix}_CAR_DATA.CAR_SALES_SAMPLE").create()

print("INSERT DATA VIA SPARK SQL")
query_4 = f"""INSERT INTO spark_catalog.{prefix}_CAR_DATA.CAR_SALES SELECT * FROM spark_catalog.{prefix}_CAR_DATA.CAR_SALES_SAMPLE"""
print(query_4)
spark.sql(query_4)

#---------------------------------------------------
#               TIME TRAVEL
#---------------------------------------------------

# NOTICE SNAPSHOTS HAVE BEEN ADDED
spark.read.format("iceberg").load(f"spark_catalog.{prefix}_CAR_DATA.CAR_SALES.history").show(20, False)

# POST-INSERT COUNT
print("POST-INSERT COUNT")
spark.sql(f"SELECT COUNT(*) FROM spark_catalog.{prefix}_CAR_DATA.CAR_SALES").show()

# TIME TRAVEL AS OF PREVIOUS TIMESTAMP
df = spark.read.option("as-of-timestamp", int(timestamp*1000)).format("iceberg").load(f"spark_catalog.{prefix}_CAR_DATA.CAR_SALES")

# POST TIME TRAVEL COUNT
print("POST-TIME TRAVEL COUNT")
print(df.count())
