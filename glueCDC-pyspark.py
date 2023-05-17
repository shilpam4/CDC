import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import when
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ['s3_target_path_key','s3_target_path_bucket'])

bucket = args['s3_target_path_bucket']
fileName = args['s3_target_path_key']

print(bucket+" "+fileName)

spark = SparkSession.builder.appName("CDC").getOrCreate()

inputFilePath = f"s3a://{bucket}/{fileName}"
finalFilePath = f"s3a://cdc-output-pyspark/output"
if "LOAD" in fileName:
    fullloaddf = spark.read.csv(inputFilePath)
    fullloaddf = fullloaddf.withColumnRenamed("_c0","id").withColumnRenamed("_c1","FullName").withColumnRenamed("_c2","City")
    fullloaddf.write.mode("overwrite").csv(finalFilePath)
else:
    updateddf = spark.read.csv(inputFilePath)
    updateddf = updateddf.withColumnRenamed("_c0","action").withColumnRenamed("_c1","id").withColumnRenamed("_c2","FullName").withColumnRenamed("_c3","City")
    finalFiledf = spark.read.csv(finalFilePath)
    finalFiledf = finalFiledf.withColumnRenamed("_c0","id").withColumnRenamed("_c1","FullName").withColumnRenamed("_c2","City")
    for row in updateddf.collect():
        if row['action'] == 'U':
            finalFiledf = finalFiledf.withColumn("FullName", when(finalFiledf['id'] == row['id'], row['FullName']).otherwise(finalFiledf['FullName']))
            finalFiledf = finalFiledf.withColumn("City", when(finalFiledf['id'] == row['id'], row['City']).otherwise(finalFiledf['City']))
        if row['action'] == 'I':
            insertedRow = [list(row)[1:]]
            columns = ['id', 'FullName','City']
            newdf = spark.createDataFrame(insertedRow, columns)
            finalFiledf = finalFiledf.union(newdf)
        if row['action'] == 'D':
            finalFiledf =finalFiledf.filter(finalFiledf.id != row['id'])
    
    finalFiledf.write.mode("overwrite").csv(finalFilePath)
