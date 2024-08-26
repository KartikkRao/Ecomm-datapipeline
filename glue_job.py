import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
from pyspark.sql.functions import *
from datetime import datetime 
from awsglue.dynamicframe import DynamicFrame
import boto3
s3_path = "Your_path"

df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s3_path)
df.printSchema()
df.show(1)
df = df.drop("category_code")
df = df.withColumn("brand", when(col("brand").isNull(), "other").otherwise(col("brand"))).show(10)
output_path = f"your_s3_path/ecomm_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"

df.write.mode("overwrite").option("header" , "true").format("csv").save(output_path)
s3 = boto3.client('s3')

bucket_name = 'Your_bucket'
prefix = 'raw_data/' #location

# List objects in the bucket with the specified prefix
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

#print(response)

if 'Contents' in response:
    for obj in response['Contents']:
        if obj['Key'] != 'raw_data/':
            s3.delete_object(Bucket=bucket_name, Key=obj['Key'])

job.commit()
