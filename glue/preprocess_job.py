import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import date_format, substring
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
 
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
 
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
 
datasource0 = glueContext. \
  create_dynamic_frame. \
  from_catalog(
    database = "itvghlandingdb",
    table_name = "ghactivitycsv",
    transformation_ctx = "datasource0"
  )
 
df = datasource0. \
  toDF(). \
  withColumn('year', date_format(substring('created_at', 1, 10), 'yyyy')). \
  withColumn('month', date_format(substring('created_at', 1, 10), 'MM')). \
  withColumn('day', date_format(substring('created_at', 1, 10), 'dd'))
 
dyf = DynamicFrame.fromDF(dataframe=df, glue_ctx=glueContext, name="dyf")
 
datasink4 = glueContext. \
  write_dynamic_frame. \
  from_options(frame=dyf,
    connection_type="s3",
    connection_options={"path": "s3://github-activity-gb/raw/ghactivity/",
      "compression": "snappy",
      "partitionKeys": ["year", "month", "day"]},
    format="glueparquet",
    transformation_ctx="datasink4")
 
job.commit()