## This is the modified code of the AWS Glue Job generated pyspark code in order to some catogory string values (male and female) can be mapped to numerical values (0, or 1)
##  while transforming JSON format to csv format.
##  

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from itertools import chain
from pyspark.sql.functions import create_map, lit
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "userdata-json", table_name = "json_data_from_firehose_920", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "userdata-json", table_name = "json_data_from_firehose_920", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("first", "string", "first", "string"), ("last", "string", "last", "string"), ("age", "int", "age", "int"), ("gender", "string", "gender", "string"), ("latitude", "double", "latitude", "double"), ("longitude", "double", "longitude", "double")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
df = datasource0.toDF()
gender_dict = {'male': 1, 'female': 0}
mapping_expr = create_map([lit(x) for x in chain(*gender_dict.items())])
df = df.withColumn('gender', mapping_expr[df['gender']])
datasource_transformed = DynamicFrame.fromDF(df, glueContext, "datasource0")

applymapping1 = ApplyMapping.apply(frame = datasource_transformed, mappings = [("first", "string", "first", "string"), ("last", "string", "last", "string"), ("age", "int", "age", "int"), ("gender", "string", "gender", "string"), ("latitude", "double", "latitude", "double"), ("longitude", "double", "longitude", "double")], transformation_ctx = "applymapping1")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://userdata-json-to-csv-transformed"}, format = "csv", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping1]
datasink2 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", connection_options = {"path": "s3://userdata-json-to-csv-transformed"}, format = "csv", transformation_ctx = "datasink2")
job.commit()
