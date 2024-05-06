# Import Padrão
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Import adicionais
import boto3
import json
import pyspark.sql.functions as sf
from pyspark.sql.type import *

# Custom
from s3.AppS3 import AppS3
from glue.AppGlueJob import AppGlueJob

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Glue Context e Spark Session
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Instancia Glue Job
job = Job(glueContext)

# Inicio
job.init(args['JOB_NAME'], args)



job.commit()