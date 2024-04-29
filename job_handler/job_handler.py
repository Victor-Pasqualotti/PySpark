from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Minhas importações
import boto3
import json

# Get the resolved options (including JOB_NAME and JOB_RUN_ID)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Spark context and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize the Glue job
job = Job(glueContext)

# Pass the job name and job ID to job.init() along with other arguments
job.init(args['JOB_NAME'], args)


#######################################################################
# PRIMEIRA ETAPA: RESGATA ARQUIVOS DO S3
# 1) Obtém conteúdo do arquivo .json de congiuração do projeto
# 2) Obtém conteúdo do arquivo .sql de ETL do projeto
#######################################################################

# Initialize Boto3 to use the S3 client
s3_client = boto3.client('s3')

# Specify the S3 bucket name and the key (file path)
bucket_name = 'your-bucket-name'
config_key  = f'path/to/your/{args["JOB_NAME"]}.json'
sql_key     = f'path/to/your/{args["JOB_NAME"]}.sql'

# Get the file content
config_response     = s3_client.get_object(Bucket=bucket_name, Key=config_key)
config_file_content = config_response['Body'].read().decode('utf-8')

sql_response        = s3_client.get_object(Bucket=bucket_name, Key=sql_key)
sql_file_content    = sql_response['Body'].read().decode('utf-8')

# Parse the JSON content
config_content  = json.loads(config_file_content)
sql_content     = json.loads(sql_file_content)

# TO DO: Resgatar predicado da tabela origem
#######################################################################
# SEGUNDA ETAPA: Resgata predicado para leitura das origens
# 1) Lê dados da origem via DynamicFrame e Data Catalog
# 2) Transforma DynamicFrame para Spark DataFrame
# 3) Define tempView como "{database}__{table_name}""
#######################################################################

# Access specific keys in the JSON
etl = config_content['ETL']

for origin in etl['origins']:
    locals()[f'df_partitions_{origin["database"]}_{origin["table_name"]}'] = \
    spark.sql(f""" SHOW PARTITIONS {origin["database"]}.{origin["table_name"]}""")



#######################################################################
# TERCEIRA ETAPA: Cria TempView para cada origem na configuração do projeto
# 1) Lê dados da origem via DynamicFrame e Data Catalog
# 2) Transforma DynamicFrame para Spark DataFrame
# 3) Define tempView como "{database}__{table_name}""
#######################################################################

for origin in etl['origins']:

    # Cria DynamicFrame
    locals()[f'dyf_{origin["database"]}_{origin["table_name"]}'] = \
        glueContext.create_dynamic_frame_from_catalog(
            database            = origin["database"], 
            tbale_name          = origin["table_name"], 
            transformation_ctx  = f"Read origin: {origin['database']}.{origin['table']}", 
            push_down_predicate = "", 
            additional_options  = {}
        )
    
    # Cria DataFrame
    locals()[f'df_{origin["database"]}_{origin["table_name"]}'] = \
        f'dyf_{origin["database"]}_{origin["table_name"]}'.toDF()
    
    # Cria TempView com os dados da origem
    locals()[f'df_{origin["database"]}_{origin["table_name"]}']\
        .creatOrReplaceTempView(f'{origin["database"]}__{origin['table']}')

#######################################################################
# QUARTA ETAPA: Lê arquivo SQL e executa cada query em spark sql
# 1) Separa o conteúdo do arquivo SQL por ';' para obter cada query
# 2) Remove os caracteres de quebra de linha '\n' e identação '\t'
# 3) Executa o script em Spark SQL
#######################################################################

translator = str.maketrans({chr(10): '', chr(9): ''})
for query in sql_content.split(';'):
    spark.sql(
        f"""
        {query.translate(translator)}
        """
    )

# Commit the job
job.commit()


