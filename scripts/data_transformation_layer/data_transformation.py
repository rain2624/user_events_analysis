# importing libraries
import sys
import boto3
import logging
import great_expectations as gx
from user_events_function import great_expectations_validations
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import *


# initialize glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'WORKFLOW_NAME', 'WORKFLOW_RUN_ID', 'S3_OUTPUT_PATH'])

# Apache iceberg - config
spark = SparkSession.builder\
                    .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
                    .config("spark.sql.catalog.glue_iceberg", "org.apache.iceberg.spark.SparkCatalog")\
                    .config("spark.sql.catalog.glue_iceberg.warehouse",args['S3_OUTPUT_PATH'])\
                    .config("spark.sql.catalog.glue_iceberg.catalog-impl",  "org.apache.iceberg.aws.glue.GlueCatalog") \
                    .getOrCreate()


# Setting up glue context and glue workflow
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


output_path = args['S3_OUTPUT_PATH']
workflow_name = args['WORKFLOW_NAME']
workflow_run_id = args['WORKFLOW_RUN_ID']

glue_client = boto3.client("glue")
run_props = glue_client.get_workflow_run_properties(
    Name=workflow_name,
    RunId=workflow_run_id 
)
input_path=run_props['RunProperties']['S3_INPUT_PATH']



# Setting up logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
data_transf_logger = logging.getLogger(__name__)



# 1. Reading raw data 
data_transf_logger.info("1. Reading raw data")

user_int_df = (spark.read.json(input_path))

total_rows = user_int_df.count()
data_transf_logger.info(f"Total rows - {total_rows}")

# 2. Standardize column names

# No need to change column name it is perfect already.


# 3. Convert data types

user_int_df = user_int_df.withColumn("event_timestamp", col("event_timestamp").cast("timestamp_ntz")) \
                        .withColumn("ingestion_timestamp", col("ingestion_timestamp").cast("timestamp_ntz"))


data_transf_logger.info("2. Completed with casting data types.")


# 4. Clean and standardize values
user_int_df = (user_int_df.withColumn("country", initcap(trim(col("country"))))
                        .withColumn("device_type", lower(trim(col("device_type"))))
                        .withColumn("event_id", upper(trim(col("event_id"))))
                        .withColumn("event_type", lower(trim(col("event_type"))))
                        .withColumn('event_date', to_date(col("event_timestamp")))    
                        .withColumn("platform", lower(trim(col("platform"))))
                        .withColumn("product_id", upper(trim(col("product_id"))))
                        .withColumn("session_id", upper(trim(col("session_id"))))
                        .withColumn("traffic_source", lower(trim(col("traffic_source"))))
                        .withColumn("user_id", upper(trim(col("user_id")))))



# aranging columns 
user_int_df = user_int_df.select(
    'event_id', 'event_timestamp', 'event_date','event_type','user_id', 'ingestion_timestamp','platform',
     'product_id', 'session_id', 'traffic_source','country', 'device_type')

data_transf_logger.info("3. Clean and Standardizing data values.")


# 5. Remove duplicates
data_transf_logger.info("4. Started with duplicate removal")
total_rows = user_int_df.count()
data_transf_logger.info(f"Total rows before duplicate removal: {total_rows} rows.")

events_dup_df = user_int_df.groupBy(user_int_df.columns).count().filter("count > 1")
dup_rows = events_dup_df.count()
data_transf_logger.info(f"Total exact duplicates in data: {dup_rows}")

user_int_df = user_int_df.dropDuplicates()

# also checking for event_id dups: as each event_id represents unique events.
dups_event_id_count = user_int_df.groupBy("event_id").count().filter("count> 1").count()
data_transf_logger.info(f"Duplicate event_id - {dups_event_id_count} rows")
user_int_df = user_int_df.dropDuplicates(["event_id"])


total_rows = user_int_df.count()
data_transf_logger.info(f"Total rows after removing duplicates: {total_rows} rows")


# 6. Handle null values
data_transf_logger.info("5. Starting with Null removal process:")
mandatory_cols = ['event_id',
 'event_timestamp',
 'event_type',
 'ingestion_timestamp',
 'session_id',
 'user_id']

null_count_dict = user_int_df.select([count(when(col(c).isNull(), c)).alias(c) for c in mandatory_cols]).collect()[0].asDict()


for key, val in null_count_dict.items():
    if val > 0 and key in  mandatory_cols:
        user_int_df = user_int_df.filter(col(key).isNotNull())
        data_transf_logger.warning(f"Null found for column {key} and null rows - {val}")


# Now one more thing to be verified except event_type = 'page_view' rest other event_type must not have null product_id
product_count_null = user_int_df.filter((col("event_type") != "page_view") & (col("product_id").isNull())).count()

data_transf_logger.warning(f"Null found for product id having event type other than page view - {product_count_null} rows")

# lets remove it 
user_int_df = user_int_df.filter(((col("event_type") != "page_view") & col("product_id").isNotNull()) | (col("event_type") == "page_view"))


user_int_df = user_int_df.fillna({
    'country': 'unknown',
    'device_type': 'unknown',
    'platform': 'others',
    'traffic_source': 'others'
})


total_rows = user_int_df.count()
data_transf_logger.info(f"Total rows in user interaction data after removal of null values from important columns and replacing few of them in other columns: {total_rows} rows")


# 7. Validate data using Great Expectations

data_transf_logger.info("6. Starting with data validation using Great Expectations")
data_transf_logger.info("""
We are looking into the following validations with the help of great expectations:
- Unique values:
  1. event_id
- Not null values:
  1. event_id
  2. event_timestamp
  3. event_type
  4. user_id
  5. session_id
  6. product_id (except event_type='page_view')
""")


# We are looking into the following validations with the help of great expectations:
# - Unique values:
#   1. event_id
# - Not null values:
#   1. event_id
#   2. event_timestamp
#   3. event_type
#   4. user_id
#   5. session_id
#   6. product_id (except event_type='page_view')
# 

df_source_name = 'user_events'
df = user_int_df
layer = 'silver'
expectation_list = [gx.expectations.ExpectColumnValuesToBeUnique(column="event_id"),
gx.expectations.ExpectColumnValuesToNotBeNull(column="event_id"),
gx.expectations.ExpectColumnValuesToNotBeNull(column="event_timestamp"),
gx.expectations.ExpectColumnValuesToNotBeNull(column="event_type"),
gx.expectations.ExpectColumnValuesToNotBeNull(column="user_id"),
gx.expectations.ExpectColumnValuesToNotBeNull(column="session_id"),
gx.expectations.ExpectColumnValuesToNotBeNull(column="product_id", row_condition='event_type != "page_view"', condition_parser="spark")]

validation_results = great_expectations_validations.data_validation(df_source_name, df, expectation_list, layer)

for result in validation_results.results:
    data_transf_logger.info(f"Expectation: {result.expectation_config.type}| ")
    data_transf_logger.info(f"Parameters: {result.expectation_config.kwargs}| ")
    data_transf_logger.info(f"Success: {result.success}")
if not validation_results.success:
    data_transf_logger.error("Great Expectations validation failed.")
    raise Exception("Data validation failed.")


# 8. Inserting useful columns

data_transf_logger.info("7. Preparing for data insertion, adding data transformed ingestion time to data.")


user_int_df = (
    user_int_df.withColumn(
        "data_transformed_ingested_timestamp",
        current_timestamp()
    )
)


# 9. Write clean data to Silver/ Data processed layer

total_rows = user_int_df.count()
data_transf_logger.info(f"Total rows before data insertion - {total_rows}")

user_int_df.createOrReplaceTempView("source_user_interactions")
 # Here comes the iceberg part
spark.sql("CREATE DATABASE IF NOT EXISTS glue_iceberg.silver_db")

spark.sql("""CREATE TABLE IF NOT EXISTS glue_iceberg.silver_db.user_interactions_processed (
    event_id string NOT NULL,
    event_timestamp timestamp_ntz NOT NULL,
    event_date date NOT NULL,
    event_type string NOT NULL, 
    user_id string NOT NULL,
    ingestion_timestamp timestamp_ntz,
    platform string,  
    product_id string, 
    session_id string NOT NULL,
    traffic_source string, 
    country string,  
    device_type string, 
    data_transformed_ingested_timestamp timestamp)
USING iceberg
PARTITIONED BY (event_date)""")

spark.sql("""
MERGE INTO glue_iceberg.silver_db.user_interactions_processed AS target
USING source_user_interactions AS source
ON target.event_id = source.event_id

WHEN MATCHED THEN
  UPDATE SET
    target.event_timestamp = source.event_timestamp,
    target.event_date = source.event_date,
    target.event_type = source.event_type,
    target.user_id = source.user_id,
    target.ingestion_timestamp = source.ingestion_timestamp,
    target.platform = source.platform,
    target.product_id = source.product_id,
    target.session_id = source.session_id,
    target.traffic_source = source.traffic_source,
    target.country = source.country,
    target.device_type = source.device_type,
    target.data_transformed_ingested_timestamp =
        source.data_transformed_ingested_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    event_id,
    event_timestamp,
    event_date,
    event_type,
    user_id,
    ingestion_timestamp,
    platform,
    product_id,
    session_id,
    traffic_source,
    country,
    device_type,
    data_transformed_ingested_timestamp
  )
  VALUES (
    source.event_id,
    source.event_timestamp,
    source.event_date,
    source.event_type,
    source.user_id,
    source.ingestion_timestamp,
    source.platform,
    source.product_id,
    source.session_id,
    source.traffic_source,
    source.country,
    source.device_type,
    source.data_transformed_ingested_timestamp
  )
""")
data_transf_logger.info("Data added to S3.")

# Getting event_date for which we proccessed data
event_date_list = [row['event_date'] for row in user_int_df.select('event_date').distinct().collect()]
event_date_str = ",".join([str(d) for d in event_date_list])

data_transf_logger.info(f"Handshake: Passing event dates for the data that is being processed.")

# 10. Handshake block
glue_client.put_workflow_run_properties(
    Name=workflow_name,
    RunId=workflow_run_id,
    RunProperties={
        'SILVER_DATA_PATH': "glue_iceberg.silver_db.user_interactions_processed",
        'PROCESSED_DATES': event_date_str
    }
)

job.commit()
