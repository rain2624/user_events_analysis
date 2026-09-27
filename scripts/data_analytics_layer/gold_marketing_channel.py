# Which marketing channel is bringing valuable/engaged users?                    
# grain - day, traffic_source and country.

# importing libraries
import sys
import boto3
import logging
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import great_expectations as gx
from user_events_function import great_expectations_validations


args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'WORKFLOW_NAME', 'WORKFLOW_RUN_ID', 'S3_OUTPUT_PATH']
)

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


# Setting up logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
marketing_channel_logger = logging.getLogger(__name__)
marketing_channel_logger.info("Started with creating Marketing Channel Data.")
marketing_channel_logger.info("Each row in this data represents records for unique day, traffic_source and country")

workflow_name = args["WORKFLOW_NAME"]
workflow_run_id = args["WORKFLOW_RUN_ID"]
output_path =args["S3_OUTPUT_PATH"]

# handshake
glue_client = boto3.client('glue')
response=glue_client.get_workflow_run_properties(
    Name=workflow_name,
    RunId=workflow_run_id
)

run_props = response.get('RunProperties', {})

input_path = run_props.get('SILVER_DATA_PATH')
processed_dates_str = run_props.get('PROCESSED_DATES')

if not processed_dates_str:
    marketing_channel_logger.error("PROCESSED_DATES was not found")
    raise KeyError("PROCESSED_DATES is missing from the workflow context.")

# input_path=run_props['RunProperties']['SILVER_DATA_PATH']
# processed_dates_str=run_props['RunProperties']['PROCESSED_DATES']
formatted_date_str = ', '.join([f"'{d.strip()}'" for d in processed_dates_str.split(',')])

marketing_channel_logger.info(f"Handshake Successful! Resolved path from Silver: {input_path}")

# 1. Read data
marketing_channel_logger.info("1. Started with reading data from silver/transformed layer")

user_interactions_df = spark.sql(f"""Select * from {input_path} where event_date in ({formatted_date_str})""")
total_rows = user_interactions_df.count()
marketing_channel_logger.info(f"Total rows in user interactions data: {total_rows}")

# 2. Creating business metrics table
marketing_channel_logger.info("2. Creating Marketing Channel data")
gold_marketing_channel_df = user_interactions_df.groupBy("event_date", "country", "traffic_source") \
                                            .agg(
                                                countDistinct(col("user_id")).alias("total_users"),
                                                countDistinct(col("session_id")).alias("total_sessions"),
                                                count("*").alias("total_events"),
                                                countDistinct(when(col('event_type') == 'product_view', col('user_id')).otherwise(None)).alias('product_viewers'),
                                                countDistinct(when(col('event_type') == 'add_to_cart', col('user_id')).otherwise(None)).alias('cart_users'),
                                                countDistinct(when(col('event_type') == 'checkout', col('user_id')).otherwise(None)).alias('checkout_users'),
                                                countDistinct(when(col('event_type') == 'purchase', col('user_id')).otherwise(None)).alias('purchasers')
                                            )


gold_marketing_channel_df = gold_marketing_channel_df.withColumn('purchase_conversion_rate', when(col('total_users')>0, round(col('purchasers')*100/col('total_users'),2)).otherwise(0.0)) \
                                                    .withColumn('events_per_session', when(col('total_sessions') >0, round(col('total_events')/col('total_sessions'),2)).otherwise(0.0)) 


# 3. Adding surrogate key
marketing_channel_logger.info("3. Adding Surrogate Key.")
gold_marketing_channel_df = gold_marketing_channel_df.withColumn('marketing_channel_key', sha2(
                                                        concat_ws(
                                                            "|",
                                                            col('event_date').cast("string"),
                                                            col("country"),
                                                            col("traffic_source")
                                                        ),
                                                        256
                                                    ))



# arranging cols 
gold_marketing_channel_df = gold_marketing_channel_df.select(
 'marketing_channel_key',
 'event_date',
 'country',
 'traffic_source',
 'total_users',
 'total_sessions',
 'total_events',
 'product_viewers',
 'cart_users',
 'checkout_users',
 'purchasers',
 'purchase_conversion_rate',
 'events_per_session')

total_rows = gold_marketing_channel_df.count()
marketing_channel_logger.info(f"Created Marketing Channel Data with total rows - {total_rows}")

# 4. Validating data

# event_date → Not null
# purchase_conversion_rate → Between 0 and 100
# events_per_session → Greater than or equal to 0

marketing_channel_logger.info("4. Starting with data validation\n")
marketing_channel_logger.info("""We are looking into the following validations with the help of great expectations:
- Not Null Values
    1. event_date
    2. country
    3. traffic_source
- Values Btw 0 and 100
    1. purchase_conversion_rate
- Values greater than 0
    1. events_per_session
""")


df_source_name = 'marketing_channel'
df = gold_marketing_channel_df
layer = 'gold'
expectation_list = [
gx.expectations.ExpectColumnValuesToNotBeNull(column="event_date"),
gx.expectations.ExpectColumnValuesToNotBeNull(column="country"),
gx.expectations.ExpectColumnValuesToNotBeNull(column="traffic_source"),
gx.expectations.ExpectColumnValuesToBeBetween(column="purchase_conversion_rate", min_value=0, max_value=100),
gx.expectations.ExpectColumnValuesToBeBetween(column="events_per_session", min_value=0, max_value=None)
]

validation_results = great_expectations_validations.data_validation(df_source_name, df, expectation_list, layer)

for result in validation_results.results:
    marketing_channel_logger.info(f"Expectation: {result.expectation_config.type}| ")
    marketing_channel_logger.info(f"Parameters: {result.expectation_config.kwargs}| ")
    marketing_channel_logger.info(f"Success: {result.success}")
if not validation_results.success:
    marketing_channel_logger.error("Great Expectations validation failed.")
    raise Exception(f"Pipeline halted. Data validation failed for workflow run: {workflow_run_id}")


marketing_channel_logger.info(f"Validation successful! Inserting data to S3 Gold layer: {output_path}")

marketing_channel_logger.info(f"Total rows in marketing funnel data before adding to S3: {total_rows} \n")
# 5. Adding data to S3 and catalog
marketing_channel_logger.info("5. Writing data to S3")
gold_marketing_channel_df.createOrReplaceTempView('source_market_channel')

spark.sql('CREATE DATABASE IF NOT EXISTS glue_iceberg.gold_db')

spark.sql("""
    CREATE TABLE IF NOT EXISTS glue_iceberg.gold_db.marketing_channel(
        marketing_channel_key string,
        event_date date,
        country string,
        traffic_source string,
        total_users bigint,
        total_sessions bigint,
        total_events bigint,
        product_viewers bigint,
        cart_users bigint,
        checkout_users bigint,
        purchasers bigint,
        purchase_conversion_rate double,
        events_per_session double
    )
USING iceberg
""")

spark.sql("""
    MERGE INTO glue_iceberg.gold_db.marketing_channel AS target
    USING source_market_channel AS source
    ON target.event_date = source.event_date
        AND target.country = source.country
        AND target.traffic_source = source.traffic_source

    WHEN MATCHED THEN 
        UPDATE SET
            target.marketing_channel_key = source.marketing_channel_key,
            target.total_users = source.total_users,
            target.total_sessions = source.total_sessions,
            target.total_events = source.total_events,
            target.product_viewers = source.product_viewers,
            target.cart_users = source.cart_users,
            target.checkout_users = source.checkout_users,
            target.purchasers = source.purchasers,
            target.purchase_conversion_rate= source.purchase_conversion_rate,
            target.events_per_session= source.events_per_session

    WHEN NOT MATCHED THEN
        INSERT(
            marketing_channel_key,
            event_date,
            country,
            traffic_source,
            total_users,
            total_sessions,
            total_events,
            product_viewers,
            cart_users,
            checkout_users,
            purchasers,
            purchase_conversion_rate,
            events_per_session
        )
        VALUES(
            source.marketing_channel_key,
            source.event_date,
            source.country,
            source.traffic_source,
            source.total_users,
            source.total_sessions,
            source.total_events,
            source.product_viewers,
            source.cart_users,
            source.checkout_users,
            source.purchasers,
            source.purchase_conversion_rate,
            source.events_per_session
        )
""")
marketing_channel_logger.info("Data added to S3")
job.commit()




