# This table analyzes the customer journey through the website/app.         
# grain - day and country.


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


# Apache iceberg config
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
marketing_funnel_logger = logging.getLogger(__name__)
marketing_funnel_logger.info("Starting with creating Marketing funnel table.")
marketing_funnel_logger.info("Each row in this table represents unique data and country wise records.")

workflow_name=args['WORKFLOW_NAME']
workflow_run_id=args['WORKFLOW_RUN_ID']
output_path=args['S3_OUTPUT_PATH']

# handshake to get table path from silver layer
glue_client = boto3.client('glue')
response = glue_client.get_workflow_run_properties(
    Name=workflow_name,
    RunId=workflow_run_id
)
# input_path=run_props['RunProperties']['SILVER_DATA_PATH']
# processed_dates_str=run_props['RunProperties']['PROCESSED_DATES']
run_props = response.get('RunProperties', {})
input_path = run_props.get('SILVER_DATA_PATH')
processed_dates_str = run_props.get('PROCESSED_DATES')

if not processed_dates_str:
    marketing_funnel_logger.error("PROCESSED_DATES was not found")
    raise KeyError("PROCESSED_DATES is missing from the workflow context.")

formatted_dates_str = ", ".join([f"'{d.strip()}'" for d in processed_dates_str.split(',')])

marketing_funnel_logger.info(f"Handshake successful! Resolved input path from silver layer: {input_path}")

# 1. Read data
marketing_funnel_logger.info("1. Started with reading data from silver/transformed layer")

user_interactions_df = spark.sql(f"Select * from {input_path} where event_date in ({formatted_dates_str})")


total_rows = user_interactions_df.count()
marketing_funnel_logger.info(f"Total rows in user interactions data: {total_rows}")


# 2. Creating business metrics table
marketing_funnel_logger.info("2. Creating Marketing Funnel Data.")
gold_marketing_funnel_df = user_interactions_df.groupBy("event_date", "country") \
                                            .agg(
                                                countDistinct(col("user_id")).alias("visitors"),
                                                countDistinct(when(col('event_type') == 'product_view', col('user_id')).otherwise(None)).alias('product_viewers'),
                                                countDistinct(when(col('event_type') == 'add_to_cart', col('user_id')).otherwise(None)).alias('cart_users'),
                                                countDistinct(when(col('event_type') == 'checkout', col('user_id')).otherwise(None)).alias('checkout_users'),
                                                countDistinct(when(col('event_type') == 'purchase', col('user_id')).otherwise(None)).alias('purchasers')
                                            )



gold_marketing_funnel_df = gold_marketing_funnel_df.withColumn('view_to_cart_rate', when(col('product_viewers') >0, round(col('cart_users')*100/col('product_viewers'),2)).otherwise(0.0)) \
                                                    .withColumn('cart_to_checkout_rate', when(col('cart_users')>0, round(col('checkout_users')*100/col('cart_users'),2)).otherwise(0.0)) \
                                                    .withColumn('view_to_purchase_rate', when(col('product_viewers')>0, round(col('purchasers')*100/col('product_viewers'),2)).otherwise(0.0)) \
                                                    .withColumn('checkout_to_purchase_rate', when(col('checkout_users')>0, round(col('purchasers')*100/col('checkout_users'),2)).otherwise(0.0)) 


# 3. Adding surrogate key
marketing_funnel_logger.info('3. Adding Surrogate key')
gold_marketing_funnel_df = gold_marketing_funnel_df.withColumn('marketing_funnel_key', sha2(
                                                        concat_ws(
                                                            "|",
                                                            col('event_date').cast("string"),
                                                            col("country")
                                                        ),
                                                        256
                                                    ))



# arranging cols 
gold_marketing_funnel_df = gold_marketing_funnel_df.select(
 'marketing_funnel_key', 
 'event_date',
 'country',
 'visitors',
 'product_viewers',
 'cart_users',
 'checkout_users',
 'purchasers',
 'view_to_cart_rate',
 'cart_to_checkout_rate',
 'view_to_purchase_rate',
 'checkout_to_purchase_rate')

total_rows = gold_marketing_funnel_df.count()
marketing_funnel_logger.info(f"Created Marketing funnel data with {total_rows} rows\n")

# 4. Validating data
marketing_funnel_logger.info("4. Starting with data validation\n")
marketing_funnel_logger.info("""We are looking into the following validations with the help of great expectations:
- Not Null Values
    1. event_date
    2. country
- Between 0 and 100
    1. view_to_cart_rate
    2. cart_to_checkout_rate
    3. view_to_purchase_rate
    4. checkout_to_purchase_rate
""")


df_source_name = 'marketing_funnel'
df = gold_marketing_funnel_df
layer = 'gold'
expectation_list = [
gx.expectations.ExpectColumnValuesToNotBeNull(column="event_date"),
gx.expectations.ExpectColumnValuesToNotBeNull(column="country"),
gx.expectations.ExpectColumnValuesToBeBetween(column="view_to_cart_rate", min_value=0, max_value=100),
gx.expectations.ExpectColumnValuesToBeBetween(column="cart_to_checkout_rate", min_value=0, max_value=100),
gx.expectations.ExpectColumnValuesToBeBetween(column="view_to_purchase_rate", min_value=0, max_value=100),
gx.expectations.ExpectColumnValuesToBeBetween(column="checkout_to_purchase_rate", min_value=0, max_value=100)
]

validation_results = great_expectations_validations.data_validation(df_source_name, df, expectation_list, layer)

for result in validation_results.results:
    marketing_funnel_logger.info(f"Expectation: {result.expectation_config.type}| ")
    marketing_funnel_logger.info(f"Parameters: {result.expectation_config.kwargs}| ")
    marketing_funnel_logger.info(f"Success: {result.success}")
if not validation_results.success:
    marketing_funnel_logger.error("Great Expectations validation failed.")
    raise Exception(f"Pipeline halted. Data validation failed for workflow run: {workflow_run_id}")


marketing_funnel_logger.info(f"Validation successful! Inserting data to S3 Gold layer: {output_path}")

total_rows = gold_marketing_funnel_df.count()
marketing_funnel_logger.info(f"Total rows in marketing funnel data before adding to S3: {total_rows} \n")
# Writing data to S3
marketing_funnel_logger.info("5. Writing data to S3")
gold_marketing_funnel_df.createOrReplaceTempView('source_market_funnel')

spark.sql('CREATE DATABASE IF NOT EXISTS glue_iceberg.gold_db')

spark.sql("""
    CREATE TABLE IF NOT EXISTS glue_iceberg.gold_db.marketing_funnel(
        marketing_funnel_key string, 
        event_date date,
        country string,
        visitors bigint,
        product_viewers bigint,
        cart_users bigint,
        checkout_users bigint,
        purchasers bigint,
        view_to_cart_rate double,
        cart_to_checkout_rate double,
        view_to_purchase_rate double,
        checkout_to_purchase_rate double
    )
USING iceberg
""")

spark.sql("""
    MERGE INTO glue_iceberg.gold_db.marketing_funnel AS target
    USING source_market_funnel AS source
    ON target.event_date = source.event_date
        AND target.country = source.country

    WHEN MATCHED THEN 
        UPDATE SET
            target.marketing_funnel_key = source.marketing_funnel_key, 
            target.visitors = source.visitors,
            target.product_viewers = source.product_viewers,
            target.cart_users = source.cart_users,
            target.checkout_users = source.checkout_users,
            target.purchasers = source.purchasers,
            target.view_to_cart_rate = source.view_to_cart_rate,
            target.cart_to_checkout_rate = source.cart_to_checkout_rate,
            target.view_to_purchase_rate = source.view_to_purchase_rate,
            target.checkout_to_purchase_rate  = source.checkout_to_purchase_rate

    WHEN NOT MATCHED THEN
        INSERT(
            marketing_funnel_key, 
            event_date,
            country,
            visitors,
            product_viewers,
            cart_users,
            checkout_users,
            purchasers,
            view_to_cart_rate,
            cart_to_checkout_rate,
            view_to_purchase_rate,
            checkout_to_purchase_rate
        )
        VALUES(
            source.marketing_funnel_key, 
            source.event_date,
            source.country,
            source.visitors,
            source.product_viewers,
            source.cart_users,
            source.checkout_users,
            source.purchasers,
            source.view_to_cart_rate,
            source.cart_to_checkout_rate,
            source.view_to_purchase_rate,
            source.checkout_to_purchase_rate
        )
""")
marketing_funnel_logger.info("Data added to S3")
job.commit()