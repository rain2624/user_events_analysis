# Which products are getting viewed, added to carts, and reaching checkout?          
# grain - day, product_id and country.

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
    ["JOB_NAME", "WORKFLOW_NAME", "WORKFLOW_RUN_ID", "S3_OUTPUT_PATH"]
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
product_performance_logger = logging.getLogger(__name__)
product_performance_logger.info("Starting with creating Product Performance table.")
product_performance_logger.info("Each row in this table defines data for each day, country and product_id wise")

workflow_name = args["WORKFLOW_NAME"] 
workflow_run_id = args["WORKFLOW_RUN_ID"]
output_path = args["S3_OUTPUT_PATH"]

# Handshake 
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
    product_performance_logger.error("PROCESSED_DATES was not found")
    raise KeyError("PROCESSED_DATES is missing from the workflow context.")

product_performance_logger.info(f"Handshake Successful! Resolved input path from Silver: {input_path}")

# 1. Read data
product_performance_logger.info("1. Started with reading data from silver/transformed layer")

formatted_dates_str = ", ".join([f"'{d.strip()}'" for d in processed_dates_str.split(',')]) 
user_interactions_df = spark.sql(f"""Select * from {input_path} where event_date in ({formatted_dates_str})""")

total_rows = user_interactions_df.count()
product_performance_logger.info(f"Total rows in user interactions data: {total_rows}")

# 2. Creating business metrics table
product_performance_logger.info("2. Creating Product Performance data")
user_interactions_df = user_interactions_df.filter(col('product_id').isNotNull())
gold_product_performance_df = user_interactions_df.groupBy("event_date", "country", "product_id") \
                                            .agg(
                                                countDistinct(when(col('event_type') == 'product_view', col('user_id'))).alias('unique_product_viewers'),
                                                count(when(col('event_type') == 'product_view', col('user_id'))).alias('total_product_views'),
                                                countDistinct(when(col('event_type') == 'add_to_cart', col('user_id'))).alias('unique_cart_users'),
                                                countDistinct(when(col('event_type') == 'checkout', col('user_id'))).alias('unique_checkout_users'),
                                                countDistinct(when(col('event_type') == 'purchase', col('user_id'))).alias('unique_purchasers'))


gold_product_performance_df = gold_product_performance_df.withColumn('purchase_rate', round(coalesce(col('unique_purchasers')*100/col('unique_product_viewers'), lit(0.0)),2)) \
                                                        .withColumn("avg_views_per_viewer",round(coalesce(col("total_product_views")/col("unique_product_viewers"),lit(0.0)),2))
# 3. Adding surrogate key
product_performance_logger.info("3. Adding surrogate keys")
gold_product_performance_df = gold_product_performance_df.withColumn('product_performance_key', sha2(
                                                        concat_ws(
                                                            "|",
                                                            col('event_date').cast("string"),
                                                            col('product_id'),
                                                            col("country")
                                                        ),
                                                        256
                                                    ))




# 3. Arranging cols 
gold_product_performance_df = gold_product_performance_df.select(
 'product_performance_key',
 'event_date',
 'country',
 'product_id',
 'unique_product_viewers',
 'total_product_views',
 'unique_cart_users',
 'unique_checkout_users',
 'unique_purchasers',
 'purchase_rate',
 'avg_views_per_viewer')

total_rows = gold_product_performance_df.count()
product_performance_logger.info(f"Created product performance data with {total_rows} rows.")

# Performing data validation 
product_performance_logger.info("4. Starting with data validation")
product_performance_logger.info("""We are looking into the following validations with the help of great expectations:
- Not Null Values
    1. event_date
    2. product_id 
    3. country
""")


df_source_name = 'product_performance'
df = gold_product_performance_df
layer = 'gold'
expectation_list = [
gx.expectations.ExpectColumnValuesToNotBeNull(column="event_date"),
gx.expectations.ExpectColumnValuesToNotBeNull(column="product_id"),
gx.expectations.ExpectColumnValuesToNotBeNull(column="country")]

validation_results = great_expectations_validations.data_validation(df_source_name, df, expectation_list, layer)

for result in validation_results.results:
    product_performance_logger.info(f"Expectation: {result.expectation_config.type}| ")
    product_performance_logger.info(f"Parameters: {result.expectation_config.kwargs}| ")
    product_performance_logger.info(f"Success: {result.success}")
if not validation_results.success:
    product_performance_logger.error("Great Expectations validation failed.")
    raise Exception(f"Pipeline halted. Data validation failed for workflow run: {workflow_run_id}")


product_performance_logger.info(f"Validation successful! Inserting data to S3 Gold layer: {output_path}")

total_rows = gold_product_performance_df.count()
product_performance_logger.info(f"Total rows in final data : {total_rows}")

# Writing data to S3
product_performance_logger.info("5. Writing data to S3")
gold_product_performance_df.createOrReplaceTempView("source_prod_perf")

spark.sql("CREATE DATABASE IF NOT EXISTS glue_iceberg.gold_db")

spark.sql(
    """CREATE TABLE IF NOT EXISTS glue_iceberg.gold_db.product_performance (
        product_performance_key string,
        event_date date,
        country string,
        product_id string,
        unique_product_viewers bigint,
        total_product_views bigint,
        unique_cart_users bigint,
        unique_checkout_users bigint,
        unique_purchasers bigint,
        purchase_rate double,
        avg_views_per_viewer double 
    )
USING iceberg
"""
)

spark.sql("""
    MERGE INTO glue_iceberg.gold_db.product_performance AS target
    USING source_prod_perf AS source
    ON target.event_date = source.event_date
        AND target.country = source.country
        AND target.product_id = source.product_id

    WHEN MATCHED THEN 
        UPDATE SET
            target.product_performance_key = source.product_performance_key,
            target.unique_product_viewers = source.unique_product_viewers,
            target.total_product_views = source.total_product_views,
            target.unique_cart_users = source.unique_cart_users,
            target.unique_checkout_users = source.unique_checkout_users,
            target.unique_purchasers = source.unique_purchasers,
            target.purchase_rate = source.purchase_rate,
            target.avg_views_per_viewer = source.avg_views_per_viewer

    WHEN NOT MATCHED THEN
        INSERT(
            product_performance_key,
            event_date,
            country,
            product_id,
            unique_product_viewers,
            total_product_views,
            unique_cart_users,
            unique_checkout_users,
            unique_purchasers,
            purchase_rate,
            avg_views_per_viewer
        )
        VALUES(
            source.product_performance_key,
            source.event_date,
            source.country,
            source.product_id,
            source.unique_product_viewers,
            source.total_product_views,
            source.unique_cart_users,
            source.unique_checkout_users,
            source.unique_purchasers,
            source.purchase_rate,
            source.avg_views_per_viewer
        )
""")

product_performance_logger.info("Data added to S3.")
job.commit()



