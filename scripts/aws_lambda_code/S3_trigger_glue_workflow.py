import json
import boto3
import logging
import urllib.parse

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue_client = boto3.client('glue')


def lambda_handler(event, context):
    logger.info(f"Received direct S3 payload: {json.dumps(event)}")

    try:
        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        raw_key = record['s3']['object']['key']

        key = urllib.parse.unquote_plus(raw_key)
        s3_input_path = f"s3://{bucket}/{key}"
        logger.info(f"Resolved s3 input path: {s3_input_path}")

        logger.info(f"Starting Glue Workflow run..")
        response = glue_client.start_workflow_run(
            Name='User_events_workflow',
            RunProperties={
                'S3_INPUT_PATH': s3_input_path
            }
        )

        workflow_run_id = response.get('RunId')
        logger.info(f"Workflow successfully triggered. Run ID: {workflow_run_id}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': "Workflow triggered successfully",
                'WorkflowRunId': workflow_run_id
        })
        }

    except KeyError as ke:
        logger.error(f"Payload parsing error. Make sure S3 is triggering this directly. Missing key: {str(ke)}")
        raise ke
    except Exception as e:
        logger.error(f"Failed to orchestrate Glue Workflow: {str(e)}")
        raise e