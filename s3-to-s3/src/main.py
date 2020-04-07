# Lambda: s3-to-s3-copy
import json
import boto3
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.WARNING)

# Read lambda environment variables
prod_env=os.environ['prod_env']
nonprod_env=os.environ['nonprod_env']
topic_name=os.environment['topic_arn']

# Non-Production bucket names
nonprod_stage_bucket=f'vf-datalake-{nonprod_env}-stage'
nonprod_raw_bucket=f'vf-datalake-{nonprod_env}-raw'

# Production bucket names
prod_stage_bucket=f'vf-datalake-{prod_env}-stage'
prod_raw_bucket=f'vf-datalake-{prod_env}-raw'

def send_sns(region, account_id, message):
	sns_client = boto3.client('sns')
	sns_client.publish(
		TopicArn=f'arn:aws:sns:{region}:{account_id}:{topic_name}',
		Message=message,
		Subject='S3-to-S3 copy failed'
		)
		
def copy_object(src_bucket,src_key,dest_bucket):
    s3 = boto3.resource('s3')
    s3_client = boto3.client('s3')
    copy_source = {
        'Bucket': src_bucket,
        'Key': src_key
    }
    s3.meta.client.copy(copy_source, dest_bucket, src_key)
    response = s3_client.put_object_acl(ACL='bucket-owner-full-control',Bucket=dest_bucket, Key=src_key)
    

def lambda_handler(event, context):
	try:
		ACCOUNT_ID = context.invoked_function_arn.split(":")[4]
		REGION = context.invoked_function_arn.split(":")[3]
		
		for i in event['Records']:
			src_bucket = i['s3']['bucket']['name']
			src_key = i['s3']['object']['key']
			
			print('S3-to-S3 copy lambda triggered by bucket: {}'.format(src_bucket))
			print('S3-to-S3 copy lambda triggered by key:'.format(src_key))
			
			# If S3-to-S3 copy lambda triggered by production stage bucket
			# 		Copy the file(s) to production raw bucket with same src_key
			#		Copy the file(s) to non-prod stage bucket with same src_key
			if src_bucket == prod_stage_bucket:
				print('S3 COPY: {} >>>> {}'.format(src_bucket,prod_raw_bucket))
				copy_object(src_bucket,src_key,prod_raw_bucket)
			
			# If S3-to-S3 copy lambda triggered by production raw bucket
			# 		Copy the file(s) to non-prod raw bucket with same src_key
			elif src_bucket == prod_raw_bucket:
				print('S3 COPY: {} >>>> {}'.format(src_bucket,nonprod_raw_bucket))
				copy_object(src_bucket,src_key,nonprod_raw_bucket)
			
			# If S3-to-S3 copy lambda triggered by non-prod stage bucket
			# 		Copy the file(s) to non-prod raw bucket with same src_key
			elif src_bucket == nonprod_stage_bucket:
				print('S3 COPY: {} >>>> {}'.format(src_bucket,nonprod_raw_bucket))
				copy_object(src_bucket,src_key,nonprod_raw_bucket)
			else:
				print('Lambda is not triggered by any of specified buckets')
	except Exception as error:
		logger.exception(error)
		response = {
			'status': 500,
			'AccountID': ACCOUNT_ID,
			'error': {
				'type': type(error).__name__,
				'description': str(error)
			}
		}
		send_sns(REGION, ACCOUNT_ID, str(response))
		raise