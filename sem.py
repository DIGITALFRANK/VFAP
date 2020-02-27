import semantic_version
import boto3
import json

# M - Major
# m - Minor
# p - patch
sm_client = boto3.client('secretsmanager')
response = sm_client.get_secret_value(SecretId='versioning')
i  = json.loads(response['SecretString'])
version = i['version']
print(version)

version = semantic_version.Version('0.1.1')
increment = "M"
if increment == "M":
	new_version = version.next_major()
	print(str(new_version))
elif increment == "m":
	new_version = version.next_minor()
	print(str(new_version))
elif increment == "p":
	new_version = version.next_patch()
	print(str(new_version))
