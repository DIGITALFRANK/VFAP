import semantic_version
import boto3

# M - Major
# m - Minor
# p - patch
sm_client = boto3.client('secretsmanager')
response = client.get_secret_value(SecretId='versioning')
print(response['SecretString'])

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
