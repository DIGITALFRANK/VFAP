import semantic_version
import boto3
import json

# M - Major
# m - Minor
# p - patch

sm_client = boto3.client('secretsmanager')

def update_version(new_version):
	secretString="{\"version\": \"" + str(new_version) + "\"}"
	response = sm_client.update_secret(
		SecretId='versioning',
		SecretString=secretString
	)

response = sm_client.get_secret_value(SecretId='versioning')
i  = json.loads(response['SecretString'])
version = i['version']
print(version)

version = semantic_version.Version(version)
increment = "m"
if increment == "M":
	new_version = version.next_major()
	update_version(new_version)
	print(str(new_version))
elif increment == "m":
	new_version = version.next_minor()
	update_version(new_version)
	print(str(new_version))
elif increment == "p":
	new_version = version.next_patch()
	update_version(new_version)
	print(str(new_version))

with open('main.json', 'r') as file:
	json_data = json.load(file)
	json_data['Parameters']['Version']['Default']=str(new_version)
with open('main.json', 'w') as file:
	json.dump(json_data, file, indent=4)

