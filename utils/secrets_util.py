import boto3
import json

class SecretsUtil:
    def __init__(self):
        self.conn = boto3.client('secretsmanager')

    def get_secret(self, secret_id, key_name):
        # print(secret_id, key_name)
        resp = self.conn.get_secret_value(SecretId=secret_id)
        # print(resp)
        return json.loads(resp.get('SecretString', {})).get(key_name, '')
        
    def get_all_secrets(self, secret_id):
        resp = self.conn.get_secret_value(SecretId=secret_id)
        secret = json.loads(resp.get('SecretString', {}))
        return secret
        