import boto3


class SecretsUtil:
    def __init__(self):
        self.conn = boto3.client('secretsmanager')

    def get_secret(self, secret_id, key_name):
        resp = self.conn.get_secret_value(secret_id)
        return json.loads(resp.get('SecretString', {}).get(key_name, ''))

