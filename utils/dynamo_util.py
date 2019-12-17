import boto3

class DynamoUtil:
    def __init__(self):
        self.dynamo_db = boto3.resource("dynamodb")
        self.table_connection = None

    def connect_to_table(self, table_name):
        self.table_connection = self.dynamo_db.Table(table_name)
        return self.table_connection


