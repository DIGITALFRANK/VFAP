import boto3


class S3Client:
    def __init__(self):
        self.resource = boto3.resource('s3')
        self.conn = boto3.client('s3')

    def copy(self, source_object, target_object):
        bucket = target_object.get('bucket') or target_object.get('Bucket')
        key = target_object.get('key') or target_object.get('Key')
        self.resource.meta.client.copy(source_object, bucket, key)

    def move(self, source_object, target_object):
        self.copy(source_object, target_object)
        self.resource.Object(source_object.get('Bucket'), source_object.get('Key')).delete()

