from io import BytesIO
import zipfile
import boto3
s3 = boto3.client('s3')
sr3 = boto3.resource('s3')


class zip:
    def __init__(self):
        pass

    @staticmethod
    def get_zip_file(source_bucket, file_name):
        zip_obj = sr3.Object(bucket_name=source_bucket, key=file_name)
        buffer = BytesIO(zip_obj.get()["Body"].read())
        z = zipfile.ZipFile(buffer)
        return z