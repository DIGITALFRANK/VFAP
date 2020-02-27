import json
import boto3
from utils.get_zip_file import zip
s3 = boto3.client('s3')
sr3 = boto3.resource('s3')


class rf_hitwise:
    """class with default constructor to perform parsing of Hitwise
        
    """

    @staticmethod
    def parser(source_bucket, file_name, destination_bucket):
        try:
            #Getting the object of zipped file on s3
            z = zip.get_zip_file(source_bucket, file_name)
            file_part = file_name.split('/')
            destination = '/'.join(file_part[0:2]) + '/'
            #iterating through the zip files and uploading the files to s3
            for filename in z.namelist():
                if filename.find('custom-category_clickstream-websites_daily_week-ending') != -1:
                    file_part = filename.split("_")
                    file_part.insert(2, 'HITWISE')
                    date = file_part[len(file_part)-1].rsplit('-', 1)
                    file = "_".join(file_part[0:len(file_part)-1])+"_"
                    sr3.meta.client.upload_fileobj(z.open(filename), Bucket=destination_bucket,
                                                   Key=destination + file + date[0] + '_' + date[1])
                elif filename.find('industries_website-ranking_daily_week-ending') != -1:
                    file_part = filename.split("_")
                    file_part.insert(2, 'HITWISE')
                    date = file_part[len(file_part) - 1].rsplit('-', 1)
                    file = "_".join(file_part[0:len(file_part) - 1]) + "_"
                    sr3.meta.client.upload_fileobj(z.open(filename), Bucket=destination_bucket,
                                                   Key=destination + file + date[0] + '_' + date[1])
                else:
                    file_part = filename.split("_")
                    file_part.insert(2, 'HITWISE')
                    file = "_".join(file_part[0:len(file_part)])
                    sr3.meta.client.upload_fileobj(z.open(filename), Bucket=destination_bucket,
                                                   Key=destination + file)
        except Exception as error:
            print(error)