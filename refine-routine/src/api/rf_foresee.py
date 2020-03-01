import json
import boto3
from utils.get_zip_file import zip
s3 = boto3.client('s3')
sr3 = boto3.resource('s3')


class rf_foresee:
    """class with default constructor to perform parsing of foresee
        
    """

    @staticmethod
    def parser(source_bucket, file_name, destination_bucket):
        try:
            file_part = file_name.split('/')
            destination = '/'.join(file_part[0:2])+'/'
            #replacing the '+' in file name with spaces
            new_file = file_name.replace('+', ' ').replace('%28', '(').replace('%29', ')')
            #calling the zip module to get object of zipped file 
            z = zip.get_zip_file(source_bucket, new_file)
            x = file_name.rsplit('-', 2)
            y = x[1]+'-'+x[2]
            date = y.strip('.zip')
            #iterating through the zip file and uploading the individual file to s3
            for filename in z.namelist():
                if filename.find('Data-Vans Store') != -1:
                    sr3.meta.client.upload_fileobj(z.open(filename), Bucket=destination_bucket, Key=destination+f'CustomExtract_Data_FORESEE_VANS_Store_VAN_'+date+'.txt')
                elif filename.find('Data-VANS Mobile') != -1:
                    sr3.meta.client.upload_fileobj(z.open(filename), Bucket=destination_bucket, Key=destination+f'CustomExtract_Data_FORESEE_VANS_Mobile_VAN_'+date+'.txt')
                elif filename.find('Data-VANS Browse') != -1:
                    sr3.meta.client.upload_fileobj(z.open(filename), Bucket=destination_bucket, Key=destination+f'CustomExtract_Data_FORESEE_VANS_Browse_VAN_'+date+'.txt')
        except Exception as error:
            print(error)