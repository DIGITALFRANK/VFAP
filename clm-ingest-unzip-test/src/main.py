import io
import os
import sys
import boto3
import pathlib
import zipfile
import tarfile


def lambda_handler(event, context):
    """
    CC/CLM migration ingest code from stage to raw, raw to refine.
    Handles [".zip"] compressed files
    """

    # activate/deactive Lambda by uncommenting the following `return` statement
    print(event)
    # return

    # connect to S3
    s3 = boto3.resource('s3')

    try:
        # get source bucket from event
        source_bucket = event.get('Records')[0].get('s3').get('bucket').get('name')
        # get environemnt from source bucket
        environment = source_bucket.split('-')[2]
        # get the S3 object
        obj = event.get('Records')[0].get('s3').get('object')
        # ignore folders/directories (object should be a single file)
        if not int(obj.get('size', 0)):
            return
        # get the full key path
        key = obj.get('key')
        # separate the data source from key
        data_source = key.split('/')[3]
        # get the file name
        file_name = key[15 + len(data_source):]
        # get the file extension
        file_extension = pathlib.Path(file_name).suffix
        # put copy source in S3/Boto3-friendly format
        copy_source = {
            'Bucket': source_bucket,
            'Key': key
        }
        # prepare raw bucket with corret environement
        raw_bucket = f'vf-datalake-{environment}-raw'
        # prepare refined bucket with corret environement
        refined_bucket = f'vf-datalake-{environment}-refined'
    except Exception as e:
        print('Lambda triggered unexpectedly')
        print(e)
        raise e
        # return

    # copy any file from stage to raw
    s3.meta.client.copy(
        copy_source,
        Bucket=raw_bucket,
        Key=f'frank_test/{file_name}'
    )

    # if zip file, unzip and send uncompressed files to refined/current
    if file_extension in [".zip"]:  # [".zip", '.tar', '.tar.gz', '.tz', '.rar', '.7z']:

        try:
            zip_obj = s3.Object(bucket_name=copy_source['Bucket'], key=copy_source['Key'])
            buffer = io.BytesIO(zip_obj.get()["Body"].read())
            z = zipfile.ZipFile(buffer)
            for file_name in z.namelist():
                file_info = z.getinfo(file_name)
                s3.meta.client.upload_fileobj(
                    z.open(file_name),
                    Bucket=refined_bucket,
                    Key=f'frank_test/{file_name}'  # change this to f'current/{filename}'
                )


        # try:
        #     # get S3 object details
        #     bucket = s3.Bucket(copy_source['Bucket'])
        #     obj = bucket.Object(copy_source['Key'])
        #     # create an in-memory bytes IO buffer
        #     with io.BytesIO() as b:
        #         # read the file into it
        #         obj.download_fileobj(b)
        #         # rewind the file pointer to the beginning
        #         b.seek(0)
        #         # read file as zipfile, process members and send to destination
        #         with zipfile.ZipFile(b, mode='r') as zipf:
        #             for file in zipf.infolist():
        #                 file_name = file.filename
        #                 print(file_name)
        #                 print('**************')
        #                 object = s3.Object(destination_bucket, 'current/' + file_name)
        #                 object.put(Body=zipf.read(file))
        #
        #     # set up refined_bucket for access
        #     refined_bucket = s3.Bucket(destination_bucket)
        #
        #     # get name of temp unzip folder (fil_name - .zip extension)
        #     zip_folder = copy_source['Key'][18:]
        #     zip_folder = zip_folder[:-4]
        #
        #     print(refined_bucket)
        #     print(zip_folder)
        #     print(len(zip_folder))
        #
        #     # eliminate temp unzip folder (move files directly to refined/current)
        #     for key in refined_bucket.objects.filter(Prefix='current/' + zip_folder):
        #         print(key)
        #         print(key.bucket_name)
        #         print(key.key)
        #         # put files into S3/Boto3-friendly format
        #         copy_unzipped = {
        #             'Bucket': key.bucket_name,
        #             'Key': key.key
        #         }
        #         new_key = 'current/nora/' + copy_unzipped['Key'][len('current/' + zip_folder):]
        #         print(new_key)
        #
        #         if new_key == 'current/nora//':
        #             continue
        #
        #         # copy back to refined/current
        #         # refined_bucket.copy(copy_unzipped, 'current' + copy_unzipped['Key'][len('current/' + zip_folder):])
        #         s3.meta.client.copy(copy_unzipped, f'vf-datalake-{environment}-refined', new_key)
        #
        #     # remove both temp unzip folder & MACOSX env folder from refined/current
        #     refined_bucket.objects.filter(Prefix='current/' + zip_folder).delete()
        #     refined_bucket.objects.filter(Prefix='current/' + '__MACOSX').delete()
        #     print('UNZIPPED COMPRESSED FILE')


        except Exception as e:
            print(e)
            print('Error getting object {} from bucket {}. Make sure they exist and your bucket \
            is in the same region as this function.'.format(copy_source['Key'], copy_source['Bucket']))
            raise e
        return  # exit if file was zipped

    # copy all other regular files from raw to refined/current
    s3.meta.client.copy(
        copy_source,
        Bucket=refined_bucket,
        Key=f'frank_test/{file_name}'  # change this to f'current/{filename}'
    )


