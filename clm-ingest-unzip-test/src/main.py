
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
    
    print(event)
    return

    s3 = boto3.resource('s3')
    try:
        source_bucket = event.get('Records')[0].get('s3').get('bucket').get('name')
        environment = source_bucket.split('-')[2]
        from_source = source_bucket.split('-')[3]
        obj = event.get('Records')[0].get('s3').get('object')
        # ignore folders/directories (should not be present)
        if not int(obj.get('size', 0)):
            return
        filename = obj.get('key')
        file_extension = pathlib.Path(filename).suffix
    except:
        print('Lambda triggered unexpectedly')
        return
    copy_source = {
        'Bucket': source_bucket,
        'Key': filename
    }


    # copy from stage to raw
    s3.meta.client.copy(copy_source, f'vf-datalake-{environment}-raw', filename)
    
    # unzip if compressed and copy from raw to refined
    if file_extension in [".zip"]: #[".zip", '.tar', '.tar.gz', '.tz', '.rar', '.7z']:
        s3 = boto3.client('s3')
        destination_bucket = f'vf-datalake-{environment}-refined'
        try:
            obj = s3.get_object(Bucket=copy_source['Bucket'], Key=copy_source['Key'])
            with io.BytesIO(obj["Body"].read()) as tf:
                # rewind the file
                tf.seek(0)
                # Read the file as a zipfile and process the members
                with zipfile.ZipFile(tf, mode='r') as zipf:
                    for file in zipf.infolist():
                        file_name = file.filename
                        s3.put_object(Bucket=destination_bucket, Key='current/' + copy_source['Key'] + '/' + file_name, Body=zipf.read(file))
            # delete MACOSX hidden files created during unzipping
            s3_res = boto3.resource('s3')
            bucket = s3_res.Bucket(destination_bucket)
            bucket.objects.filter(Prefix='current/' + copy_source['Key'] + '/' + '__MACOSX').delete()
            print('UNZIPPED COMPRESSED FILE')
        except Exception as e:
            print(e)
            print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(copy_source['Key'], copy_source['Bucket']))
            raise e
        return # exit if file was zipped
    
    # regular file copy from raw to refined
    filename = f'current/{filename}'
    s3.meta.client.copy(copy_source, f'vf-datalake-{environment}-refined', filename)



