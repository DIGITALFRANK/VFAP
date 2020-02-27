import boto3
resource = boto3.resource('s3')

class S3Client:
    def __init__(self):
        self.resource = boto3.resource('s3')
        self.conn = boto3.client('s3')

    def move_from_tmp_to_bucket(self, source, target_folder, target_bucket):
        resource.Bucket(target_bucket).upload_file(source, target_folder)

    @staticmethod
    def read_file_from_s3(source_bucket, file_name):
        content_object = resource.Object(source_bucket, file_name)
        body = content_object.get()['Body'].read().decode(encoding="utf-8", errors="ignore")
        return body

    def read_from_s3(self, source_bucket, file_name):
        """This function reads data from s3 using spark.read method in Pyspark

        Arguments:
            bucket {[String]} -- Name of the bucket in S3
            path {String} -- s3 object key
        Returns:
            [spark.dataframe] -- Returns a spark dataframe
        """
        try:
            s3_obj = "s3://{}/{}".format(source_bucket, file_name)
            #logger.info("Reading file : {} from s3...".format(s3_obj))
            print(s3_obj)
            content_object = resource.Object(source_bucket, file_name)
            body = content_object.get()['Body'].read().decode(encoding="utf-8", errors="ignore")
            # df = (
            #     spark.read.format("csv")
            #         .option("header", "true")
            #         .option("inferSchema", "true")
            #         .option("delimiter", delimiter)
            #         .load(s3_obj)
            # )
            print("Dataframe read successfully")
            #logger.info("Schema : ")
            #df.printSchema()
            #logger.info("s3_ obj : {} has records : {}".format(
                #s3_obj, df.count()))

        except Exception as error:
            df = None
            print("Could not read dataframe ", error)
        #self.df = df
        return body

    def copy(self, source_object, target_object):
        bucket = target_object.get('bucket') or target_object.get('Bucket')
        key = target_object.get('key') or target_object.get('Key')
        self.resource.meta.client.copy(source_object, bucket, key)

    def copy_to_bucket(self, bucket_from_name, bucket_to_name,
                       file_name):
        copy_source = {
            'Bucket': bucket_from_name,
            'Key': file_name
        }
        self.resource.Object(bucket_to_name, file_name).copy(copy_source)

    def move(self, source_object, target_object):
        self.copy(source_object, target_object)
        self.resource.Object(source_object.get('Bucket'), source_object.get('Key')).delete()


