import logging
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark import SQLContext
from datetime import datetime

# Set up Glue Spark Session
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


# Todo file_path param if file isn't already loaded into Dataframe, otherwise pass in df as param, remove spark.read
def apply_effective_dating(file_path):
    """
    This method applies effective dating (5 columns) to a raw/refine files
    It takes in a filepath or dataframe parameter
    It returns the modified Spark DataFrame object

    :param file_path:
    :return: None
    """

    # get this Glue job's id >>> (or run transform Glue job prior and get that job's ID)
    job_id = "find the Glue job's ID"
    # get the file's delimiter
    delimiter = "the special charcter delimeter or whatever we convert it to"
    # save the file name -- if format is "s3://bucket/path" -- account for additional directories
    original_file_name = file_path.split("/")[1]

    # load file into Spark dataframe
    df = (
        spark.read.format("txt")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", delimiter)
            .load(file_path)
    )

    # ToDo: 'df.withColumn()' adds a new column to each row with the specified value >> confrim this is the right approach
    # ToDo: may have to do some type-casting and/or lit() (literals) for Spark
    # Set the effective date status to current
    df = df.withColumn("effective_date_status", "Y")
    # Set the effective date
    df = df.withColumn("effective_date", datetime.today())
    # set the last job id to this job >>> (or the transform job that just preceeded??)
    df = df.withColumn("last_job_id", job_id)
    # set the created time stamp ToDo: what's the difference here with "effective date" column other than timestamp vs date
    df = df.withColumn("create_time_stamp", datetime.utcnow())
    # save original (extract?) file name
    df = df.withColumn("original_file_name", original_file_name)

    # return the dataframe for further processign or writes to redshift
    return df

