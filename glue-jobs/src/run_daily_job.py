import json
import boto3
import tempfile
import configparser
import os
from awsglue.utils import getResolvedOptions
import sys
from boto3.dynamodb.conditions import Key, Attr
import ast

client = boto3.client("glue", region_name="us-east-1")
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")


class ConfigFromS3(object):
    def __init__(self, bucket_name, key, region_name):
        """Read configuration file from S3 and parse it"""
        defaults = {"aws_region": region_name}
        session = boto3.session.Session(region_name=region_name)
        self.bucket = session.resource("s3").Bucket(bucket_name)
        temporary_file = tempfile.NamedTemporaryFile()
        self.bucket.download_file(key, temporary_file.name)
        self.config = configparser.ConfigParser(defaults=defaults)
        with open(temporary_file.name, "r") as f:
            self.config.readfp(f)
        temporary_file.close()


def get_list_of_s3_objects(bucket_name, folder):
    client = boto3.client("s3")
    response = client.list_objects(Bucket=bucket_name, Delimiter="/", Prefix=folder)
    return response


def get_file_name(item):
    file_name = item.split("/")
    return file_name[-1]


def get_param_store_configs(param_name):
    try:
        client = boto3.client("ssm", region_name="us-east-1")
        response = client.get_parameter(Name=param_name)
        params = json.loads(response["Parameter"]["Value"])
    except Exception as error:
        print(
            "Error occured while retrieving parameters from param store.Check parameter key name ".format(
                error
            )
        )
        params = None
    return params


def get_parameter_store_key(config_file):
    file_path = config_file
    bucket_name = file_path.split("/")[2]
    key = file_path.split("s3://" + bucket_name + "/")[1]
    # print("Key is defined as {}".format(key))
    # print("bucket name is defined as {}".format(bucket_name))
    obj = ConfigFromS3(bucket_name, key, "us-east-1")
    return obj.config["PARAMSTORE"]["StoreKey"]


def start_glue_job(
    glue_job_name, glue_modules, file_name, config_file, machine_type, num_of_workers
):
    client = boto3.client("glue", region_name="us-east-1")
    try:
        print("Please wait..........")
        response = client.start_job_run(
            JobName=glue_job_name,
            Arguments={
                "--extra-py-files": glue_modules,
                "--FILE_NAME": file_name,
                "--extra-files": config_file,
            },
            Timeout=600,
            NotificationProperty={"NotifyDelayAfter": 20},
            WorkerType=machine_type,
            NumberOfWorkers=num_of_workers,
        )
    except Exception as error:
        print("Error triggering glue job due to {}".format(error))
    return response


def get_target_table(table_name, file_name):
    try:
        table = dynamodb.Table(table_name)
        file_parts = file_name.split("_")
        partition_key = "_".join(file_parts[0:-1])
        if not partition_key.endswith("_CNTL"):
            # print(partition_key)
            response = table.get_item(Key={"feed_name": partition_key})
            item = response["Item"]
            target_table = item["tgt_dstn_tbl_name"]
        else:
            target_table = None
    except Exception as error:
        target_table = None
        print("Error occurred due to {}".format(error))
    return target_table


def get_list_of_files(table_name, target_table):
    file_list = []
    try:
        table = dynamodb.Table(table_name)
        response = table.scan(
            FilterExpression=Attr("tgt_dstn_tbl_name").eq(target_table)
        )
        items = response["Items"]
        if len(items) > 0:
            for configs in items:
                file_list.append(configs["feed_name"])
        # print(len(response))
    except Exception as error:
        file_list = []
        print("Error occured due to {}".format(error))
    return file_list


def lambda_handler(config_file, glue_modules, glue_job_name):
    # TODO implement
    # glue_job_name = "cc_test_experian"
    all_target_tables = []
    status_table = {}
    table_to_file_mapper = {}
    key = get_parameter_store_key(config_file)
    params = get_param_store_configs(key)
    s3_objs = get_list_of_s3_objects(
        bucket_name=params["refined_bucket"], folder="current/"
    )
    count = 0
    # print(params["config_table"])
    for item in s3_objs["Contents"]:
        if item["Key"].endswith((".csv", ".txt")):
            file_name = get_file_name(item["Key"])
            try:
                # if file_name.upper().__contains__("_CNTL"):
                if "CNTL" in file_name.upper():
                    continue
                else:
                    # print(file_name)
                    #     response = start_glue_job(
                    #     glue_job_name=glue_job_name,
                    #     glue_modules=glue_modules,
                    #     file_name=str(file_name),
                    #     config_file=config_file,
                    #     machine_type="G.1X",
                    #     num_of_workers=5,
                    # )
                    # count=count+1
                    # break
                    target_table = get_target_table(params["config_table"], file_name)
                if target_table is not None and target_table not in all_target_tables:
                    # file_list = get_list_of_files(params["config_table"], target_table)
                    # all_target_tables[target_table]=file_name
                    all_target_tables.append(target_table)
                elif target_table is not None and target_table in all_target_tables:
                    pass
                    # all_target_tables[target_table].append9
                    # if target_table in table_to_file_mapper:
                    #     continue
                    # else:
                    #     table_to_file_mapper[target_table] = file_list

            except Exception as error:
                print("Failed : ", file_name)
                status_table[file_name] = None

        else:
            continue
    for tbl in all_target_tables:
        table_to_file_mapper[tbl] = []
    # print(table_to_file_mapper)
    for item in s3_objs["Contents"]:
        if item["Key"].endswith((".csv", ".txt")):
            file_name = get_file_name(item["Key"])
            target_table = get_target_table(params["config_table"], file_name)
            if target_table is not None and target_table in all_target_tables:
                table_to_file_mapper[target_table].append(file_name)

                # for part_key in table_to_file_mapper[target_table]:
                #     if file_name.startswith(part_key):
                #         temp_list = table_to_file_mapper[target_table]
                #         temp_list[temp_list.index(part_key)] = file_name
                #         table_to_file_mapper[target_table] = temp_list

    print(table_to_file_mapper)
    # Sanitise the dictionary:
    # for key, value in table_to_file_mapper.items():
    #     for file_nm in value:
    #         if not file_nm.endswith(("csv", "txt")):
    #             value.pop(value.index(file_nm))

    # Trigger Glue job
    for key in table_to_file_mapper.keys():
        try:
            file_name = table_to_file_mapper[key]
            if len(file_name) > 0:
                response = start_glue_job(
                    glue_job_name=glue_job_name,
                    glue_modules=glue_modules,
                    file_name=file_name[0],
                    config_file=config_file,
                    machine_type="G.1X",
                    num_of_workers=5,
                )
                print(response)
        except Exception as error:
            print("Exception due to {}".format(error))
            break
    print(table_to_file_mapper)


if __name__ == "__main__":
    args = getResolvedOptions(
        sys.argv, ["CONFIG_FILE", "GLUE_MODULES", "DAILY_JOB_NAME"]
    )
    config_file = args["CONFIG_FILE"]
    glue_modules = args["GLUE_MODULES"]
    glue_job_name = args["DAILY_JOB_NAME"]
    lambda_handler(config_file, glue_modules, glue_job_name)

