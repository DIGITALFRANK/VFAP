import logging
import traceback
import time
import itertools
import sys
import datetime
import configparser
import json
import tempfile
import boto3
from awsglue.utils import getResolvedOptions


def get_env_params(bucket_name, key, region_name):
    """
    Parameters:
    bucket_name: str - S3 bucket containing config file
    s3_key: str - s3 key for config file
    region_name: str - AWS region

    Returns:

    params: Dict[str, str]

    This function reads a configuration file from S3 and parses it.
    Then taking a key from this configuration file, we can access
    AWS Systems Manager parameter store
    """
    # Download .ini config file from S3 to local temp URL
    defaults = {"aws_region": region_name}
    session = boto3.session.Session(region_name=region_name)
    bucket = session.resource("s3").Bucket(bucket_name)
    temporary_file = tempfile.NamedTemporaryFile()
    bucket.download_file(key, temporary_file.name)
    # Load local .ini file and parse
    config = configparser.ConfigParser(defaults=defaults)
    with open(temporary_file.name, "r") as f:
        config.readfp(f)
    temporary_file.close()
    # Get parameter store key from ConfigParser object
    key = config["PARAMSTORE"]["StoreKey"]
    # Get parameter store key-value pairs from AWS SSM
    client = boto3.client("ssm", region_name="us-east-1")
    response = client.get_parameter(Name=key)
    return json.loads(response["Parameter"]["Value"])


class Constants:

    # Hard-coded values
    FINAL_FEED_NAMES = [
        "F_TNF_ADDRESS",
        "F_VANS_ADDRESS",
        "F_TNF_TRANS_DETAIL",
        "F_VANS_TRANS_DETAIL",
        "F_TNF_TRANS_HEADER",
        "F_VANS_TRANS_HEADER",
    ]
    AWS_REGION = "us-east-1"
    LOCAL_LOG_URL = "./{0}.txt".format(str(datetime.datetime.utcnow()))

    # Get glue job key-value pairs
    job_args = getResolvedOptions(
        sys.argv,
        [
            "GLUE_DAILY_JOB_NAME",
            "S3_LOG_DESTINATION_FOLDER",
            "S3_LOG_TOP_FOLDER_NAME",
            "S3_FEED_FILE_SOURCE_FOLDER",
            "DDB_FEED_NAME_ATTRIBUTE",
            "DDB_TARGET_TABLE_ATTRIBUTE",
            "ITERATION_PAUSE",
            "S3_CONFIG_BUCKET",
            "S3_CONFIG_FILE_PATH",
        ],
    )
    # Get env parameters from AWS systems manager
    env_params = get_env_params(
        bucket_name=job_args["S3_CONFIG_BUCKET"],
        key=job_args["S3_CONFIG_FILE_PATH"],
        region_name=AWS_REGION,
    )

    # Configured via Param store
    DDB_JOB_METADATA_TABLE = env_params["config_table"]
    S3_LOG_DESTINATION_BUCKET = env_params["log_bucket"]
    S3_FEED_FILE_SOURCE_BUCKET = env_params["refined_bucket"]

    # Configured via Glue job
    GLUE_DAILY_JOB_NAME = job_args["GLUE_DAILY_JOB_NAME"]
    ITERATION_PAUSE = int(job_args["ITERATION_PAUSE"])
    S3_LOG_DESTINATION_FOLDER = job_args["S3_LOG_DESTINATION_FOLDER"]
    S3_LOG_TOP_FOLDER_NAME = job_args["S3_LOG_TOP_FOLDER_NAME"]
    S3_FEED_FILE_SOURCE_FOLDER = job_args["S3_FEED_FILE_SOURCE_FOLDER"]
    DDB_FEED_NAME_ATTRIBUTE = job_args["DDB_FEED_NAME_ATTRIBUTE"]
    DDB_TARGET_TABLE_ATTRIBUTE = job_args["DDB_TARGET_TABLE_ATTRIBUTE"]


log = logging.getLogger()
# Set root logging level
log.setLevel(logging.INFO)
# Set log message formatter
formatter = logging.Formatter("%(levelname)s: %(asctime)s - %(message)s")
# Set stream handler
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)
# Set file handler
file_handler = logging.FileHandler(Constants.LOCAL_LOG_URL)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
# Attach handlers to log
log.addHandler(stream_handler)
log.addHandler(file_handler)


def load_log_to_s3(
    log=log,
    local_url=Constants.LOCAL_LOG_URL,
    s3_bucket=Constants.S3_LOG_DESTINATION_BUCKET,
    s3_root=Constants.S3_LOG_DESTINATION_FOLDER,
    s3_top_folder=Constants.S3_LOG_TOP_FOLDER_NAME,
):
    """
    This function loads the log from a local URL to a configured location in S3
    """
    s3_client = boto3.client("s3")
    now = datetime.datetime.now()
    datetime_extension = (
        str(now.year)
        + "%02d" % now.month
        + "%02d" % now.day
        # + "%02d" % now.hour
        # + "%02d" % now.minute
        # + "%02d" % now.second
    )
    s3_object_url = (
        s3_root
        + datetime_extension
        + "/"
        + s3_top_folder
        + "/"
        + "daily_clm_queues_"
        + datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        + ".log"
    )
    log.info("Loading log file to s3://{0}/{1}".format(s3_bucket, s3_object_url))
    s3_client.upload_file(local_url, s3_bucket, s3_object_url)
    return s3_object_url


class OrchestrationException(Exception):
    def __init__(self, message):
        """
        This exception class loads the log to a stable location in S3
        before exiting S3
        """
        load_log_to_s3()
        super().__init__(message)


class AWSResourceLimitException(Exception):
    def __init__(self, message):
        super().__init__(message)


def trigger_glue_job(job_name, file_name, log):
    """
    This function triggers an AWS Glue job, passing a file_name parameter to it
    """
    log.info("Getting Boto3 Glue client")
    print("Getting Glue client")
    # TODO: constant required for region  name
    client = boto3.client("glue", region_name="us-east-1")
    log.info("Successfully created Boto3 Glue client")
    print("Successfully created Boto3 Glue client")
    log.info("Trigger job - {0} with file name - {1}".format(job_name, file_name))
    try:
        response = client.start_job_run(
            JobName=job_name, Arguments={"--FILE_NAME": file_name}, Timeout=2880
        )
        log.info(
            "Job successfully triggered. Job run ID - {0}".format(response["JobRunId"])
        )
        return response["JobRunId"]
    except client.exceptions.ResourceNumberLimitExceededException:
        log.error(
            "AWS Glue resource limit reached while trying to start job {0}".format(
                job_name
            ),
            exc_info=True,
        )
        raise AWSResourceLimitException(
            message="Could not start AWS Glue Job {0}".format(job_name)
        )


def get_glue_job_run_status(job_name, job_run_id, log):
    """
    This function gets the status of a running AWS Glue job
    """
    log.info("Getting Boto3 Glue client")
    # TODO: constant required for region name
    client = boto3.client("glue", region_name="us-east-1")
    log.info("Successfully created Boto3 Glue client")
    print("Checking status of job - {0} with run id - {1}".format(job_name, job_run_id))
    log.info(
        "Checking status of job - {0} with run id - {1}".format(job_name, job_run_id)
    )
    response = client.get_job_run(JobName=job_name, RunId=job_run_id)
    log.info("Job run status - {0}".format(response["JobRun"]["JobRunState"]))
    print("Job run status - {0}".format(response["JobRun"]["JobRunState"]))
    return response["JobRun"]["JobRunState"]


class Job:
    """
    A Job is a model of an AWS Glue job status composed of a status and a name

    The modelled status of the job must be mapped from the set of possible DynamoDB
    status' to a subset of relevant states
    """

    # Range of possible job status states
    READY_STATUS = "ready"
    PROCESSING_STATUS = "processing"
    COMPLETE_STATUS = "complete"

    # Groupings of DynamoDB job status states to relevant states
    GLUE_JOB_PROCESSING_RESPONSES = ["STARTING", "RUNNING", "STOPPING"]
    GLUE_JOB_COMPLETE_RESPONSES = ["STOPPED", "SUCCEEDED", "FAILED", "TIMEOUT"]

    def __init__(self, job_name, file_name):
        """
        Parameters:

        file_name: str
        """
        # TODO: Configure job name
        self.file_name = file_name
        self.status = self.READY_STATUS
        self.job_name = job_name

    def run(self):
        self.job_run_id = trigger_glue_job(
            job_name=self.job_name, file_name=self.file_name, log=log
        )
        self.job_run_id = trigger_glue_job(
            job_name=self.job_name, file_name=self.file_name, log=log
        )
        try:
            self.job_run_id = trigger_glue_job(
                job_name=self.job_name, file_name=self.file_name, log=log
            )
            self.status = self.PROCESSING_STATUS
        except AWSResourceLimitException:
            self.status = self.READY_STATUS
        return

    def check_glue_status(self):
        """
        This function checks the status of an AWS Glue job which
        was most previously run from this object
        """
        ddb_status = get_glue_job_run_status(
            job_name=self.job_name, job_run_id=self.job_run_id, log=log
        )
        if ddb_status in Job.GLUE_JOB_COMPLETE_RESPONSES:
            self.status = self.COMPLETE_STATUS
        return self.status


class Job_Queue:
    """
    The Job Queue is composed of a list of Job objects
    The Job Queue two key states depending on the position of the head:

        * Non-Empty
        * Empty
    """

    NON_EMPTY_STATUS = 1
    EMPTY_STATUS = 0

    def __init__(self, job_list):
        """
        Parameters:

        job_list: List[Job]
        """
        self.queue = job_list
        self.queue_len = len(job_list)
        self.head = 0
        self.status = Job_Queue.NON_EMPTY_STATUS

    def check_queue(self, log=log):
        if self.status is Job_Queue.NON_EMPTY_STATUS:
            log.info([job.file_name for job in self.queue[self.head:]])
            self.check_job(job=self.queue[self.head])

        return self.status

    def check_job(self, job):
        if job.status == Job.READY_STATUS:
            job.run()
        if job.status == Job.PROCESSING_STATUS:
            latest_job_status = job.check_glue_status()
            if latest_job_status == Job.COMPLETE_STATUS:
                log.info("Job complete - {0}".format(self.queue[self.head].file_name))
                self.head += 1
                if self.head == self.queue_len:
                    self.status = Job_Queue.EMPTY_STATUS


class Array_Job_Queue:
    """
    Array Job Queues are composed out of a collection of Job Queues.
    The Array Job Queue has two possible states which depends on the aggregated
    states of the composite queues.

        * Non-Empty
        * Empty

    The presence of a single non-empty Job Queue will result in the Array Job Queue
    to be non-empty.
    """

    NON_EMPTY_STATUS = 1
    EMPTY_STATUS = 0
    ACTIVE_QUEUE_COUNT_THRESHOLD = 0
    ARRAY_QUEUE_COMPLETION = 1
    ITERATION_INTERVAL = 0.5

    def __init__(self, job_queue_list, iteration_pause):
        """
        Parameters:

        job_queue_list: List[Job_Queue]
        iteration_pause: int
        """
        self.parallel_queues = job_queue_list
        self.iteration_pause = iteration_pause
        self.status = Array_Job_Queue.NON_EMPTY_STATUS

    def check_queues(self):
        """
        This function iterates through each queue and checks the queue status.
        If a single queue is non-empty, the array of job queues is considered non-empty.
        If all queues are empty, the array of job queues is considered empty.
        """
        active_queue_count = [
            job_queue.check_queue() for job_queue in self.parallel_queues
        ].count(Job_Queue.NON_EMPTY_STATUS)
        print("Active queue count - {0}".format(active_queue_count))
        if active_queue_count == Array_Job_Queue.ACTIVE_QUEUE_COUNT_THRESHOLD:
            self.status = Array_Job_Queue.EMPTY_STATUS
        else:
            self.status = Array_Job_Queue.NON_EMPTY_STATUS
        return self.status

    def execute_queues(self):
        completion_status = Array_Job_Queue.NON_EMPTY_STATUS
        while completion_status == Array_Job_Queue.NON_EMPTY_STATUS:
            completion_status = self.check_queues()
            time.sleep(self.iteration_pause)
        return Array_Job_Queue.ARRAY_QUEUE_COMPLETION


def test_running_completed():
    job_run_id = trigger_glue_job(
        job_name="test_trigger_dummy_1", file_name="hello", log=log
    )
    for index in range(20):
        time.sleep(5)
        get_glue_job_run_status(
            job_name="test_trigger_dummy_1", job_run_id=job_run_id, log=log
        )


# test_running_completed()
def test_failed():
    job_run_id = trigger_glue_job(
        job_name="test_trigger_dummy_2", file_name="hello", log=log
    )
    for index in range(20):
        time.sleep(5)
        get_glue_job_run_status(
            job_name="test_trigger_dummy_2", job_run_id=job_run_id, log=log
        )


# test_failed()


def test_job_queue_1():
    jq = Job_Queue(job_list=[Job(job_name="test_trigger_dummy_1", file_name="hello")])
    for index in range(5):
        print(jq.check_queue())
        time.sleep(20)


# test_job_queue_1()


def test_job_queue_2():
    jq = Job_Queue(
        job_list=[
            Job(job_name="test_trigger_dummy_1", file_name="hello"),
            Job(job_name="test_trigger_dummy_2", file_name="hello"),
        ]
    )
    for index in range(10):
        print(jq.check_queue())
        time.sleep(20)


# test_job_queue_2()


def test_array_job_queue():
    test_array_jq = Array_Job_Queue(
        job_queue_list=[
            Job_Queue(
                [
                    Job(job_name="test_trigger_dummy_1", file_name="hello"),
                    Job(job_name="test_trigger_dummy_2", file_name="hello"),
                ]
            ),
            Job_Queue(
                [
                    Job(job_name="test_trigger_dummy_2", file_name="hello"),
                    Job(job_name="test_trigger_dummy_1", file_name="hello"),
                ]
            ),
        ],
        iteration_pause=10,
    )
    test_array_jq.execute_queues()


# test_array_job_queue()


def test_job_class_1():
    test_job = Job(job_name="test_trigger_dummy_1", file_name="hello")
    test_job.run()
    for index in range(3):
        print(test_job.check_glue_status())
        time.sleep(15)


# test_job_class_1()


def test_job_class_2():
    test_job = Job(job_name="test_trigger_dummy_2", file_name="hello")
    test_job.run()
    for index in range(3):
        print(test_job.check_glue_status())
        time.sleep(20)


# test_job_class_2()


def test_array_job_queue_2():
    test_array_jq = Array_Job_Queue(
        job_queue_list=[
            Job_Queue(
                [
                    Job(
                        job_name="Test_pipeline_anchal",
                        file_name="I_TNF_Responsys_CONVERT_20200210135657.csv",
                    ),
                    Job(
                        job_name="Test_pipeline_anchal",
                        file_name="I_TNF_Responsys_CONVERT_20200213135653.csv ",
                    ),
                ]
            ),
            Job_Queue(
                [
                    Job(
                        job_name="Test_pipeline_anchal",
                        file_name="I_TNF_Responsys_CONVERT_20200211135706.csv",
                    ),
                    Job(
                        job_name="Test_pipeline_anchal",
                        file_name="I_TNF_Responsys_CONVERT_20200215135649.csv",
                    ),
                ]
            ),
        ],
        iteration_pause=10,
    )
    test_array_jq.execute_queues()


# test_array_job_queue_2()


def get_list_of_feed_files_in_s3(bucket_name, folder):
    client = boto3.client("s3")
    response = client.list_objects(Bucket=bucket_name, Delimiter="/", Prefix=folder)
    qualified_bucket_objects = map(lambda x: x["Key"], response["Contents"])
    # remove directory prefix from filenames
    unqualified_bucket_objects = map(
        lambda x: x.split("/")[-1], qualified_bucket_objects
    )
    # remove empty-file directory reference from collection
    unqualified_bucket_files = filter(lambda x: x != "", unqualified_bucket_objects)
    # remove control files from collection
    feed_files = list(filter(lambda x: "_CNTL_" not in x, unqualified_bucket_files))
    return feed_files


# s3_objects = get_list_of_feed_files_in_s3(bucket_name = "vf-datalake-dev-refined",
#                                          folder = "current/")
# print(s3_objects)


def get_feeds_and_destinations(
    table_name, ddb_region, feed_name_attribute, target_table_attribute
):
    dynamodb = boto3.resource("dynamodb", region_name=ddb_region)
    table = dynamodb.Table(table_name)
    response = table.scan(
        AttributesToGet=[feed_name_attribute, target_table_attribute],
        Select="SPECIFIC_ATTRIBUTES",
    )["Items"]
    return response

    table_name: str - name of DynamoDB table to get data from
    ddb_region: str - region of DynamoDB table to get data from
    feed_name_attribute: str - attribute in DynamoDB table which corresponds to the feed name associated with the job
    target_table_attribute: str - attribute in DynamoDB table which corresponds to the target table associated with the job
    log: logging.Logger

    Returns:

    response - List[Dict[str, str]] - contains the feed names and destination table names for each job in the DynamoDB table

    This function reads the feed name and target table metadata associated with ETL jobs from user specified attributes
    from a user specified DynamoDB table. Each dict in the returned list corresponds to job metadata for a specific
    ETL job.
    """
    log.info("Connecting with DynamoDB...")
    try:
        dynamodb = boto3.resource("dynamodb", region_name=ddb_region)
        table = dynamodb.Table(table_name)
    except BaseException:
        error_msg = "Failed to connect/retrieve table object for DynamoDB table {0} - please check whether table exists or permission is enabled".format(
            table_name
        )
        log.error(error_msg, exc_info=True)
        raise Exception(error_msg)
    try:
        log.info(
            "Scanning DynamoDB table - {0} from region - {1} for attributes - {2}, {3}".format(
                table_name, ddb_region, feed_name_attribute, target_table_attribute
            )
        )
        response = table.scan(
            AttributesToGet=[feed_name_attribute, target_table_attribute],
            Select="SPECIFIC_ATTRIBUTES",
        )["Items"]
        log.info(
            "Captured data from DynamoDB table {0} - {1}".format(table_name, response)
        )
        return response
    except BaseException:
        error_msg = "Failed to pull attributes {0}, {1} from DynamoDB table - {2}".format(
            feed_name_attribute, target_table_attribute, table_name
        )
        log.error(error_msg)
        log.error(traceback.format_exc())
        raise Exception(error_msg)

    table_name: str - name of DynamoDB table to get data from
    ddb_region: str - region of DynamoDB table to get data from
    feed_name_attribute: str - attribute in DynamoDB table which corresponds to the feed name associated with the job
    target_table_attribute: str - attribute in DynamoDB table which corresponds to the target table associated with the job
    log: logging.Logger

def split_feeds_destinations_by_first_and_final_jobs(
    feed_destination_pairs, feed_name_attribute, final_feed_names, log
):
    """
    Parameters:

    feed_destination_pairs: List[Dict[str,str]]
    feed_name_attribute: str
    final_feed_names: List[str]
    log: logging.Logger

    Returns:

    Tuple[List[Dict[str,str]], List[Dict[str,str]]]

    This function takes the entire list of feed destination pairs in DynamoDB and
    splits this collection into feed destination pairs for all jobs which should be processed
    together first and feed destination pairs which should be processed once all of the
    jobs in the first collection have been completed. It uses the final_feeds list of feed
    names to determine which pairs belong in the latter group.
    """
    first_feed_destination_pairs = []
    final_feed_destination_pairs = []
    for feed_destination_pair in feed_destination_pairs:
        if feed_destination_pair[feed_name_attribute] in final_feed_names:
            final_feed_destination_pairs.append(feed_destination_pair)
        else:
            first_feed_destination_pairs.append(feed_destination_pair)
    return first_feed_destination_pairs, final_feed_destination_pairs


def trim_feed_file_name(feed_file_name):
    """
    Parameters:

    feed_file_name: str - underscore delimited feed file name including timestamp and file extension

    Returns:

    str - trimmed file name

    This function trims a feed file name in S3 by removing the timestamp, file extension
    and trailing underscore from the file name.

    eg

    my_file_012345.csv -> my_file.csv
    """
    return "_".join(feed_file_name.split("_")[:-1])


def map_feed_files_to_destination_tables(
    feed_files, feeds_destinations, feed_name_attribute, target_table_attribute
):
    """
    Parameters:

    feed_files: List[str] - list of feed files in S3
    feeds_destinations: List[Dict[str, str]] - list of dictionaries containing feed name
                                               and target destination table name, each with
                                               {"feed_name": ..., "tgt_dstn_tbl_name": ....}
    feed_name_attribute: str - feed name attribute in feeds destinations dicts
    target_table_attribute: str - target table attribute in feeds destinations dicts
    log: logging.Logger

    Returns:

    files_to_destinations: List[Tuple[str, str]] - list of tuples containing file names
                                                   and their corresponding target destination table
                                                   names - [(feed_file_name, target_destination_table_name), ...]

    This function left joins the trimmed file names with the relation of [feed names, destination names] on
    the feed names attribute. The join occurs if the trimmed file name matches a feed name OR the trimmed file
    name plus an additional underscore matches a feed name (feed name occasionally contains trailing underscores)
    """
    files_to_feeds = list(
        map(
            lambda x: (
                x,
                list(
                    filter(
                        lambda y: y[feed_name_attribute] == trim_feed_file_name(x)
                        or y[feed_name_attribute] == trim_feed_file_name(x) + "_",
                        feeds_destinations,
                    )
                ),
            ),
            feed_files,
        )
    )
    file_destination_pairs = list(
        map(lambda x: (x[0], x[1][0][target_table_attribute]), files_to_feeds)
    )
    return file_destination_pairs


# file_destination_pairs = map_feed_files_to_destination_tables(feed_files = get_list_of_feed_files_in_s3(bucket_name = "vf-datalake-dev-refined", folder = "current/"),
#                                                              feeds_destinations = get_feeds_and_destinations(table_name = "vf-dev-etl-file-broker",
#                                                                                                              ddb_region = "us-east-1",
#                                                                                                              feed_name_attribute = "feed_name",
#                                                                                                              target_table_attribute = "tgt_dstn_tbl_name"),
#                                                              feed_name_attribute = "feed_name",
#                                                              target_table_attribute = "tgt_dstn_tbl_name")

# print(file_destination_pairs)


def group_file_names_by_destination_table(file_destination_pairs):
    """
    Parameters:

    file_destination_pairs: List[Tuple[str, str]] - list of tuples containing file names
                                                    and their corresponding target destination table
                                                    names - [(feed_file_name, target_destination_table_name), ...]
    log: logging.Logger

    Returns:

    List[List[str]] - list of lists of file names which have been grouped together according
                      to their target destination tables
    """
    job_groups = []
    for key, group in itertools.groupby(file_destination_pairs, key=lambda x: x[1]):
        job_groups.append([element[0] for element in group])
    return job_groups


# group_file_names_by_destination_table(file_destination_pairs = file_destination_pairs)
# print(job_groups)


def run_job_queues(glue_job, job_groups, iteration_pause):
    """
    Parameters:

    glue_job: str - Generic AWS Glue job used to run every ETL job
    job_groups: List[List[str]] - List of lists of jobs grouped by common destination tables
    iteration_pause: int - Number of seconds paused at the end of each iteration over the job queues
    log: logging.Logger

    Returns:

    None

    This function executes one or more queues of AWS Glue jobs in parallel using a commong
    AWS Glue job template with a variable file_name parameter for each job. User specifies
    pause length after each iteration of queue updates.
    """
    array_jq = Array_Job_Queue(
        job_queue_list=[
            Job_Queue(
                [Job(job_name=glue_job, file_name=s3_file) for s3_file in job_group]
            )
            for job_group in job_groups
        ],
        iteration_pause=iteration_pause,
    )
    array_jq.execute_queues()


# run_job_queues(glue_job = "Test_pipeline_anchal",
#               job_groups = job_groups,
#               iteration_pause = 30)
        first_feeds_destination_pairs, final_feeds_destination_pairs = split_feeds_destinations_by_first_and_final_jobs(
            feed_destination_pairs=feeds_destinations,
            feed_name_attribute=Constants.DDB_FEED_NAME_ATTRIBUTE,
            final_feed_names=Constants.FINAL_FEED_NAMES,
            log=log,
        )
        for feeds_destinations in [
            first_feeds_destination_pairs,
            final_feeds_destination_pairs,
        ]:
            file_destination_pairs = map_feed_files_to_destination_tables(
                feed_files=s3_objects,
                feeds_destinations=feeds_destinations,
                feed_name_attribute=Constants.DDB_FEED_NAME_ATTRIBUTE,
                target_table_attribute=Constants.DDB_TARGET_TABLE_ATTRIBUTE,
                log=log,
            )
            job_groups = group_file_names_by_destination_table(
                file_destination_pairs=file_destination_pairs, log=log
            )

            ordered_job_groups = [
                order_jobs(job_list=job_group, log=log) for job_group in job_groups
            ]

            log.info("Ordered job groups - {0}".format(ordered_job_groups))

            run_job_queues(
                glue_job=Constants.GLUE_DAILY_JOB_NAME,
                job_groups=ordered_job_groups,
                iteration_pause=Constants.ITERATION_PAUSE,
                log=log,
            )
            log.info("Parallel queue of jobs executed successfully")
        load_log_to_s3()

    except BaseException as e:
        raise OrchestrationException(str(e))
