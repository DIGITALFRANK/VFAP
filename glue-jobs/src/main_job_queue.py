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
    FAILFAST_QUEUE_FEEDS = ["I_TNF_WeatherTrends_vf_historical_data"]
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
    client = boto3.client("glue", region_name=Constants.AWS_REGION)
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
    client = boto3.client("glue", region_name=Constants.AWS_REGION)
    log.info(
        "Checking status of job - {0} with run id - {1}".format(job_name, job_run_id)
    )
    response = client.get_job_run(JobName=job_name, RunId=job_run_id)
    log.info("Job run status - {0}".format(response["JobRun"]["JobRunState"]))
    return response["JobRun"]["JobRunState"]


def trim_feed_file_name(feed_file_name):
    """
    Parameters:

    feed_file_name: str - underscore delimited feed file name including timestamp and file extension

    Returns:

    str - trimmed file name

    This function trims a feed file name in S3 by removing the timestamp, file extension
    and trailing underscore from the file name.

    eg

    my_file_012345.csv -> my_file
    """
    return "_".join(feed_file_name.split("_")[:-1])


class Job:
    """
    A Job is a model of an AWS Glue job which allows the object to run
    and subsequently monitor Glue jobs.
    """

    # Range of possible job status states
    READY_STATUS = "ready"
    PROCESSING_STATUS = "processing"
    COMPLETE_STATUS = "complete"
    SUCCESS_STATUS = "success"
    FAILED_STATUS = "failed"

    # Groupings of DynamoDB job status states to relevant states
    GLUE_JOB_PROCESSING_RESPONSES = ["STARTING", "RUNNING", "STOPPING"]
    GLUE_JOB_COMPLETE_RESPONSES = ["STOPPED", "SUCCEEDED", "FAILED", "TIMEOUT"]
    GLUE_JOB_FAILED_RESPONSES = ["FAILED", "TIMEOUT", "STOPPED"]
    GLUE_JOB_SUCCESS_RESPONSES = ["SUCCEEDED"]

    # Detailed reporting flag for checking job status
    PERMISSIVE_REPORT = 0
    FAILFAST_REPORT = 1

    def __init__(self, job_name, file_name):
        """
        Parameters:

        job_name: str - the name of the AWS Glue job
        file_name: str - the file name of the feed file which has arrived
        """
        self.file_name = file_name
        self.status = self.READY_STATUS
        self.job_name = job_name

    def run(self, log):
        """
        This function runs an AWS Glue job using the objects
        job and file name arguments
        """
        log.info(
            "Triggering AWS Glue job {0} with file name parameter {1}".format(
                self.job_name, self.file_name
            )
        )
        try:
            self.job_run_id = trigger_glue_job(
                job_name=self.job_name, file_name=self.file_name, log=log
            )
            self.status = self.PROCESSING_STATUS
        except AWSResourceLimitException:
            self.status = self.READY_STATUS
        return

    def check_glue_status(self, reporting_flag):
        """
        This function checks the status of an AWS Glue job. The set
        of return values is determined by whether success/failure
        is a required detail.
        """
        ddb_status = get_glue_job_run_status(
            job_name=self.job_name, job_run_id=self.job_run_id, log=log
        )
        if reporting_flag == Job.PERMISSIVE_REPORT:
            if ddb_status in Job.GLUE_JOB_COMPLETE_RESPONSES:
                self.status = self.COMPLETE_STATUS
            return self.status
        elif reporting_flag == Job.FAILFAST_REPORT:
            if ddb_status in Job.GLUE_JOB_FAILED_RESPONSES:
                self.status = self.FAILED_STATUS
            elif ddb_status in Job.GLUE_JOB_SUCCESS_RESPONSES:
                self.status = self.SUCCESS_STATUS
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
    PERMISSIVE = 0
    FAILFAST = 1

    def __init__(self, job_list):
        """
        Parameters:

        job_list: List[Job]
        """
        self.queue = job_list
        self.queue_len = len(job_list)
        self.head = 0
        self.status = Job_Queue.NON_EMPTY_STATUS
        self.mode = self.determine_queue_mode(
            job_list=job_list, failfast_jobs=Constants.FAILFAST_QUEUE_FEEDS
        )

    def determine_queue_mode(self, job_list, failfast_jobs):
        """
        Parameters:

        job_list: List[Job]

        This function is responsible for determining the execution
        mode of the Queue. The following two modes control the behaviour
        of the Queue in the event where a job fails:
        
        FAILFAST - abandon all pending jobs as a result of a single failed job
        PERMISSIVE - move to the next job in the queue and continue

        The mode is determined by whether the file_name attached
        to any of the Jobs matches any of the patterns in the
        list of jobs which require failfast behaviour
        """
        file_names = list(map(lambda x: x.file_name, job_list))
        # Queues are permissive by default
        mode = Job_Queue.PERMISSIVE
        if (
            len(
                list(
                    filter(
                        lambda file_name: trim_feed_file_name(file_name)
                        in Constants.FAILFAST_QUEUE_FEEDS,
                        file_names,
                    )
                )
            )
            > 0
        ):
            mode = Job_Queue.FAILFAST
        return mode

    def check_queue(self, log):
        """
        Parameters:

        log: logging.Logger

        Returns:

        status: int - the status of the queue is either empty or non-empty

        This function checks to see whether the queue is empty or non-empty by checking
        the status of the job at the head
        """
        if self.status is Job_Queue.NON_EMPTY_STATUS:
            log.info(
                "Queue executing job {0} of {1} - {2}".format(
                    self.head + 1, self.queue_len, self.queue[self.head].file_name
                )
            )
            self.check_job(job=self.queue[self.head], log=log)

        return self.status

    def check_job(self, job, log):
        """
        Parameters:

        job: Job - Job object represents AWS Glue job
        log: logging.Logger

        Returns:

        None

        This function checks the status of the job and either runs the job, passes or
        dequeues the job depending on the reported status of the job.
        """
        if job.status == Job.READY_STATUS:
            job.run(log)
        if job.status == Job.PROCESSING_STATUS:
            if self.mode == Job_Queue.PERMISSIVE:
                latest_job_status = job.check_glue_status(
                    reporting_flag=Job.PERMISSIVE_REPORT
                )
                if latest_job_status == Job.COMPLETE_STATUS:
                    log.info(
                        "Job complete - {0}".format(self.queue[self.head].file_name)
                    )
                    self.head += 1
                    if self.head == self.queue_len:
                        self.status = Job_Queue.EMPTY_STATUS
            elif self.mode == Job_Queue.FAILFAST:
                latest_job_status = job.check_glue_status(
                    reporting_flag=Job.FAILFAST_REPORT
                )
                if latest_job_status == Job.SUCCESS_STATUS:
                    log.info(
                        "Job completed successfully - {0}".format(
                            self.queue[self.head].file_name
                        )
                    )
                    self.head += 1
                    if self.head == self.queue_len:
                        self.status = Job_Queue.EMPTY_STATUS
                elif latest_job_status == Job.FAILED_STATUS:
                    log.error(
                        "Job failed - {0}".format(self.queue[self.head].file_name)
                    )
                    log.error(
                        "FAILFAST QUEUE - skipping to end of queue, abandoning any/all pending jobs"
                    )
                    self.head = self.queue_len
                    self.status = Job_Queue.EMPTY_STATUS


class Array_Job_Queue:
    """
    Array Job Queues are composed out of a list of Job Queues.
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

    def check_queues(self, log):
        """
        This function iterates through each queue and checks the queue status.
        If a single queue is non-empty, the array of job queues is considered non-empty.
        If all queues are empty, the array of job queues is considered empty.
        """
        active_queue_count = 0
        for job_queue in self.parallel_queues:
            queue_status = job_queue.check_queue(log)
            if queue_status is Job_Queue.NON_EMPTY_STATUS:
                active_queue_count += 1
            time.sleep(Array_Job_Queue.ITERATION_INTERVAL)
        log.info("Active queue count - {0}".format(active_queue_count))
        if active_queue_count == Array_Job_Queue.ACTIVE_QUEUE_COUNT_THRESHOLD:
            self.status = Array_Job_Queue.EMPTY_STATUS
        else:
            self.status = Array_Job_Queue.NON_EMPTY_STATUS
        return self.status

    def execute_queues(self, log):
        """
        Parameters:

        log: logging.Logger

        Returns:

        int - Either the success constant is returned or a Glue timeout occurs

        This function repeatedly checks each queue for queue completion
        until all queues are empty. Each set of checks are separated by
        a pause specified by the user at instantiation.
        """
        completion_status = Array_Job_Queue.NON_EMPTY_STATUS
        while completion_status == Array_Job_Queue.NON_EMPTY_STATUS:
            completion_status = self.check_queues(log)
            time.sleep(self.iteration_pause)
        return Array_Job_Queue.ARRAY_QUEUE_COMPLETION


def get_list_of_feed_files_in_s3(bucket_name, folder, log):
    """
    Parameters:

    bucket_name: str - name of AWS S3 bucket containing feed files
    folder: str - directory containing feed files (/ delimited with no starting delimiter eg home/contents/)
    log: logging.Logger

    Returns:

    feed_files: List[str] - list of feed files in S3 containing feed data

    This function returns a list of feed file names contained in a user specified S3 directory.
    The criteria for filtering is conveyed in function comments
    """
    log.info("Connecting with S3...")
    try:
        client = boto3.client("s3")
        response = client.list_objects(Bucket=bucket_name, Delimiter="/", Prefix=folder)
    except BaseException:
        error_msg = "Failed to connect/retrieve file objects for S3 bucket {0}, folder - {1} - please check whether bucket/folder exists or permission is enabled".format(
            bucket_name, folder
        )
        log.error(error_msg)
        log.error(traceback.format_exc())
        raise Exception(error_msg)
    try:
        # extract S3 fully qualified names for each object
        qualified_bucket_objects = [x["Key"] for x in response["Contents"]]
        log.info(
            "Objects in bucket {0}, folder {1} - {2}".format(
                bucket_name, folder, qualified_bucket_objects
            )
        )
        # remove directory prefix from filenames
        unqualified_bucket_objects = [
            x.split("/")[-1] for x in qualified_bucket_objects
        ]
        # remove empty-file directory reference from collection
        unqualified_bucket_files = filter(lambda x: x != "", unqualified_bucket_objects)
        # remove control files from collection
        feed_files = list(filter(lambda x: "_CNTL_" not in x, unqualified_bucket_files))
        log.info(
            "Feed files in bucket {0}, folder {1} - {2}".format(
                bucket_name, folder, feed_files
            )
        )
        return feed_files
    except BaseException:
        error_msg = "Failed to filter S3 objects into usable file names"
        log.error(error_msg)
        raise Exception(error_msg)


def get_feeds_and_destinations(
    table_name, ddb_region, feed_name_attribute, target_table_attribute, log
):
    """
    Parameters:

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


def map_feed_files_to_destination_tables(
    feed_files, feeds_destinations, feed_name_attribute, target_table_attribute, log
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
    log.info(
        "Left joining feed file names with job metadata from DynamoDB on recognized feed names"
    )
    try:
        files_to_feeds = list(
            filter(
                lambda x: len(x[1]) > 0,
                map(
                    lambda x: (
                        x,
                        list(
                            filter(
                                lambda y: y[feed_name_attribute]
                                == trim_feed_file_name(x)
                                or y[feed_name_attribute]
                                == trim_feed_file_name(x) + "_",
                                feeds_destinations,
                            )
                        ),
                    ),
                    feed_files,
                ),
            )
        )
        log.info("File names to feed name mapping - {0}".format(files_to_feeds))
        file_destination_pairs = list(
            map(lambda x: (x[0], x[1][0][target_table_attribute]), files_to_feeds)
        )
        log.info(
            "Left join of feed file names with job metadata - {0}".format(
                file_destination_pairs
            )
        )
        return file_destination_pairs
    except BaseException:
        error_msg = (
            "Left join of feed file names with job metadata from DynamoDB failed"
        )
        log.error(error_msg)
        log.error(traceback.format_exc())
        raise Exception(error_msg)


def group_file_names_by_destination_table(file_destination_pairs, log):
    """
    Parameters:

    file_destination_pairs: List[Tuple[str, str]] - list of tuples containing file names
                                                    and their corresponding target destination table
                                                    names - [(feed_file_name, target_destination_table_name), ...]
    log: logging.Logger

    Returns:

    job_groups: List[List[str]] - list of lists of file names which have been grouped together according
                                  to their target destination tables

    This function groups the file names in lists by common destination tables
    """
    log.info("Grouping file names by their destination tables")
    try:
        job_groups = []
        for key, group in itertools.groupby(
            sorted(file_destination_pairs, key=lambda x: x[1]), key=lambda x: x[1]
        ):
            job_groups.append([element[0] for element in group])
        log.info("Grouped file names by destination table - {0}".format(job_groups))
        return job_groups
    except BaseException:
        error_msg = (
            "Left join of feed file names with job metadata from DynamoDB failed"
        )
        log.error(error_msg)
        log.error(traceback.format_exc())
        raise Exception(error_msg)


def extract_timestamp(job, log):
    """
    Parameters:

    job: str
    log: logging.Logger

    Returns:

    datetime.datetime

    This function extracts a python datetime object from a feed file name
    """
    file_name = job.split(".")[0]
    dt = file_name.split("_")[-1]

    year = int(dt[0:4])
    month = int(dt[4:6])
    day = int(dt[6:8])
    if len(dt) == 8:
        hour, minute, second = 0, 0, 0
    elif len(dt) == 14:
        hour = int(dt[8:10])
        minute = int(dt[10:12])
        second = int(dt[12:14])
    else:
        error_msg = "Timestamp/date substring for file should be 8 or 14 characters - {0}".format(
            job
        )
        log.error(error_msg)
        raise ValueError(error_msg)
    try:
        ts = datetime.datetime(
            year=year, month=month, day=day, hour=hour, minute=minute, second=second
        )
    except ValueError as e:
        error_msg = "Timestamp/date substring for file - {0} is invalid - {1}".format(
            job, str(e)
        )
        log.error(error_msg)
        raise ValueError(error_msg)
    return ts


def order_jobs(job_list, log):
    """
    Parameters:

    job_list: List[str]
    log: logging.Logger

    Returns:

    List[str]

    This function orders the jobs in a list according to some timestamp or date
    embedded in the name of each job and returns the ordered list
    """
    ordered_job_list = sorted(job_list, key=lambda x: extract_timestamp(x, log))
    return ordered_job_list


def run_job_queues(glue_job, job_groups, iteration_pause, log):
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
    log.info(
        "Running {0} parallel queues of jobs for filenames - {1}".format(
            len(job_groups), job_groups
        )
    )
    log.info(
        "Iteration through queues will be subject to {0} second pause".format(
            iteration_pause
        )
    )
    log.info("Executing all jobs using common Glue job - {0}".format(glue_job))
    try:
        array_jq = Array_Job_Queue(
            job_queue_list=[
                Job_Queue(
                    [Job(job_name=glue_job, file_name=s3_file) for s3_file in job_group]
                )
                for job_group in job_groups
            ],
            iteration_pause=iteration_pause,
        )
    except BaseException:
        error_msg = "Failed to instantiate array of job queues"
        log.error(error_msg)
        log.error(traceback.format_exc())
        raise Exception(error_msg)
    try:
        array_jq.execute_queues(log)
        return
    except BaseException:
        error_msg = (
            "Error occured during the parallel execution of the array of job queues"
        )
        log.error(error_msg)
        log.error(traceback.format_exc())
        raise Exception(error_msg)


if __name__ == "__main__":
    try:
        s3_objects = get_list_of_feed_files_in_s3(
            bucket_name=Constants.S3_FEED_FILE_SOURCE_BUCKET,
            folder=Constants.S3_FEED_FILE_SOURCE_FOLDER,
            log=log,
        )

        feeds_destinations = get_feeds_and_destinations(
            table_name=Constants.DDB_JOB_METADATA_TABLE,
            ddb_region=Constants.AWS_REGION,
            feed_name_attribute=Constants.DDB_FEED_NAME_ATTRIBUTE,
            target_table_attribute=Constants.DDB_TARGET_TABLE_ATTRIBUTE,
            log=log,
        )
        (
            first_feeds_destination_pairs,
            final_feeds_destination_pairs,
        ) = split_feeds_destinations_by_first_and_final_jobs(
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
