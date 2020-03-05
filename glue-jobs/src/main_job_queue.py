import logging
import time
import traceback
import boto3
import itertools
import sys

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
file_handler = logging.FileHandler("./log.txt")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
# Attach handlers to log
log.addHandler(stream_handler)
log.addHandler(file_handler)


def trigger_glue_job(job_name, file_name, log):
    """
    This function triggers an AWS Glue job using simple a name
    """
    client = boto3.client("glue", region_name=Constants.AWS_REGION)
    log.info("Trigger job - {0} with file name - {1}".format(job_name, file_name))
    response = client.start_job_run(
        JobName=job_name, Arguments={"--FILE_NAME": file_name}, Timeout=2880
    )
    log.info(
        "Job successfully triggered. Job run ID - {0}".format(response["JobRunId"])
    )
    return response["JobRunId"]


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


class Job:
    """
    A Job is a model of an AWS Glue job which allows the object to run
    and subsequently monitor Glue jobs.
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
        self.job_run_id = trigger_glue_job(
            job_name=self.job_name, file_name=self.file_name, log=log
        )
        self.status = self.PROCESSING_STATUS
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
            latest_job_status = job.check_glue_status()
            if latest_job_status == Job.COMPLETE_STATUS:
                log.info("Job complete - {0}".format(self.queue[self.head].file_name))
                self.head += 1
                if self.head == self.queue_len:
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
        active_queue_count = [
            job_queue.check_queue(log) for job_queue in self.parallel_queues
        ].count(Job_Queue.NON_EMPTY_STATUS)
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
        qualified_bucket_objects = map(lambda x: x["Key"], response["Contents"])
        log.info(
            "Objects in bucket {0}, folder {1} - {2}".format(
                bucket_name, folder, list(qualified_bucket_objects)
            )
        )
        # remove directory prefix from filenames
        unqualified_bucket_objects = map(
            lambda x: x.split("/")[-1], qualified_bucket_objects
        )
        # remove empty-file directory reference from collection
        unqualified_bucket_files = filter(lambda x: x != "", unqualified_bucket_objects)
        # remove control files from collection
        feed_files = list(filter(lambda x: "_CNTL_" not in x, unqualified_bucket_files))
        log.info(
            "Feed files in bucket {0}, folder {1} - {2}".format(
                bucket_name, folder, list()
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
        log.error(error_msg)
        log.error(traceback.format_exc())
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
            feed_name_attribute, target_table_attribute
        )
        log.error(error_msg)
        log.error(traceback.format_exc())
        raise Exception(error_msg)


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
        for key, group in itertools.groupby(file_destination_pairs, key=lambda x: x[1]):
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
    log.info("Running parallel queues of jobs for filenames - {0}".format(job_groups))
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


class Constants:
    # Configured via Param store
    DDB_JOB_METADATA_TABLE = "vf-dev-etl-file-broker"
    S3_LOG_DESTINATION_BUCKET = "vf-datalake-dev-logs"
    S3_FEED_FILE_SOURCE_BUCKET = "vf-datalake-dev-refined"
    AWS_REGION = "us-east-1"
    # Configured via Glue job
    GLUE_JOB_NAME = "Test_pipeline_anchal"
    S3_LOG_DESTINATION_FOLDER = "glue_process"
    S3_FEED_FILE_SOURCE_FOLDER = "current/"
    DDB_FEED_NAME_ATTRIBUTE = "feed_name"
    DDB_TARGET_TABLE_ATTRIBUTE = "tgt_dstn_tbl_name"
    ITERATION_PAUSE = 30


s3_objects = get_list_of_feed_files_in_s3(
    bucket_name=Constants.S3_FEED_FILE_SOURCE_BUCKET,
    folder=Constants.S3_FEED_FILE_SOURCE_FOLDER,
    log=log,
)
print(s3_objects)

response = get_feeds_and_destinations(
    table_name=Constants.DDB_JOB_METADATA_TABLE,
    ddb_region=Constants.AWS_REGION,
    feed_name_attribute=Constants.DDB_FEED_NAME_ATTRIBUTE,
    target_table_attribute=Constants.DDB_TARGET_TABLE_ATTRIBUTE,
    log=log,
)
print(response)


file_destination_pairs = map_feed_files_to_destination_tables(
    feed_files=s3_objects,
    feeds_destinations=response,
    feed_name_attribute=Constants.DDB_FEED_NAME_ATTRIBUTE,
    target_table_attribute=Constants.DDB_TARGET_TABLE_ATTRIBUTE,
    log=log,
)

print(file_destination_pairs)


job_groups = group_file_names_by_destination_table(
    file_destination_pairs=file_destination_pairs, log=log
)
print(job_groups)

run_job_queues(
    glue_job=Constants.GLUE_JOB_NAME,
    job_groups=job_groups,
    iteration_pause=Constants.ITERATION_PAUSE,
    log=log
)
