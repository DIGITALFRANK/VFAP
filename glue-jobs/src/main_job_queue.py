import logging
import time
import boto3
import itertools

log = logging.getLogger()


def trigger_glue_job(job_name, file_name, log):
    """
    This function triggers an AWS Glue job using simple a name
    """
    log.info("Getting Boto3 Glue client")
    print("Getting Glue client")
    # TODO: constant required for region  name
    client = boto3.client("glue", region_name="us-east-1")
    log.info("Successfully created Boto3 Glue client")
    print("Successfully created Boto3 Glue client")
    log.info("Trigger job - {0} with file name - {1}".format(job_name, file_name))
    print("Trigger job - {0} with file name - {1}".format(job_name, file_name))
    response = client.start_job_run(
        JobName=job_name, Arguments={"--FILE_NAME": file_name}, Timeout=2880
    )
    log.info(
        "Job successfully triggered. Job run ID - {0}".format(response["JobRunId"])
    )
    print("Job successfully triggered. Job run ID - {0}".format(response["JobRunId"]))
    return response["JobRunId"]


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
        self.status = self.PROCESSING_STATUS
        return

    def check_glue_status(self):
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


# response = get_feeds_and_destinations(table_name = "vf-dev-etl-file-broker",
#                                      ddb_region = "us-east-1",
#                                      feed_name_attribute = "feed_name",
#                                      target_table_attribute = "tgt_dstn_tbl_name")
# print(response)


def trim_feed_file_name(feed_file_name):
    return "_".join(feed_file_name.split("_")[:-1])


def map_feed_files_to_destination_tables(
    feed_files, feeds_destinations, feed_name_attribute, target_table_attribute
):
    """
    feed_files: List[str] - list of feed files in S3
    feeds_destinations: List[Dict[str, str]] - list of dictionaries containing feed name
                                               and target destination table name, each with
                                               {"feed_name": ..., "tgt_dstn_tbl_name": ....}

    Returns:

    files_to_destinations: List[Tuple[str, str]] - list of tuples containing file names
                                                   and their corresponding target destination table
                                                   names - [(feed_file_name, target_destination_table_name), ...]
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
    file_destination_pairs: List[Tuple[str, str]] - list of tuples containing file names
                                                    and their corresponding target destination table
                                                    names - [(feed_file_name, target_destination_table_name), ...]

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

    Returns:

    None
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
