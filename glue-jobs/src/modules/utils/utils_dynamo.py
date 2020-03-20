import boto3
import json
import modules.config.config as config
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from modules.exceptions.AppUtilsException import AppUtilsError
from modules.constants import constant
import traceback
from time import sleep
from boto3.dynamodb.conditions import Attr

# craete dynamodb client
dynamodb_client = boto3.client("dynamodb", region_name=config.REGION)


def convert_dynamodb_item_to_dict(dynamodb_item):
    """This function is created to convert dynamodb item having its dynamodb types
        to python dictionary

        Arguments:
            dynamodb_item {[dictionary]} -- [dynamodb item ]

        Returns:
            [class] -- Returns a deserialized python dict
    """
    deserializer = TypeDeserializer()
    item_dict = {
        k: deserializer.deserialize(v) for k, v in dynamodb_item["Item"].items()
    }
    return item_dict


def convert_dict_to_dynamodb(data_dict):
    """This function is created to convert python dictionary to
        dynamodb item having its dynamodb types

        Arguments:
            data_dict {[dictionary]} -- [python dict having key, value pair of dynamodb
             attributes and respective values ]

        Returns:
            [dict] -- Returns a dynamodb type serialized python dict
    """
    serializer = TypeSerializer()
    dndb_item = {k: serializer.serialize(v) for k, v in data_dict.items()}
    return dndb_item


def get_ddb_attributes(table_name, ddb_region, attribute_list, log):
    """
    Parameters:

    table_name: str - name of DynamoDB table to get data from
    ddb_region: str - region of DynamoDB table to get data from
    attribute_list: List[str] - list of DynamoDB attributes to get
    log: logging.Logger

    Returns:

    response - List[Dict[str, Any]] - contains the feed names and destination table names for each job in the DynamoDB table

    This function reads the specified attributes from the specified dynamodb table and returns the
    results as a list of dictionaries with each element representing a record in DynamoDB
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
        result = []
        response = {"LastEvaluatedKey": None}
        ddb_page = 0
        while "LastEvaluatedKey" in response.keys():
            log.info(
                "Scanning DynamoDB table - {0} from region - {1} for attributes - {2}".format(
                    table_name, ddb_region, attribute_list
                )
            )
            if ddb_page == 0:
                response = table.scan(
                    AttributesToGet=attribute_list, Select="SPECIFIC_ATTRIBUTES"
                )
            else:
                response = table.scan(
                    ExclusiveStartKey=response["LastEvaluatedKey"],
                    AttributesToGet=attribute_list,
                    Select="SPECIFIC_ATTRIBUTES",
                )
            log.info(
                "Captured data from page {0} of DynamoDB table {1} - {2} records".format(
                    ddb_page, table_name, len(response["Items"])
                )
            )
            result += response["Items"]
            ddb_page += 1
        return result
    except BaseException:
        error_msg = "Failed to pull attributes {0} from DynamoDB table - {1}".format(
            attribute_list, table_name
        )
        log.error(error_msg)
        log.error(traceback.format_exc())
        raise Exception(error_msg)


def get_filtered_ddb_attributes(
    table_name,
    ddb_region,
    attribute_list,
    filter_attribute,
    begins_with_constraints,
    log,
):
    """
    Parameters:

    table_name: str - name of DynamoDB table to get data from
    ddb_region: str - region of DynamoDB table to get data from
    attribute_list: List[str] - list of DynamoDB attributes to get
    filter_attribute: str
    begins_with_constraints: List[Any]
    log: logging.Logger

    Returns:

    response - List[Dict[str, Any]] - contains the feed names and destination table names for each job in the DynamoDB table

    This function reads the specified attributes from the specified dynamodb table and returns the
    results as a list of dictionaries with each element representing a record in DynamoDB
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
        result = []
        for begins_with_constraint in begins_with_constraints:
            response = {"LastEvaluatedKey": None}
            ddb_page = 0
            while "LastEvaluatedKey" in response.keys():
                log.info(
                    "Scanning DynamoDB table - {0} from region - {1} for attributes - {2}".format(
                        table_name, ddb_region, attribute_list
                    )
                )
                if ddb_page == 0:
                    response = table.scan(
                        FilterExpression=Attr(filter_attribute).begins_with(
                            begins_with_constraint
                        )
                    )
                else:
                    response = table.scan(
                        ExclusiveStartKey=response["LastEvaluatedKey"],
                        FilterExpression=Attr(filter_attribute).begins_with(
                            begins_with_constraint
                        ),
                    )
                log.info(
                    "Captured data from page {0} of DynamoDB table {1} - {2} records for {3} matching {4}".format(
                        ddb_page,
                        table_name,
                        len(response["Items"]),
                        filter_attribute,
                        begins_with_constraint,
                    )
                )
                selected_attributes = lambda row, attributes: {
                    key: value for key, value in row.items() if key in attributes
                }
                filtered_page = [
                    selected_attributes(row, attribute_list)
                    for row in response["Items"]
                ]
                result += filtered_page
                ddb_page += 1
        if len(result) == 0:
            raise Exception(
                "Found 0 results from DynamoDB table {0} where {1} begins with any of {2}, please check attribute and begins-with constraints for errors".format(
                    table_name, filter_attribute, begins_with_constraints
                )
            )
        return result
    except BaseException:
        error_msg = "Failed to pull attributes {0} from DynamoDB table - {1}".format(
            attribute_list, table_name
        )
        log.error(error_msg)
        log.error(traceback.format_exc())
        raise Exception(error_msg)


class DynamoUtils:
    def __init__(self):
        pass

    @staticmethod
    def get_dndb_item(
        partition_key_atrr,
        partition_key_value,
        table,
        sort_key_attr=None,
        sort_key_value=None,
        logger=None,
    ):
        """This function will get the dynamodb item based on parimary, partition key of table

            Arguments:
               partition_key_atrr {String} -- partition_key_atrr key attibute name of dynamodb table
               partition_key_value{String,Int,Float,Dict} -- partition_key attribute value
               {List} -- List of attributes to be fetched
               table{String} -- name of dynamodb table
               sort_key_attr{String} -- sort_key attibute name of dynamodb table
               sort_key_value{String,Int,Float,Dict} -- sort key attribute value
               logger {logger} -- logger object

            Returns:
                [dict] -- Returns a dynamodb item with  type deserialized python dict
        """
        dynamo_db_items = None
        try:
            if sort_key_attr != None and sort_key_value != None:
                Key = {
                    partition_key_atrr: convert_dict_to_dynamodb(
                        {partition_key_atrr: partition_key_value}
                    )[partition_key_atrr],
                    sort_key_attr: convert_dict_to_dynamodb(
                        {sort_key_attr: sort_key_value}
                    )[sort_key_attr],
                }

            else:
                Key = {
                    partition_key_atrr: convert_dict_to_dynamodb(
                        {partition_key_atrr: partition_key_value}
                    )[partition_key_atrr]
                }
            stage_status_response = dynamodb_client.get_item(TableName=table, Key=Key)
            dynamo_db_items = convert_dynamodb_item_to_dict(stage_status_response)
            logger.info("DynamoDB  response : {}".format(dynamo_db_items))

        except Exception as error:
            logger.error(
                "Error Occured in get_dndb_item due to : {}".format(error),
                exc_info=True,
            )
            dynamo_db_items = {}
            raise AppUtilsError(
                moduleName=constant.DYNAMODB_UTILS,
                exeptionType=constant.IO_DYNAMODB_READ_EXCEPTION,
                message="Error Occured in get_dndb_item due to : {}".format(
                    traceback.format_exc()
                ),
            )
        return dynamo_db_items

    @staticmethod
    def update_dndb_items(
        table,
        partition_key_atrr,
        partition_key_value,
        attributes_to_be_updated_dict,
        sort_key_attr=None,
        sort_key_value=None,
        logger=None,
    ):
        """This function will update the dynamodb item attribute  based on parimary, partition key of table

            Arguments:
               primkey_atrr {String} -- primary key attibute name of dynamodb table
               primkey_value{String,Int,Float,Dict} -- primary key attribute value
               attributes_to_be_updated_dict{Dict} -- python dict with attribute name and respective values to be updated
               table{String} -- name of dynamodb table
               sortkey_attr{String} -- sort key attibute name of dynamodb table
               sortkey_value{String,Int,Float,Dict} -- sort key attribute value
               logger {logger} -- logger object

            Returns:
                [dict] -- Returns a dynamodb item with  type deserialized python dict
        """
        status_updated = False
        try:
            update_expression = ""
            AttributeNames = {}
            AttributeValues = {}
            for index, (key, value) in enumerate(attributes_to_be_updated_dict.items()):
                AttributeNames["#FIELD{}".format(index)] = key
                AttributeValues[":value{}".format(index)] = convert_dict_to_dynamodb(
                    {key: value}
                )[key]
                if index != len(attributes_to_be_updated_dict) - 1:
                    update_expression = (
                        update_expression
                        + "#FIELD{} = :value{},".format(index, index, index)
                    )
                else:
                    update_expression = (
                        update_expression
                        + "#FIELD{} = :value{}".format(index, index, index)
                    )
            update_expression = "SET " + update_expression

            if sort_key_attr != "" and sort_key_value != "":
                Key = {
                    partition_key_atrr: convert_dict_to_dynamodb(
                        {partition_key_atrr: partition_key_value}
                    )[partition_key_atrr],
                    sort_key_attr: convert_dict_to_dynamodb(
                        {sort_key_attr: sort_key_value}
                    )[sort_key_attr],
                }

            else:
                Key = {
                    partition_key_atrr: convert_dict_to_dynamodb(
                        {partition_key_atrr: partition_key_value}
                    )[partition_key_atrr]
                }

            update_stage_response = dynamodb_client.update_item(
                TableName=table,
                Key=Key,
                ReturnValues="UPDATED_NEW",
                UpdateExpression=update_expression,
                ExpressionAttributeNames=AttributeNames,
                ExpressionAttributeValues=AttributeValues,
            )
            status_updated = True
            logger.info(
                "Update stage response : {}".format(json.dumps(update_stage_response))
            )
            sleep(1)
        except Exception as error:
            status_updated = False
            logger.error(
                "Error Occured in update_dndb_items due to : {}".format(error),
                exc_info=True,
            )
            raise AppUtilsError(
                moduleName=constant.DYNAMODB_UTILS,
                exeptionType=constant.IO_DYNAMODB_UPDATE_EXCEPTION,
                message="Error Occured in update_dndb_items due to : {}".format(
                    traceback.format_exc()
                ),
            )
        return status_updated

    @staticmethod
    def put_dndb_item(dndb_item, table, logger):
        """This function will put the dynamodb item

            Arguments:
              dndb_item{Dict} -- python dict with attributes and respective value

            Returns:
                [Boolean] -- Returns a Boolean status
        """
        put_db_item_status = False
        try:
            dndb_put_item_response = dynamodb_client.put_item(
                TableName=table, Item=convert_dict_to_dynamodb(dndb_item)
            )
            logger.info("DynamoDB  responce : {}".format(dndb_put_item_response))
            put_db_item_status = True

        except Exception as error:
            logger.error(
                "Error Occured in put_dndb_item due to : {}".format(error),
                exc_info=True,
            )
            raise AppUtilsError(
                moduleName=constant.DYNAMODB_UTILS,
                exeptionType=constant.IO_DYNAMODB_PUT_EXCEPTION,
                message="Error Occured in put_dndb_item due to : {}".format(
                    traceback.format_exc()
                ),
            )
            put_db_item_status = False
        return put_db_item_status
