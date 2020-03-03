import boto3
import json
import modules.config.config as config
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from modules.exceptions.AppUtilsException import AppUtilsError
from modules.constants import constant
import traceback
from time import sleep

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
