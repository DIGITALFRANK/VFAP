import boto3
import json
import modules.config.config as config
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from boto3.dynamodb.conditions import Attr, Key

# craete dynamodb client
dynamodb_client = boto3.client('dynamodb', region_name=config.REGION)
dynamo_db = boto3.resource("dynamodb", region_name=config.REGION)


def convert_dynamodb_item_to_dict(dynamodb_item):
    """This function is created to convert dynamodb item having its dynamodb types
        to python dictionary

        Arguments:
            dynamodb_item {[dictionary]} -- [dynamodb item ]

        Returns:
            [class] -- Returns a deserialized python dict
    """
    deserializer = TypeDeserializer()
    item_dict = {k: deserializer.deserialize(
        v) for k, v in dynamodb_item['Item'].items()}
    return item_dict


def convert_dict_to_dynamodb(data_dict):
    """This function is created to convert python dictionary to
        dynamodb item having its dynamodb types

        Arguments:
            data_dict {[dictionary]} -- [python dict having key, value pair of dynamodb attibites and respective values ]

        Returns:
            [dict] -- Returns a dynamodb type serialized python dict
    """
    serializer = TypeSerializer()
    dndb_item = {k: serializer.serialize(v) for k, v in data_dict.items()}
    return dndb_item


class DynamoUtils():
    def __init__(self):
        pass

    @staticmethod
    def get_dndb_item(partition_key_atrr, partition_key_value, table, sort_key_attr=None, sort_key_value=None, logger=None):
        # import ipdb;ipdb.set_trace()
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
                Key = {partition_key_atrr: convert_dict_to_dynamodb({partition_key_atrr: partition_key_value})[partition_key_atrr],
                       sort_key_attr: convert_dict_to_dynamodb({sort_key_attr: sort_key_value})[sort_key_attr]}

            else:
                Key = {partition_key_atrr: convert_dict_to_dynamodb(
                    {partition_key_atrr: partition_key_value})[partition_key_atrr]}
            stage_status_response = dynamodb_client.get_item(
                TableName=table, Key=Key)
            dynamo_db_items = convert_dynamodb_item_to_dict(
                stage_status_response)
            logger.info("DynamoDB  responce : {}".format(dynamo_db_items))

        except Exception as error:
            logger.info(
                "Error Occured in get_stage_status due to : {}".format(error))
            dynamo_db_items = {}
        return dynamo_db_items

    @staticmethod
    def update_dndb_items(table, partition_key_atrr, partition_key_value, attributes_to_be_updated_dict, sort_key_attr,
                          sort_key_value, logger):
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
                AttributeNames['#FIELD{}'.format(index)] = key
                AttributeValues[':value{}'.format(index)] = convert_dict_to_dynamodb({
                    key: value})[key]
                print("AttributeNames in DynamoUtil: ")
                print(AttributeNames)
                print("AttributeValues in DynamoUtil: ")
                print(AttributeValues)
                if index != len(attributes_to_be_updated_dict) - 1:
                    update_expression = update_expression + \
                        "#FIELD{} = :value{},".format(index, index, index)
                    print("update_expression in IF Cond: " + update_expression)
                else:
                    update_expression = update_expression + \
                        "#FIELD{} = :value{}".format(index, index, index)
                    print("update_expression in ELSE Cond: " +
                          update_expression)
            update_expression = "SET "+update_expression
            print("update_expression outer IF: " + update_expression)

            if sort_key_attr != None and sort_key_value != None:
                Key = {partition_key_atrr: convert_dict_to_dynamodb({partition_key_atrr: partition_key_value})[partition_key_atrr],
                       sort_key_attr: convert_dict_to_dynamodb({sort_key_attr: sort_key_value})[sort_key_attr]}
                print("Key from IF: ")
                print(Key)

            else:
                Key = {partition_key_atrr: convert_dict_to_dynamodb(
                    {partition_key_atrr: partition_key_value})[partition_key_atrr]}
                print("Key from ELSE: ")
                print(Key)

            update_stage_response = dynamodb_client.update_item(TableName=table, Key=Key, ReturnValues="UPDATED_NEW",
                                                                UpdateExpression=update_expression, ExpressionAttributeNames=AttributeNames, ExpressionAttributeValues=AttributeValues)
            print("update_stage_response: " + update_stage_response)
            status_updated = True
            logger.info("Update stage response : {}".format(
                json.dumps(update_stage_response)))
        except Exception as error:
            status_updated = False
            logger.info(
                "Error Occured in update stage status due to : {}".format(error))
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
            logger.info("DynamoDB  responce : {}".format(
                dndb_put_item_response))
            put_db_item_status = True

        except Exception as error:
            logger.info(
                "Error Occured in get_stage_status due to : {}".format(error))
            put_db_item_status = False
        return put_db_item_status

    @staticmethod
    def check_record(file_name, params):
        try:
            filetable = dynamo_db.Table(params['status_table'])
            response = filetable.query(
                KeyConditionExpression=Key('file_name').eq(file_name))
            if response['Count'] == 1:
                response = filetable.scan(
                    FilterExpression=Attr('file_name').eq(file_name) & Attr(
                        'job_status').eq('Success')
                )
                if response['Count'] == 1:
                    return True
                else:
                    print("Record available with failed status.Deleting the "
                          "record from the table")
                    filetable.delete_item(
                        Key={
                            'file_name': file_name,
                            'processing_date': response["Items"][0][
                                "processing_date"]
                            }
                    )
                    return False
            else:
                return False

        except Exception as error:
            logger.info(
                "Error Occurred in check_record due to : {}".format(error))
