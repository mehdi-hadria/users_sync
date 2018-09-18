



# Functions for dynamodb

def get_dynamodb_table(aws_access_key_id,aws_secret_access_key,region_name,table_name):

    import boto3
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )
    dynamoDB = session.resource('dynamodb')
    return dynamoDB.Table(table_name)


# Function to get data from dynamodb (dp)
def get_dp_data(aws_access_key_id,aws_secret_access_key,region_name,table_name,attributes_list,filters_list=None,attribute_names=None,attribute_values=None):

    scan_parameter = {}
    if attributes_list is not None:
        scan_parameter["ProjectionExpression"] = ','.join(attributes_list)

    if filters_list is not None:
        scan_parameter["FilterExpression"] = ' and '.join(filters_list)
        scan_parameter["ExpressionAttributeNames"] = attribute_names
        scan_parameter["ExpressionAttributeValues"] = attribute_values

    dp_table = get_dynamodb_table(aws_access_key_id,aws_secret_access_key,region_name,table_name)
    response = dp_table.scan(**scan_parameter)
    data = response['Items']
    while 'LastEvaluatedKey' in response:
        response = dp_table.scan(**scan_parameter,ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
    return data


#Function to push data to dynamodb
def push_ad_data(aws_access_key_id,aws_secret_access_key,region_name,table_name,data):

    dp_table = get_dynamodb_table(aws_access_key_id,aws_secret_access_key,region_name,table_name)

    with dp_table.batch_writer() as batch:
        for item in data:
            batch.put_item(
                Item=item
            )

#Function to delete data from dynamodb:
def delete_dp_data(aws_access_key_id,aws_secret_access_key,region_name,table_name,keys_list):


    dp_table = get_dynamodb_table(aws_access_key_id,aws_secret_access_key,region_name,table_name)
    db_table_key = dp_table.key_schema[0]['AttributeName']
    with dp_table.batch_writer() as batch:
        for key in keys_list:
            batch.delete_item(Key={db_table_key: key})

# For RDS : Add method to get, insert and delete data from RDS :