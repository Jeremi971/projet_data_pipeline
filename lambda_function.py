import os
import boto3
import pandas as pd

def lambda_handler(event, context):
    S3_MESSAGE_USER_BUCKET = "messages-users-raw-data-bucket-md4-api"
    S3_PIPELINE_BUCKET = "pipeline-result-data-bucket-md4-api"
        
    s3client = boto3.client(
        's3',
        aws_access_key_id = os.environ['aws_access_key_id'],
        aws_secret_access_key = os.environ['aws_secret_access_key']
    )
    
    messages_file = "messages.csv"
    users_file = "users.csv"
    pipeline_file = "pipeline_result.csv"
    
    user_message_objects_list  = s3client.list_objects(Bucket=S3_MESSAGE_USER_BUCKET)['Contents']
    pipeline_objects_list  = s3client.list_objects(Bucket=S3_PIPELINE_BUCKET)['Contents']
    
    for index in range(len(user_message_objects_list)):
        if user_message_objects_list[index]['Key'] == messages_file:
            message_file = user_message_objects_list[index]['Key']
                
        elif user_message_objects_list[index]['Key'] == users_file:
            user_file = user_message_objects_list[index]['Key']
            
    for index in range(len(pipeline_objects_list)):
        if pipeline_objects_list[index]['Key'] == messages_file:
            pipeline_file = pipeline_objects_list[index]['Key']

    message_obj = s3client.get_object(Bucket=S3_MESSAGE_USER_BUCKET, Key=message_file)
    message_df = pd.read_csv(message_obj['Body'], sep = ",")
        
    user_obj = s3client.get_object(Bucket=S3_MESSAGE_USER_BUCKET, Key=user_file)
    user_df = pd.read_csv(user_obj['Body'], sep = ",")
        
    user_id_list = [index for index in user_df['user_id']]
    author_id_list = [index for index in message_df['author_id']]
    user_first_name_list = [name for name in user_df['first_name']]
    user_last_name_list = [name for name in user_df['last_name']]
    number_of_messages_list = [author_id_list.count(index) for index in user_id_list]
        
    dict_out = {
        "user_id": user_id_list,
        "first_name": user_first_name_list,
        "last_name": user_last_name_list,
        "number_of_messages": number_of_messages_list
    }
    
    pipeline_df = pd.DataFrame(dict_out).sort_values("number_of_messages")
    file_to_upload = pipeline_df.to_csv(path_or_buf=None, index=False)

    s3client.put_object(Bucket=S3_PIPELINE_BUCKET, Key=pipeline_file, Body=file_to_upload)