
pip install boto3
from boto3.s3.transfer import S3Transfer

import boto3

def upload_files(path,bucket_name,folder,file):

    client = boto3.client('s3', aws_access_key_id='xxxxxxxx',aws_secret_access_key='xxxxxxxxx')

    transfer = S3Transfer(client)

    transfer.upload_file(path, bucket_name, folder + file)

upload_files('C:/Users/thejaswinir/OneDrive - Maveric Systems Limited/SnowFlakeTraining/capstone/transactions1.csv','capston','banking/','transactions.csv')
upload_files('C:/Users/thejaswinir/OneDrive - Maveric Systems Limited/SnowFlakeTraining/capstone/accounts1.csv','capston','banking/','transactions.csv')
upload_files('C:/Users/thejaswinir/OneDrive - Maveric Systems Limited/SnowFlakeTraining/capstone/customers1.csv','capston','banking/','transactions.csv')
upload_files('C:/Users/thejaswinir/OneDrive - Maveric Systems Limited/SnowFlakeTraining/capstone/credit_data1.csv','capston','banking/','transactions.csv')
upload_files('C:/Users/thejaswinir/OneDrive - Maveric Systems Limited/SnowFlakeTraining/capstone/watchlist1.csv','capston','banking/','transactions.csv')
