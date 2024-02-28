import os
import configparser
# Reading environment variables from aws credential file

def get_aws_key():
    aws_profile = 'default' 
    config = configparser.ConfigParser()
    config.read(os.path.expanduser("~/.aws/credentials"))

    access_id = config.get(aws_profile, "aws_access_key_id") 
    access_key = config.get(aws_profile, "aws_secret_access_key") 
    return access_id, access_key