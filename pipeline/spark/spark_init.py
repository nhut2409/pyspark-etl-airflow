from spark import aws_key
from spark import spark_config

access_id, access_key = aws_key.get_aws_key()
spark = spark_config.spark_conf(access_id, access_key)