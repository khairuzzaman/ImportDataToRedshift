__author__ = 'Abu Zafor Khairuzzaman'

import boto
import os
from boto.s3.connection import OrdinaryCallingFormat
from boto.s3.key import Key


class S3Service:
    """ This class is responsible for provide s3 services """

    def __init__(self, **kwargs):
        config = kwargs["config"]
        # Create private s3 connection (instance attribute)
        self.__connection = boto.connect_s3(aws_access_key_id=config["AWS_ACCESS_KEY_ID"],
                                       aws_secret_access_key=config["AWS_SECRET_ACCESS_KEY"],
                                       calling_format=OrdinaryCallingFormat()) # , is_secure = True, validate_certs = False

    def upload_file_to_s3(self, bucket_name, **kwargs):
        bucket_name = bucket_name
        file_location = os.path.abspath('.')
        if 'file_location' in kwargs:
            file_location = kwargs["file_location"]

        directory_name = os.path.dirname(file_location)

        file_name = kwargs["file_name"]

        file_full_path = directory_name + "\\" + file_name

        bucket = self.__connection.get_bucket(bucket_name)

        key = file_name
        k = Key(bucket)
        k.key = key
        k.set_contents_from_filename(file_full_path)

        self.close_connection()

    def close_connection(self):
        self.__connection.close()
