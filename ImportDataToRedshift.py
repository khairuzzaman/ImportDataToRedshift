__author__ = 'Abu Shaz'

import sys
from os.path import expanduser
import yaml
import os
from Util.ParseFile import ParseFile
from DataContexts.SqlServerDataContext import SqlServerDataContext
from DataContexts.RedshiftDataContext import RedshiftDataContext
from AWSServices.S3Service import S3Service
from Util.CompressedFile import FileCompression
from Util.FileManipulation import FileManipulation


def main():
    if len(sys.argv) > 1:
        file_location = os.path.dirname(sys.argv[1]) # Need to work here for space in argv
    else:
        file_location = os.path.dirname(expanduser("~"))

    #print(file_location + "/config.yml")

    #print(os.path.abspath('.'))

    #file_location = os.path.dirname("C:\\Users\khairuzzaman\Desktop\Python Script\\config.yml")

    try:
        config_file = ParseFile.parse_yml_file(file_location + "/config.yml")
        sql_configuration = config_file['SQLServer']
        redshift_configuration = config_file['Redshift']
        s3_configuration = config_file['S3Config']
    except Exception as err:
        print('Config file is not well formatted or not available')

    table_name = sql_configuration["table"]
    file_name = table_name + ".gz"

    print("Data Loading from SQL Server Start ...\n")
    try:
        sql_context = SqlServerDataContext(config=sql_configuration)
        sql_context.export_data_to_file(table_name,directory=file_location)

    except Exception as err:
        print(err)
        raise

    print("SQL Server Data Loading Complete ...\n")

    try:
        FileCompression.gzip_compression(table_name,"txt",directory=file_location)
    except Exception as err:
        FileManipulation.remove_file(file_location + "/" + table_name + ".txt")
        print(err)
        raise

    print("Upload File to S3 ...\n")
    try:
        s3_service = S3Service(config=s3_configuration)
        s3_service.upload_file_to_s3(s3_configuration['bucket_name'],file_location=file_location,file_name=file_name)
    except Exception as err:
        FileManipulation.remove_file(file_location + "/" + table_name + ".txt")
        FileManipulation.remove_file(file_location + "/" + file_name )
        print(err)
        raise

    print("File Upload Complete ...\n")

    print("Start Executing Copy Command ...\n")
    try:
        cpoy_command = {}
        cpoy_command['table_name'] = redshift_configuration['table']
        cpoy_command['bucket_name'] = s3_configuration['bucket_name']
        cpoy_command['file_name'] = file_name
        cpoy_command['aws_access_key_id'] = s3_configuration['AWS_ACCESS_KEY_ID']
        cpoy_command['aws_secret_access_key'] = s3_configuration['AWS_SECRET_ACCESS_KEY']
        cpoy_command['delimiter'] = '\\t'

        redshift_configuration["port"] = "5439"
        redshift_context = RedshiftDataContext(config=redshift_configuration)
        redshift_context.execute_copy_command(command_param=cpoy_command)

    except Exception as err:
        FileManipulation.remove_file(file_location + "/" + table_name + ".txt")
        FileManipulation.remove_file(file_location + "/" + file_name )
        print(err)
        raise

    print("Executing Copy Command Finish ...\n")

    FileManipulation.remove_file(file_location + "/" + table_name + ".txt")
    FileManipulation.remove_file(file_location + "/" + file_name )

    #print(sql_configuration)
    #print(redshift_configuration)


if __name__ == '__main__':
    main()


