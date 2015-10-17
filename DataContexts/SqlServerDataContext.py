__author__ = 'Abu Zafor Khairuzzaman'

import subprocess
import os


class SqlServerDataContext:
    """ This class is responsible for SQL Server related task """

    def __init__(self, **kwargs):
        config = kwargs["config"]
        self.__server_name = config["server"]
        self.__user_id = config["user"]
        self.__password = config["password"]
        self.__database_name = config["database"]

    def export_data_to_file(self, table_name, **kwargs):
        schema_name = "dbo"
        directory_location = os.path.abspath('.')
        if "schema_name" in kwargs:
            schema_name = kwargs["schema_name"]
        if "directory" in kwargs:
            directory_location = kwargs["directory"]

        directory_name = os.path.dirname(directory_location)

        directory_to_export = directory_name + "/" + table_name + ".txt"

        export_bcp_command = "bcp \"SELECT * FROM [" + self.__database_name + "].[" + schema_name + "].[" + table_name + "] \" queryout " + directory_to_export + " -S " + self.__server_name + " -U " + self.__user_id + " -P " + self.__password + " -c"

        subprocess.call(export_bcp_command, shell=True)
