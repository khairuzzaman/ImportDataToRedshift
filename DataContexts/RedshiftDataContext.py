__author__ = 'Abu Zafor Khairuzzaman'

import psycopg2


class RedshiftDataContext:
    """ This class is responsible for Redshift Database related task """

    def __init__(self, **kwargs):
        config = kwargs["config"]

        try:
            self.__connection = psycopg2.connect(database=config["database"], user=config["user"], password=config["password"],
                                                 host=config["host"], port=config["port"])
        except Exception as err:
            print(err)

    def execute_data_retrieval_query(self, **kwargs):
        """ This method is used to execute the query and return result set """
        cursor = self.__connection.cursor()
        query = kwargs["query"]

        try:
            cursor.execute(query)
        except Exception as err:
            print(err)

        row = cursor.fetchall()
        self.close_connection()
        return row

    def execute_query(self, **kwargs):
        """ This method is used to execute the query and return result set """
        cursor = self.__connection.cursor()
        query = kwargs["query"]

        try:
            cursor.execute(query)
            self.__connection.commit()
            #cursor.commit()
        except Exception as err:
            print(err)
        self.close_connection()

    def execute_copy_command(self, **kwargs):
        """ This method is used to generate abd execute the copy command """
        copy_command_parameter = kwargs["command_param"]

        ignore_header = ''

        if "ignore_header" in copy_command_parameter:
            ignore_header = copy_command_parameter["ignore_header"]

        copy_command = ("TRUNCATE TABLE " + copy_command_parameter["table_name"] + ";" +"COPY public." + copy_command_parameter["table_name"] +
                        " FROM 's3://" + copy_command_parameter["bucket_name"] + "/" + copy_command_parameter["file_name"] + "'" +
                        " CREDENTIALS 'aws_access_key_id=" + copy_command_parameter["aws_access_key_id"] + ";aws_secret_access_key=" + copy_command_parameter["aws_secret_access_key"] + "'" +
                        " DELIMITER '" + copy_command_parameter["delimiter"] + "' GZIP " + ignore_header + " TRIMBLANKS FILLRECORD EMPTYASNULL BLANKSASNULL MAXERROR 10000;")

        self.execute_query(query=copy_command)

    def close_connection(self):
        self.__connection.close()
