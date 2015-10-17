__author__ = 'Abu Zafor Khairuzzaman'

import gzip
import os


class FileCompression:
    """ This class is responsible for compress file """

    @staticmethod
    def gzip_compression(source_file_name, source_file_extension, **kwargs):
        file_location = os.path.abspath('.')
        if "directory" in kwargs:
            file_location = kwargs["directory"]

        directory_name = os.path.dirname(file_location)

        source_file_full_name = source_file_name + "." + source_file_extension
        destination_file_name = source_file_name + ".gz"

        source_file_full_path = directory_name + "\\" + source_file_full_name
        destination_file_full_path = directory_name + "\\" + destination_file_name

        with open(source_file_full_path, "rb") as input_file:
            with gzip.open(destination_file_full_path, "wb") as output_file:
                output_file.writelines(input_file)
