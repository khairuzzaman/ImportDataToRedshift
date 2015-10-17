__author__ = 'Khairuzzaman'

import os

class FileManipulation:
    """ This class is responsible for manipulate file """

    @staticmethod
    def remove_file(file_full_path):
        if os.path.exists(file_full_path):
            os.remove(file_full_path)