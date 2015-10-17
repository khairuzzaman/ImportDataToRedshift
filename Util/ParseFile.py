__author__ = 'Khairuzzaman'

import yaml

class ParseFile:
    """ This class is responsible for parsing file """

    @staticmethod
    def parse_yml_file(file_full_path):
        with open(file_full_path, 'r') as ymlfile:
            cfg = yaml.load(ymlfile)
        return cfg
