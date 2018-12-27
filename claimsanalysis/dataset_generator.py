__author__ = 'Manikandan Pounraj'

import random
from random import randint
import json


class Generator:
    def __init__(self):
        self.file_name = ""

    def execute(self):
        self.mappings = "/home/sys-user/Desktop/Dataset/global-dataset/field-mappings.json"
        with open(self.mappings, "r") as mapping_file:
            mappings_json = json.load(mapping_file)

        for key in mappings_json.keys():
            print(key)
            self.file_name = "/home/sys-user/Desktop/Dataset/{}.csv".format(key)
            file_obj = open(self.file_name, "a")


if __name__ == "__main__":
    generator = Generator()
    print("Generating customer dataset")
    generator.execute()
