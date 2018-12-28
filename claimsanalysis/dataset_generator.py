__author__ = 'Manikandan Pounraj'

from random import randint
from datetime import datetime
from collections import OrderedDict
import random
import json
import calendar
import time


class Generator:
    def __init__(self):
        self.file_name = ""
        self.config_directory = "../datasets/"
        self.output_directory = "../datasets/"
        self.person_datafile = self.config_directory+"person-names.txt"
        self.person_names_data = []
        self.mail_exchange_datafile = self.config_directory+"mailexchanges.txt"
        self.mail_exchange_data = []
        self.sequence = 0
        self.repeat = 1
        self.name_store = ""
        self.generated_ids = {}
        self.errors = {}
        self.current_processing_dataset = ""

    def load_lookup(self):
        try:
            with open(self.person_datafile, "r") as person_datafile:
                self.person_names_data = person_datafile.readlines()
                self.person_names_data = [x.strip( ) for x in self.person_names_data]
        finally:
            person_datafile.close()

        try:
            with open(self.mail_exchange_datafile, "r") as mail_exchange_datafile:
                self.mail_exchange_data = mail_exchange_datafile.readlines()
                self.mail_exchange_data = [x.strip( ) for x in self.mail_exchange_data]
        finally:
            mail_exchange_datafile.close()

        return "success"

    def execute(self):
        self.load_lookup()
        self.mappings = self.config_directory+"field-mappings.json"

        with open(self.mappings, "r") as mapping_file:
            mappings_json = json.load(mapping_file, object_pairs_hook=OrderedDict)

        for key in mappings_json.keys():
            print("Generating {} dataset".format(key))
            self.current_processing_dataset = key

            fields = mappings_json[key]["fields"]
            records = mappings_json[key]["records"]
            self.file_name = (self.output_directory+"{}.csv").format(key)
            self.generated_ids[key] = []
            self.errors[key] = {}

            file_obj = open(self.file_name, "a")
            self.sequence = 0
            self.name_store = ""
            try:
                separator = ",";
                field_processed = 1
                for field in fields:
                    if len(fields) == field_processed:
                        separator = ""
                    file_obj.write("{}{}".format(field["name"], separator))
                    field_processed += 1

                file_obj.write("\n")

                for record in range(0,records):
                    self.repeat = 1
                    for field in fields:
                        ref = self.get_field(field, "ref", "")

                        if field["type"] == "identity" and ref != "":
                            self.repeat = int(str(ref).split("/")[1])

                    for index in range (0, self.repeat):
                        separator = ","
                        field_processed = 1
                        for field in fields:
                            method_name = 'generate_' + str(field["type"])
                            method = getattr(self, method_name, lambda: "Invalid Field Generator")
                            data = method(field)
                            if len(fields) == field_processed:
                                separator = ""

                            file_obj.write("{}{}".format(data, separator))
                            field_processed += 1

                            if field["type"] == "identity" and field["store"] == 1:
                                self.generated_ids[key].append(data)

                        file_obj.write("\n")
            finally:
                file_obj.close()

    def get_field(self, field, field_name, default):
        data = default
        try:
            if field.__getitem__(field_name) is not None:
                data = field[field_name]
        except:
            self.errors[self.current_processing_dataset][field_name] = "Field {} in {} not found. Using default.".format(field_name, self.current_processing_dataset)
        return data

    def generate_identity(self, field):
        seq = self.get_field(field, "seq", 0)
        prefix = self.get_field(field, "prefix", "")
        ref = self.get_field(field, "ref", "")

        if ref != "":
            key = str(field["ref"]).split("/")[0]
            repeat_ind = self.sequence
            if self.repeat > 1:
                repeat_ind = round(self.sequence / self.repeat)

            if repeat_ind == 0:
                repeat_ind = 1

            if repeat_ind > len(self.generated_ids[key]):
                repeat_ind = len(self.generated_ids[key])

            return self.generated_ids[key][repeat_ind - 1]

        if seq == 1:
            self.sequence += 1
        if prefix is not None:
            return "{}{}".format(prefix,self.sequence)
        else:
            return int(self.sequence)

    def generate_number(self, field):
        range_value = self.get_field(field, "range", "")

        if range_value != "":
            if int(range_value.split("-")[0]) == int(range_value.split("-")[1]):
                return int(range_value.split("-")[0])
            else:
                return random.randrange(int(range_value.split("-")[0]), int(range_value.split("-")[1]))

    def generate_phone(self, field):
        first = str(random.randint(100, 999))
        second = str(random.randint(1, 888)).zfill(3)
        last = str(random.randint(1, 9998)).zfill(4)
        return "{}-{}-{}".format(first, second, last)

    def generate_date(self, field):
        range_value = self.get_field(field, "range", "")

        end_year = datetime.now( ).year
        if str(range_value.split("-")[1]).upper() != "NOW":
            end_year = range_value.split("-")[1]

        year = random.randint(int(range_value.split("-")[0]), end_year)
        month = random.randint(1, 12)
        date_value = calendar.monthrange(year, month)
        date_value = str(random.randint(1, date_value[1])).zfill(2)
        return "{}-{}-{}".format(year, str(month).zfill(2), date_value)

    def generate_string(self, field):
        delimiter = self.get_field(field, "delimiter", "")
        options = self.get_field(field, "options", "")
        prefix = self.get_field(field, "prefix", "")
        store = self.get_field(field, "store", 0)
        length = self.get_field(field, "length", 100)
        seq = self.get_field(field, "seq", 0)

        if options != "":
            return random.choice(options.split(","))

        if seq == 1:
            return "{}-{}".format(prefix, self.sequence);

        temp_name_store = ""
        self.name_store = temp_name_store

        names_ind = self.sequence
        if self.sequence >= len(self.person_names_data):
            names_ind = random.randint(2, len(self.person_names_data) -1)
            temp_name_store = "{}{}{}{}{}".format(self.person_names_data[names_ind], delimiter,
                                             self.person_names_data[names_ind - 1], delimiter, self.person_names_data[names_ind - 2])
        elif self.person_names_data[self.sequence] is not None:
            temp_name_store = "{}{}{}{}{}".format(self.person_names_data[self.sequence], delimiter,
                                             self.person_names_data[self.sequence], delimiter, self.person_names_data[self.sequence])

        if store == 1:
            self.name_store = temp_name_store

        return self.name_store[0:length]

    def generate_email(self, field):
        email = ""

        if self.name_store != "":
            email = self.name_store.replace(" ", "")

        mail_exchange_index = random.choice('01234')
        email = "{}@{}".format(email,self.mail_exchange_data[int(mail_exchange_index)])

        return email


if __name__ == "__main__":
    generator = Generator()
    print("Started")
    start = time.time()
    generator.execute()
    end = time.time()
    print("Completed. Took Time {} secs".format(round(end - start, 3)))

