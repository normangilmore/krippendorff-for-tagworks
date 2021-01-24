#  Copyright 2021 Thusly, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import os
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
import argparse
import gzip
from contextlib import closing
import csv
import re
from collections import defaultdict, OrderedDict
from operator import itemgetter
from itertools import groupby, chain
import numpy as np
from krippendorff import alpha

ANSWER_LABEL_RE = re.compile(
    r'\s*T(?P<topic_number>\d+)\.'
    r'Q(?P<question_number>\d+)\.'
    r'A(?P<answer_number>\d+)'
)

# This code takes a Data Hunt Schema file and Data Hunt data file and
# organizes it into reliability matrices to calculate Krippendorff's alpha.
# The answer_uuid key is used to join a data row to the Schema row
#that has metadata about the question being answered.
# The question_types are RADIO, CHECKBOX, and TEXT.
# The setup code is annoying because CHECKBOX answers have to be
# analyzed as individual binary questions, whereas answers
# to a RADIO question are grouped together for comparison.
# We ignore TEXT answers.

# In the case of CHECKBOX questions, it gathers just the one
# schema row for the answer, since each CHECKBOX is analyzed
# in isolation.
# DataHunt CSVs currently do not include enough information to
# determine or infer if a checkbox was not shown vs. not chosen.

# This class gathers the data for the reliability matrix for
# RADIO questions. It gathers the schema rows
# and data rows for all answers to that question.
class RadioVariable:
    def __init__(self, schema_rows):
        if len(schema_rows) == 0:
            raise Exception("No schema rows loaded.")
        self.label = schema_rows[0]['question_label']
        self.question_uuid = schema_rows[0]['question_uuid']
        self.alpha_distance = schema_rows[0]['alpha_distance']
        self.question_text = schema_rows[0]['question_text']
        self.schema_rows = schema_rows
        self.values_map = {}
        for row in schema_rows:
            answer_uuid = row['answer_uuid']
            answer_number = row['answer_number']
            self.values_map[answer_uuid] = int(answer_number)
        self.data_rows = []

    def add_data_row(self, data_row):
        # Make a copy
        data_row = dict(data_row)
        answer_uuid = data_row['answer_uuid']
        if answer_uuid in self.values_map:
            self.data_rows.append(data_row)
        else:
            raise Exception(
                "Data row with answer_uuid {} does not belong to {}."
                .format(answer_uuid, self.label)
            )

    def print_alpha_for_question(self):
        reliability_data = self.to_reliability()
        value_domain = sorted(self.values_map.values())
        k_alpha = alpha(
            reliability_data=reliability_data,
            value_domain=value_domain,
            level_of_measurement=self.alpha_distance
        )
        maximum_raters = reliability_data.shape[0]
        total_units = reliability_data.shape[1]
        print("----{}".format(self.label))
        print("{}".format(self.question_text))
        print("Units: {} Max raters: {}"
              .format(total_units, maximum_raters)
        )
        print("Krippendorff alpha for '{}' is {:.3f} Alpha distance: {} Value domain: {}"
              .format(self.label, k_alpha, self.alpha_distance, value_domain)
        )

    def to_reliability(self):
        dtype=float
        unit_dict = defaultdict(list)
        for data_row in self.data_rows:
            quiz_task_uuid = data_row['quiz_task_uuid']
            unit_dict[quiz_task_uuid].append(data_row)
        total_units = len(unit_dict)
        maximum_raters = self.seq_raters_per_unit(unit_dict)
        reliability_data = np.full((maximum_raters, total_units), np.nan, dtype=dtype)
        for column, rows in enumerate(unit_dict.values()):
            for row in rows:
                user_sequence_id = row['user_sequence_id']
                value = self.values_map[row['answer_uuid']]
                reliability_data[user_sequence_id][column] = value
        return reliability_data

    def seq_raters_per_unit(self, unit_dict):
        # Number each task run ordered by time submitted for that task.
        # Because of case numbers, rows may repeat for the same rater.
        # The answer_uuid's won't conflict so will be flattened by
        # assigning consistent rater sequence id.
        maximum_raters = 0
        for quiz_task_uuid, rows in unit_dict.items():
            rater_map = {}
            counter = 0
            sortkey = itemgetter('created')
            rows_by_date = sorted(rows, key=sortkey)
            for row in rows_by_date:
                contributor_uuid = row['contributor_uuid']
                if contributor_uuid not in rater_map:
                    rater_map[contributor_uuid] = counter
                    counter += 1
            for row in rows:
                row['user_sequence_id'] = rater_map[row['contributor_uuid']]
            maximum_raters = max(counter, maximum_raters)
        return maximum_raters


class Schema:
    def __init__(self):
        self.answer_index = {}
        self.question_index = OrderedDict()

    def add_schema_rows(self, schema_rows):
        # Need to iterate over this more than once, so copy iterator.
        schema_rows = list(schema_rows)
        # Make a lookup table so we can find an answer's schema metadata
        # Augment with derived columns.
        for schema_row in schema_rows:
            answer_uuid = schema_row['answer_uuid']
            match = ANSWER_LABEL_RE.match(schema_row['answer_label'])
            topic_number = int(match[1])
            question_number = int(match[2])
            answer_number = int(match[3])
            schema_row['topic_number'] = topic_number
            schema_row['question_number'] = question_number
            schema_row['answer_number'] = answer_number
            self.answer_index[answer_uuid] = schema_row
        # First prepare RADIO questions.
        radio_rows = filter(
            lambda x: x['question_type'] == "RADIO",
            schema_rows
        )
        sort_by_question = itemgetter('topic_number', 'question_number')
        radio_rows = sorted(radio_rows, key=sort_by_question)
        for (topic_number, question_number), rows in groupby(radio_rows, key=sort_by_question):
            radio_variable = RadioVariable(list(rows))
            lookup_key = radio_variable.question_uuid
            self.question_index[lookup_key] = radio_variable
        # TODO: Prepare CHECKBOX questions

    def add_data_rows(self, data_rows):
        data_rows = list(data_rows)
        for data_row in data_rows:
            lookup_key = self.get_data_row_key(data_row)
            if lookup_key:
                question = self.question_index[lookup_key]
                question.add_data_row(data_row)

    def get_data_row_key(self, data_row):
        answer_uuid = data_row['answer_uuid']
        schema_row = self.answer_index[answer_uuid]
        if schema_row['question_type'] == "RADIO":
            return schema_row['question_uuid']
        return None

    def print_alpha_per_question(self):
        for variable in self.question_index.values():
            variable.print_alpha_for_question()


def calculate_alphas_for_datahunt(schema_path, input_path):
    schema = load_data_hunt_schema(schema_path)
    load_data_hunt(input_path, schema)
    schema.print_alpha_per_question()

def load_data_hunt_schema(input_path):
    print("Loading schema for '{}' for Krippendorff calculation."
          .format(os.path.basename(input_path))
    )
    with closing(gunzip_if_needed(input_path)) as csv_file:
        reader = csv.DictReader(csv_file)
        schema = Schema()
        schema.add_schema_rows(reader)
    return schema

def load_data_hunt(input_path, schema):
    print("Loading '{}' for Krippendorff calculation."
          .format(os.path.basename(input_path))
    )
    with closing(gunzip_if_needed(input_path)) as csv_file:
        reader = csv.DictReader(csv_file)
        schema.add_data_rows(reader)

# Call this by passing to contextlib.closing()
def gunzip_if_needed(input_path):
    bare_filename, ext = os.path.splitext(os.path.basename(input_path))
    if ext == ".gz":
        file_handle = gzip.open(input_path, mode='rt', encoding='utf-8-sig', errors='strict')
    else:
        file_handle = open(input_path, mode='rt', encoding='utf-8-sig', errors='strict')
    return file_handle

def convert_to_int(row, key):
    row[key] = int(row[key])

def unique_raters(rows):
    raters = set()
    for row in rows:
        raters.add(row['contributor_uuid'])
    return raters

def load_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-s', '--schema-file',
        help='Schema file in CSV format for Data Hunt')
    parser.add_argument(
        '-i', '--input-file',
        help='CSV file for Data Hunt')
    parser.add_argument(
        '-o', '--output-dir',
        help='Output directory')
    parser.add_argument(
        '-m', '--minimum-redundancy',
        default=0,
        help='Create negative task runs for articles below this number of task runs.'
    )
    return parser.parse_args()

if __name__ == "__main__":
    args = load_args()
    schema_file = 'Schema.csv'
    if args.schema_file:
        schema_file = args.schema_file
    input_file = 'Highlighter.csv'
    if args.input_file:
        input_file = args.input_file
    bare_filename, ext = os.path.splitext(os.path.basename(input_file))
    if ext == ".gz":
        bare_filename, ext = os.path.splitext(os.path.basename(bare_filename))
    calculate_alphas_for_datahunt(schema_file, input_file)
