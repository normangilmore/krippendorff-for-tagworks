import os
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import gzip
from contextlib import closing
import csv
from collections import defaultdict
from operator import itemgetter
from itertools import groupby, chain
import numpy as np
from krippendorff import alpha

def split_highlighter(input_path, output_dir, batch_name):
    with closing(gunzip_if_needed(input_path)) as csv_file:
        article_dict = group_by_article(csv_file)
    print("Loading '{}' for Krippendorff calculation.".format(os.path.basename(input_path)))
    topic_map = map_topic_names(article_dict)
    remove_if_not_pairable(article_dict)
    cumulative_length, virtual_corpus_positions = cumulative_corpus_lengths(article_dict)
    print("Article count: {}. Corpus character length: {}.".format(len(article_dict), cumulative_length))
    maximum_raters = user_seq_per_article(article_dict)
    print("Maximum raters for an article: {}".format(maximum_raters))
    remove_overlaps(article_dict, show_trims=True)
    output_separate_topics(
        article_dict, maximum_raters, cumulative_length, virtual_corpus_positions,
        output_dir, batch_name
    )

# Call this by passing to contextlib.closing()
def gunzip_if_needed(input_path):
    bare_filename, ext = os.path.splitext(os.path.basename(input_path))
    if ext == ".gz":
        file_handle = gzip.open(input_path, mode='rt', encoding='utf-8-sig', errors='strict')
    else:
        file_handle = open(input_path, mode='rt', encoding='utf-8-sig', errors='strict')
    return file_handle

def group_by_article(input_file):
    article_dict = defaultdict(list)
    reader = csv.DictReader(input_file)
    for row in reader:
        fresh_row = dict(row)
        convert_to_int(fresh_row, 'article_text_length')
        convert_to_int(fresh_row, 'start_pos')
        convert_to_int(fresh_row, 'end_pos')
        article_sha256 = fresh_row['article_sha256']
        article_dict[article_sha256 ].append(fresh_row)
    return article_dict

def convert_to_int(row, key):
    row[key] = int(row[key])

def map_topic_names(article_dict):
    topic_names = set()
    for article_sha256, rows in article_dict.items():
        for row in rows:
            topic_names.add(row['topic_name'])
    topic_map = dict(zip(topic_names, range(len(topic_names))))
    for article_sha256, rows in article_dict.items():
        for row in rows:
            row['topic_number'] = topic_map[row['topic_name']]
    return topic_map

def remove_if_not_pairable(article_dict):
    removed_articles = set()
    for article_sha256, rows in list(article_dict.items()):
        raters = set()
        for row in rows:
            raters.add(row['contributor_uuid'])
        if len(raters) < 2:
            removed_articles.add(article_sha256)
            del article_dict[article_sha256]
    print("Removing {} articles with less than two raters.".format(len(removed_articles)))
    return removed_articles

def cumulative_corpus_lengths(article_dict):
    virtual_corpus_positions = {}
    cumulative_length = 0
    for article_sha256 in article_dict.keys():
        virtual_corpus_positions[article_sha256] = cumulative_length
        cumulative_length += article_dict[article_sha256][0]['article_text_length']
    return cumulative_length, virtual_corpus_positions

def user_seq_per_article(article_dict):
    maximum_raters = 0
    for article_sha256, rows in article_dict.items():
        article_user_map = {}
        counter = 0
        sortkey = itemgetter('created')
        rows_by_date = sorted(rows, key=sortkey)
        for row in rows_by_date:
            contributor_uuid = row['contributor_uuid']
            if contributor_uuid not in article_user_map:
                article_user_map[contributor_uuid] = counter
                counter += 1
        for row in rows:
            row['user_sequence_id'] = article_user_map[row['contributor_uuid']]
        maximum_raters = max(counter, maximum_raters)
    return maximum_raters

def remove_overlaps(article_dict, show_trims=True):
    sortkeys = itemgetter('article_sha256', 'contributor_uuid', 'topic_name')
    for article_rows in article_dict.values():
        grouped_rows = sorted(article_rows, key=sortkeys)
        for (article_sha256, contributor_uuid, topic_name), mergeable_rows in groupby(grouped_rows, key=sortkeys):
            first_row = True
            sort_by_pos = itemgetter('start_pos', 'end_pos')
            for row in sorted(mergeable_rows, key=sort_by_pos):
                if first_row:
                    first_row = False
                    max_pos = row['end_pos']
                    continue
                trimmed = False
                initial = ("{}:{}".format(row['start_pos'], row['end_pos']))
                if row['start_pos'] < max_pos:
                    row['start_pos'] = max_pos
                    trimmed = True
                if row['end_pos'] < max_pos:
                    row['end_pos'] = max_pos
                    trimmed = True
                if trimmed and show_trims:
                    print("{} trimmed to {}:{}".format(initial, row['start_pos'], row['end_pos']))
                max_pos = max(row['end_pos'], max_pos)

def output_separate_topics(
        article_dict, maximum_raters, cumulative_length, virtual_corpus_positions,
        output_dir=None, batch_name=None
    ):
    sort_by_topic = itemgetter('topic_name')
    sorted_rows = sorted(chain.from_iterable(article_dict.values()), key=sort_by_topic)
    for topic_name, rows in groupby(sorted_rows, key=sort_by_topic):
        print_alpha_for_topic(topic_name, rows, maximum_raters, cumulative_length, virtual_corpus_positions)
        if output_dir and batch_name:
            out_filename = batch_name.format(topic_name)
            print("Saving topic '{}' to '{}'".format(topic_name, out_filename))
            save_ualpha_format(rows, virtual_corpus_positions, output_dir, out_filename)

def print_alpha_for_topic(topic_name, rows, maximum_raters, cumulative_length, virtual_corpus_positions):
    dtype=float
    reliability_data = np.full((maximum_raters, cumulative_length), np.nan, dtype=dtype)
    for row_count, output_row in output_generator(rows, virtual_corpus_positions):
        start_pos = output_row['start_pos']
        end_pos = output_row['end_pos']
        user_sequence_id = output_row['user_sequence_id']
        topic_number = output_row['topic_number']
        reliability_data[user_sequence_id][start_pos:end_pos] = dtype(topic_number)
    k_alpha = alpha(reliability_data=reliability_data, level_of_measurement='nominal')
    print("Krippendorff alpha is {:.3f} for '{}'".format(k_alpha, topic_name))

def save_ualpha_format(rows, virtual_corpus_positions, output_dir, out_filename):
    fieldnames = [
        'row_label',
        'user_sequence_id',
        'topic_number',
        'empty_col',
        'start_pos',
        'end_pos',
    ]
    output_path = os.path.join(output_dir, out_filename + ".csv")
    with open(output_path, 'w') as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        for row_count, output_row in output_generator(rows, virtual_corpus_positions):
            writer.writerow(output_row)

def output_generator(rows, virtual_corpus_positions):
    sort_by_article = itemgetter('article_sha256')
    sorted_rows = sorted(rows, key=sort_by_article)
    sort_by_rater = itemgetter('user_sequence_id')
    sort_by_pos = itemgetter('start_pos', 'end_pos')
    row_count = 0
    skipped_articles = set()
    for article_sha256, article_rows_sorted in groupby(sorted_rows, key=sort_by_article):
        rows_by_rater = sorted(article_rows_sorted, key=sort_by_rater)
        raters = unique_raters(rows_by_rater)
        if len(raters) < 2:
            skipped_articles.add(article_sha256)
            continue
        for user_sequence_id, taskrun_rows_sorted  in groupby(rows_by_rater, key=sort_by_rater):
            rows_by_pos = sorted(taskrun_rows_sorted, key=sort_by_pos)
            virtual_position = virtual_corpus_positions[article_sha256]
            current_pos = 0
            for row in rows_by_pos:
                if current_pos < row['start_pos']:
                    negative_highlight = {
                        'row_label': "u{}".format(row_count),
                        'user_sequence_id': row['user_sequence_id'],
                        'topic_number': 9999,
                        'empty_col': '',
                        'start_pos': virtual_position + current_pos,
                        'end_pos': virtual_position + row['start_pos'],
                    }
                    yield row_count, negative_highlight
                    row_count += 1
                output_row = {
                    'row_label': "u{}".format(row_count),
                    'user_sequence_id': row['user_sequence_id'],
                    'topic_number': row['topic_number'],
                    'empty_col': '',
                    'start_pos': virtual_position + row['start_pos'],
                    'end_pos': virtual_position + row['end_pos'],
                }
                yield row_count, output_row
                row_count += 1
                current_pos = row['end_pos']
            article_text_length = row['article_text_length']
            if current_pos < article_text_length:
                negative_highlight = {
                    'row_label': "u{}".format(row_count),
                    'user_sequence_id': row['user_sequence_id'],
                    'topic_number': 9999,
                    'empty_col': '',
                    'start_pos': virtual_position + current_pos,
                    'end_pos': virtual_position + article_text_length,
                }
                yield row_count, negative_highlight
                row_count += 1
                current_pos = article_text_length
        if len(skipped_articles):
            print("Skipped {} articles with less than two raters.".format(len(skipped_articles)))

def unique_raters(rows):
    raters = set()
    for row in rows:
        raters.add(row['contributor_uuid'])
    return raters
