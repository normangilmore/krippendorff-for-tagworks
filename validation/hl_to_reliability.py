import os
import csv
from collections import defaultdict
from operator import itemgetter
from itertools import groupby, chain

def split_highlighter(input_path, output_dir, batch_name):
    with open(input_path, 'r') as input_file:
        article_dict = group_by_article(input_file)
    topic_map = map_topic_names(article_dict)
    remove_if_not_pairable(article_dict)
    virtual_corpus_positions = cumulative_corpus_lengths(article_dict)
    user_seq_per_article(article_dict)
    remove_overlaps(article_dict)
    output_separate_topics(article_dict, virtual_corpus_positions, output_dir, batch_name)

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
    return virtual_corpus_positions

def user_seq_per_article(article_dict):
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

def remove_overlaps(article_dict):
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
                if trimmed:
                    print("{} trimmed to {}:{}".format(initial, row['start_pos'], row['end_pos']))
                max_pos = max(row['end_pos'], max_pos)

def output_separate_topics(article_dict, virtual_corpus_positions, output_dir, batch_name):
    sort_by_topic = itemgetter('topic_name')
    sorted_rows = sorted(chain.from_iterable(article_dict.values()), key=sort_by_topic)
    for topic_name, rows in groupby(sorted_rows, key=sort_by_topic):
        out_filename = batch_name.format(topic_name)
        save_ualpha_format(rows, virtual_corpus_positions, output_dir, out_filename)

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
        sort_by_pos = itemgetter('start_pos', 'end_pos')
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        sortkeys = itemgetter('article_sha256', 'user_sequence_id')
        sorted_rows = sorted(rows, key=sortkeys)
        row_count = 0
        for (article_sha256, user_sequence_id), taskrun_rows in groupby(sorted_rows, key=sortkeys):
            taskrun_rows_sorted = sorted(taskrun_rows, key=sort_by_pos)
            raters = unique_raters(taskrun_rows_sorted)
            if len(raters) < 2:
                print("Skipping {} with {} raters".format(article_sha256, len(raters)))
                continue
            virtual_position = virtual_corpus_positions[article_sha256]
            current_pos = 0
            for row in taskrun_rows_sorted:
                if current_pos < row['start_pos']:
                    negative_highlight = {
                        'row_label': "u{}".format(row_count),
                        'user_sequence_id': row['user_sequence_id'],
                        'topic_number': 9999,
                        'empty_col': '',
                        'start_pos': virtual_position + current_pos,
                        'end_pos': virtual_position + row['start_pos'],
                    }
                    writer.writerow(negative_highlight)
                    row_count += 1
                output_row = {
                    'row_label': "u{}".format(row_count),
                    'user_sequence_id': row['user_sequence_id'],
                    'topic_number': row['topic_number'],
                    'empty_col': '',
                    'start_pos': virtual_position + row['start_pos'],
                    'end_pos': virtual_position + row['end_pos'],
                }
                writer.writerow(output_row)
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
                writer.writerow(negative_highlight)
                row_count += 1
                current_pos = article_text_length

def unique_raters(rows):
    raters = set()
    for row in rows:
        raters.add(row['contributor_uuid'])
    return raters
