import os
import csv
from collections import defaultdict
from operator import itemgetter

def split_highlighter(input_path, output_dir, batch_name):
    with open(input_path, 'r') as input_file:
        article_dict = group_by_article(input_file)
    topic_map = map_topic_names(article_dict)
    virtual_corpus_positions = cumulative_corpus_lengths(article_dict)
    user_seq_per_article(article_dict)
    remove_overlaps(article_dict)
    save_ualpha_format(article_dict, virtual_corpus_positions, output_dir, batch_name)

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
        rows_by_date = sorted(rows, key=lambda x: x['created'])
        for row in rows_by_date:
            contributor_uuid = row['contributor_uuid']
            if contributor_uuid not in article_user_map:
                article_user_map[contributor_uuid] = counter
                counter += 1
        for row in rows:
            row['user_sequence_id'] = article_user_map[row['contributor_uuid']]

def remove_overlaps(article_dict):
    for article_sha256, rows in article_dict.items():
        sortkeys = itemgetter('contributor_uuid', 'start_pos', 'end_pos')
        rows_by_user_start = sorted(rows, key=sortkeys)
        current_user = ''
        for row in rows_by_user_start:
            if current_user != row['contributor_uuid']:
                current_user = row['contributor_uuid']
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

def save_ualpha_format(article_dict, virtual_corpus_positions, output_dir, batch_name):
    output_path = os.path.join(output_dir, batch_name + ".csv")
    with open(output_path, 'w') as output_file:
        fieldnames = [
            'user_sequence_id',
            'topic_number',
            'start_pos',
            'end_pos',
        ]
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        for article_sha256, rows in article_dict.items():
            virtual_position = virtual_corpus_positions[article_sha256]
            for row in rows:
                output_row = {
                    'user_sequence_id': row['user_sequence_id'],
                    'topic_number': row['topic_number'],
                    'start_pos': virtual_position + row['start_pos'],
                    'end_pos': virtual_position + row['end_pos'],
                }
                writer.writerow(output_row)

if __name__ == "__main__":
    main_dir = "/Users/ng/Documents/CLIENTS/Thusly Inc/Customers/UT Austin-Howison/"
    in_dir_1E = os.path.join(
        main_dir,
        "Pipeline-1-SoftCite Mentions-Oct 2020/Pipeline-1E-Validation/krippendorff"
    )
    highlighter_1E = os.path.join(
        in_dir_1E,
        "Pipeline-1E-SoftciteI_fromJSON-MTurk-2020-10-30T1440-Highlighter.csv"
    )
    out_dir_1E = os.path.join(in_dir_1E,"uAlpha-format")
    split_highlighter(highlighter_1E, out_dir_1E, "Pipeline-1E-uAlpha")

    in_dir_3E = os.path.join(
        main_dir,
        "Pipeline-3-SoftCite-HEP-Mentions/krippendorff"
    )
    highlighter_3E = os.path.join(
        in_dir_3E,
        "Pipeline-3E-HEP-Mentions-MTurk-2021-01-10T0211-Highlighter.csv"
    )
    out_dir_3E = os.path.join(in_dir_3E,"uAlpha-format")


    split_highlighter(highlighter_3E, out_dir_3E, "Pipeline-3E-uAlpha")

