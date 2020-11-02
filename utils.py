import pandas as pd
import os
import json


def bundle(entry=None):
    fhir_bundle = {
        "resourceType": "Bundle",
        "type": "collection"
    }
    if entry:
        fhir_bundle["entry"] = entry
    return fhir_bundle


def write_fhir_0_json(input_dir):
    file_name = '0.json'
    write_fhir_json(bundle(), input_dir, file_name)


def write_fhir_json(content, input_path, input_file_name):
    if not os.path.exists(input_path):
        os.makedirs(input_path)
    with open(os.path.join(input_path, input_file_name), 'w') as outfile:
        json.dump(content, outfile, indent=4)


def get_input_df(table_cd, input_path, map_df, use_cols=None):
    input_file_name = f'{table_cd}.csv'
    input_file = os.path.join(input_path, input_file_name)
    subset_map_df = map_df.loc[table_cd, :]

    if use_cols:
        input_df = pd.read_csv(input_file, sep='|', usecols=use_cols)
    else:
        input_df = pd.read_csv(input_file, sep='|')
    return input_df, subset_map_df
