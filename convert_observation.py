import pandas as pd
import os

from utils import bundle, write_fhir_json, get_input_df, write_fhir_0_json


def obs_conversion(input_path, map_df, output_path, partition):
    filter_cols = ['OBSCLINID', 'PATID', 'ENCOUNTERID', 'OBSCLIN_PROVIDERID', 'OBSCLIN_CODE', 'OBSCLIN_TYPE',
                   'OBSCLIN_DATE', 'OBSCLIN_TIME', 'OBSCLIN_RESULT_NUM', 'OBSCLIN_RESULT_UNIT']
    input_df, subset_map_df = get_input_df("OBS_CLIN", input_path, map_df, use_cols=filter_cols)

    def map_one_row(row):
        entry = {
            "fullUrl": "https://www.hl7.org/fhir/observation.html",
            "resource": {
                "resourceType": "Observation",
                "id": row['OBSCLINID'],
                "status": "final"
            }
        }
        if not pd.isnull(row['PATID']):
            entry['resource']["subject"] = {
                "reference": "Patient/{}".format(row['PATID'])
            }
        if not pd.isnull(row['ENCOUNTERID']):
            entry['resource']["context"] = {
                "reference": "Encounter/{}".format(row['ENCOUNTERID'])
            }
        if not pd.isnull(row['OBSCLIN_PROVIDERID']):
            entry['resource']["performer"] = {
                'actor': {
                    "reference": "Practitioner/{}".format(row['OBSCLIN_PROVIDERID'])
                }
            }
        if not pd.isnull(row['OBSCLIN_RESULT_NUM']) or not pd.isnull(row['OBSCLIN_RESULT_UNIT']):
            value_dict = {
                'system': "http://unitsofmeasure.org",
            }
            if not pd.isnull(row['OBSCLIN_RESULT_NUM']):
                value_dict['value'] = row['OBSCLIN_RESULT_NUM']
            if not pd.isnull(row['OBSCLIN_RESULT_UNIT']):
                value_dict['unit'] = row['OBSCLIN_RESULT_UNIT']
            entry['resource']['valueQuantity'] = value_dict

        if not pd.isnull(row['OBSCLIN_DATE']):
            entry['resource']['valueDateTime'] = row['OBSCLIN_DATE']

        if not pd.isnull(row['OBSCLIN_TIME']):
            entry['resource']['valueTime'] = row['OBSCLIN_TIME']

        if not pd.isnull(row['OBSCLIN_CODE']):
            code_dict = {
                "code": row['OBSCLIN_CODE']
            }
            if not pd.isnull(row['OBSCLIN_TYPE']):
                code_dict['system'] = subset_map_df.loc['OBSCLIN_TYPE', row['OBSCLIN_TYPE']].at['fhir_out_cd']
            entry['resource']['code'] = {
                "coding": [code_dict]
            }

        input_fhir_entries.append(entry)
        return

    i = 0
    partition = int(partition)
    input_df_len = len(input_df)
    input_dir = os.path.join(output_path, 'Observation')
    write_fhir_0_json(input_dir)
    while i < input_df_len:
        input_fhir_entries = []
        part_input_df = input_df.iloc[i:i+partition, :]
        part_input_df.apply(lambda row: map_one_row(row), axis=1)
        part_input_df_len = len(part_input_df)
        file_name = f'{part_input_df_len}.json'
        write_fhir_json(bundle(entry=input_fhir_entries), input_dir, file_name)
        i = i + partition
    return
