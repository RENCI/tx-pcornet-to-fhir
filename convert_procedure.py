import pandas as pd
import os

from utils import bundle, write_fhir_json, write_fhir_0_json


def procedure_conversion(input_path, output_path, partition):
    filter_cols = ['PROCEDURESID', 'PATID', 'ENCOUNTERID', 'PX_TYPE', 'PX', 'PX_DATE', 'PROVIDERID']
    input_df = pd.read_csv(os.path.join(input_path, 'PROCEDURES.csv'), sep='|', usecols=filter_cols)
    type_to_url = {
        '09': 'http://hl7.org/fhir/sid/icd-9-cm/',
        '10': 'http://hl7.org/fhir/sid/icd-10-cm/',
        '11': 'http://hl7.org/fhir/sid/icd-11-cm/',
        'CH': 'http://www.ama-assn.org/go/cpt/',
        'LC': 'http://loinc.org/',
        'ND': 'http://hl7.org/fhir/sid/ndc/'
    }
    def map_one_row(row):
        entry = {
            "fullUrl": "https://www.hl7.org/fhir/procedure.html",
            "resource": {
                "resourceType": "Procedure",
                "id": row['PROCEDURESID'],
                "status": "unknown",
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
        if not pd.isnull(row['PROVIDERID']):
            entry['resource']["performer"] = {
                'actor': {
                    "reference": "Practitioner/{}".format(row['PROVIDERID'])
                }
            }
        if not pd.isnull(row['PX']) and not pd.isnull(row['PX_TYPE']):
            coding_dict = {
                'system': type_to_url[row['PX_TYPE']] if row['PX_TYPE'] in
                                                         type_to_url else '',
                'code': row['PX']
            }
            code_dict = {
                'coding': [coding_dict]
            }
            entry['resource']['code'] = code_dict

        if not pd.isnull(row['PX_DATE']):
            entry['resource']['performed'] = {
                'performedDateTime': row['PX_DATE']
            }

        input_fhir_entries.append(entry)
        return

    i = 0
    partition = int(partition)
    input_df_len = len(input_df)
    input_dir = os.path.join(output_path, 'Procedure')
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
