import pandas as pd
import os

from utils import bundle, write_fhir_json, get_input_df, write_fhir_0_json


def vital_conversion(input_path, map_df, output_path, partition):
    def map_one_vital(row):
        entry = {
            "fullUrl": "https://www.hl7.org/fhir/vital.html",
            "resource": {
                "resourceType": "Observation",
                "id": row['VITALID'],
                "status": 'final'
              }
        }
        if not pd.isnull(row['MEASURE_DATE']):
            entry['resource']["effectiveDateTime"] = row['MEASURE_DATE']
        if not pd.isnull(row['PATID']):
            entry['resource']["subject"] = {
                "reference": "Patient/{}".format(row['PATID'])
            }
        if not pd.isnull(row['ENCOUNTERID']):
            entry['resource']["context"] = {
                "reference": "Encounter/{}".format(row['ENCOUNTERID'])
            }

        if not pd.isna(row['SMOKING']) and not pd.isnull(row['SMOKING']):
            cat_code_dict = {
                'system': "http://hl7.org/fhir/ValueSet/observation-category",
                'code': 'social-history',
                "display": 'Social History'
            }
            cat_dict = {
                'coding': [cat_code_dict]
            }
            entry['resource']['category'] = [cat_dict]
            coding_dict = {
                "system": 'http://snomed.info/sct/',
                "code": subset_map_df.loc['SMOKING', row['SMOKING']].at['fhir_out_cd'],
                "display": subset_map_df.loc['SMOKING', row['SMOKING']].at['fhir_out_char']
            }
            entry['resource']['code'] = {
                'coding': [coding_dict]
            }
        elif not row['TOBACCO']:
            entry['resource']['valueQuantity'] = []
            cat_code_dict = {
                'system': "http://hl7.org/fhir/ValueSet/observation-category",
                'code': 'vital-signs',
                "display": 'Vital Signs'
            }
            cat_dict = {
                'coding': [cat_code_dict]
            }
            entry['resource']['category'] = [cat_dict]
            code_code_array = []
            if row['HT']:
                entry['resource']['valueQuantity'].append({
                    'system': 'https://unitsofmeasure.org',
                    'value': row['HT'],
                    'code': '[in_i]'
                })

                coding_dict = {
                    "system": 'http://loinc.org/',
                    "code": '8302-2',
                    "display": 'Body height'
                }
                code_code_array.append(coding_dict)
            if row['WT']:
                entry['resource']['valueQuantity'].append({
                    'system': 'https://unitsofmeasure.org',
                    'value': row['WT'],
                    'code': '[lb_av]'
                })
                coding_dict = {
                    "system": 'http://loinc.org/',
                    "code": '29463-7',
                    "display": 'Body weight'
                }
                code_code_array.append(coding_dict)
            if row['SYSTOLIC']:
                entry['resource']['valueQuantity'].append({
                    'system': 'https://unitsofmeasure.org',
                    'value': row['SYSTOLIC'],
                    'code': 'mm[Hg]'
                })
                coding_dict = {
                    "system": 'http://loinc.org/',
                    "code": '8480-6',
                    "display": 'Systolic blood pressure'
                }
                code_code_array.append(coding_dict)
            if row['DIASTOLIC']:
                entry['resource']['valueQuantity'].append({
                    'system': 'https://unitsofmeasure.org',
                    'value': row['DIASTOLIC'],
                    'code': 'mm[Hg]'
                })
                coding_dict = {
                    "system": 'http://loinc.org/',
                    "code": '8462-4',
                    "display": 'Diastolic blood pressure'
                }
                code_code_array.append(coding_dict)
            if row['ORIGINAL_BMI']:
                entry['resource']['valueQuantity'].append({
                    'system': 'https://unitsofmeasure.org',
                    'value': row['ORIGINAL_BMI'],
                    'code': 'kg/m2'
                })
                coding_dict = {
                    "system": 'http://loinc.org/',
                    "code": '36156-5',
                    "display": 'Body mass index'
                }
                code_code_array.append(coding_dict)

            entry['resource']['code'] = {
                'coding': code_code_array
            }

        vital_fhir_entries.append(entry)
        return

    filter_cols = ["VITALID", "PATID", "ENCOUNTERID", "HT", "SYSTOLIC", "MEASURE_DATE", "SMOKING", "TOBACCO",
                   "DIASTOLIC", "ORIGINAL_BMI", "WT"]
    input_df, subset_map_df = get_input_df("VITAL", input_path, map_df, use_cols=filter_cols)
    i = 0
    partition = int(partition)
    input_df_len = len(input_df)

    vital_dir = os.path.join(output_path, 'Vital')
    write_fhir_0_json(vital_dir)

    while i < input_df_len:
        vital_fhir_entries = []

        part_pat_df = input_df.iloc[i:i+partition, :]

        part_pat_df.apply(lambda row: map_one_vital(row), axis=1)
        part_pat_df_len = len(part_pat_df)
        file_name = f'{part_pat_df_len}.json'
        write_fhir_json(bundle(entry=vital_fhir_entries), vital_dir, file_name)
        i = i + partition
    return
