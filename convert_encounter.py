import pandas as pd
import os

from utils import bundle, write_fhir_json, get_input_df,write_fhir_0_json, get_partition_file_name


def encounter_conversion(input_path, map_df, output_path, partition):
    def map_one_encounter(row):
        entry = {
            "fullUrl": "https://www.hl7.org/fhir/encounter.html",
            "resource": {
                "resourceType": "Encounter",
                "id": row["ENCOUNTERID"],
                "subject": {
                    "reference": "Patient/{}".format(row["PATID"])
                },
                "participant": [
                    {
                        "individual": {
                            "display": "Practitioner/{}".format(row['PROVIDERID'])
                        }
                    }
                ],
                "period": {
                    "start": row['ADMIT_DATE'],
                    "end": row['DISCHARGE_DATE']
                },
            }
        }

        if str(row['ENC_TYPE']).strip() and not pd.isnull(row['ENC_TYPE']):
            mapped_code = subset_map_df.loc['ENC_TYPE', row['ENC_TYPE']].at['fhir_out_cd']
            if not pd.isnull(mapped_code):
                entry['resource']['class'] = {
                    "system": "http://hl7.org/fhir/v3/ActCode",
                    "code": mapped_code,
                    "display": subset_map_df.loc['ENC_TYPE', row['ENC_TYPE']].at['fhir_out_char']
                }
        if not pd.isnull(row['ADMITTING_SOURCE']):
            if 'hospitalization' not in entry['resource']:
                entry['resource']['hospitalization'] = {}
            entry['resource']['hospitalization']['admitSource'] = {
                        "text": subset_map_df.loc['ADMITTING_SOURCE', row['ADMITTING_SOURCE']].at['fhir_out_char']
            }
        if not pd.isnull(row['DISCHARGE_STATUS']):
            if 'hospitalization' not in entry['resource']:
                entry['resource']['hospitalization'] = {}
            entry['resource']['hospitalization']['dischargeDisposition'] = {
                "coding": [
                    {
                        "system": "http://hl7.org/fhir/discharge-disposition",
                        "code": subset_map_df.loc['DISCHARGE_STATUS',
                                                  row['DISCHARGE_STATUS']].at['fhir_out_cd'],
                        "display": subset_map_df.loc['DISCHARGE_STATUS',
                                                     row['DISCHARGE_STATUS']].at['fhir_out_char']
                    }
                ]
            }
        if not pd.isnull(row['DIAGNOSISID']):
            entry['resource']["diagnosis"] = [
                {
                    "condition": {
                        "reference": "Condition/{}".format(row['DIAGNOSISID'])
                    },
                    "role": {
                        "coding": [
                            {
                                "system": "http://hl7.org/fhir/diagnosis-role",
                                "code": "DD"
                            }
                        ]
                    },
                    "rank": 1
                }
            ]

        enc_fhir_entries.append(entry)
        return

    input_df, subset_map_df = get_input_df("ENCOUNTER", input_path, map_df)
    dx_df = pd.read_csv(os.path.join(input_path, "DIAGNOSIS.csv"), sep='|',
                        usecols=['DIAGNOSISID', 'ENCOUNTERID', 'PDX', 'DX_SOURCE'])
    filter_dx_df = dx_df.loc[(dx_df['PDX'] == 'P') & (dx_df['DX_SOURCE'] == 'DI')]
    join_df = pd.merge(input_df, filter_dx_df, how='left', left_on=['ENCOUNTERID'], right_on=['ENCOUNTERID'])
    i = 0
    partition = int(partition)
    join_df_len = len(join_df)
    enc_dir = os.path.join(output_path, 'Encounter')
    write_fhir_0_json(enc_dir)
    while i < join_df_len:
        enc_fhir_entries = []
        part_pat_df = join_df.iloc[i:i+partition, :]
        part_pat_df.apply(lambda row: map_one_encounter(row), axis=1)
        part_pat_df_len = len(part_pat_df)
        file_name = get_partition_file_name(partition, part_pat_df_len, i)
        write_fhir_json(bundle(entry=enc_fhir_entries), enc_dir, file_name)
        i = i + partition
    return
