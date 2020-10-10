import pandas as pd
import os

from utils import bundle, write_fhir_json, get_input_df


def patient_conversion(input_path, map_df, output_path, partition):
    pat_df, subset_map_df = get_input_df("DEMOGRAPHIC", input_path, map_df)

    def map_one_patient(row):
        entry = {
            "fullUrl": "https://www.hl7.org/fhir/patient.html",
            "resource": {
                "resourceType": "Patient",
                "id": row['PATID'],
                "gender": subset_map_df.loc['SEX', row['SEX']].at['fhir_out_cd'],
                "birthDate": row['BIRTH_DATE'],
                "maritalStatus": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus"
                        }
                    ]
                }
            }
        }
        mapped_race = subset_map_df.loc['RACE', row['RACE']].at['fhir_out_cd']
        if not pd.isnull(mapped_race):
            if 'extension' not in entry['resource']:
                entry['resource']['extension'] = []
            entry['resource']['extension'].append(
                {
                    "url": "http://terminology.hl7.org/ValueSet/v3-Race",
                    "valueString": mapped_race
                })
        mapped_ethnic = subset_map_df.loc['HISPANIC', row['HISPANIC']].at['fhir_out_cd']
        if not pd.isnull(mapped_ethnic):
            if 'extension' not in entry['resource']:
                entry['resource']['extension'] = []
            entry['resource']['extension'].append(
                {
                    "url": "http://hl7.org/fhir/v3/Ethnicity",
                    "valueString": mapped_ethnic
                })
        pat_fhir_entries.append(entry)
        return

    i = 0
    partition = int(partition)
    pat_df_len = len(pat_df)
    pat_dir = os.path.join(output_path, 'Patient')

    while i < pat_df_len:
        pat_fhir_entries = []
        file_name = f'{i}.json'
        write_fhir_json(bundle(), pat_dir, file_name)
        part_pat_df = pat_df.loc[i:i+partition, :]
        part_pat_df.apply(lambda row: map_one_patient(row), axis=1)
        part_pat_df_len = len(part_pat_df)
        file_name = f'{part_pat_df_len}.json'
        write_fhir_json(bundle(entry=pat_fhir_entries), pat_dir, file_name)
        i = i + partition
    return
