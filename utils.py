import pandas as pd
import os
import json
import datetime


def bundle(entry=[]):
    return {
        "resourceType": "Bundle",
        "type": "collection",
        "entry": entry
    }


def patient_conversion(input_file, map_df, output_path, partition):

    pat_df = pd.read_csv(input_file, sep='|')
    partition = int(partition)

    def map_one_patient(row):
        entry = {
            "fullUrl": "https://www.hl7.org/fhir/patient.html",
            "resource": {
                "resourceType": "Patient",
                "id": row['PATID'],
                "extension": [
                ],
                "gender": map_df.loc['SEX', row['SEX']].at['fhir_out_cd'],
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
        mapped_race = map_df.loc['SEX', row['RACE']].at['fhir_out_cd']
        if mapped_race:
            entry['resource']['extension'].append(
                {
                    "url": "http://terminology.hl7.org/ValueSet/v3-Race",
                    "valueString": mapped_race
                })
        mapped_ethnic = map_df.loc['SEX', row['HISPANIC']].at['fhir_out_cd']
        if mapped_ethnic:
            entry['resource']['extension'].append(
                {
                    "url": "http://hl7.org/fhir/v3/Ethnicity",
                    "valueString": mapped_ethnic
                })
        pat_fhir_entries.append(entry)
        return
    i = 0
    pat_df_len = len(pat_df)
    pat_dir = os.path.join(output_path, 'Patient')
    if not os.path.exists(pat_dir):
        os.makedirs(pat_dir)
    while i < pat_df_len:
        pat_fhir_entries = []
        file_name = f'{i}.json'
        with open(os.path.join(pat_dir, file_name), 'w') as outfile:
            json.dump(bundle(), outfile, indent=4)
        part_pat_df = pat_df.loc[i:i+partition, :]
        part_pat_df.apply(lambda row: map_one_patient(row), axis=1)
        part_pat_df_len = len(part_pat_df)
        file_name = f'{part_pat_df_len}.json'
        with open(os.path.join(pat_dir, file_name), 'w') as outfile:
            json.dump(bundle(entry=pat_fhir_entries), outfile, indent=4)
        i = i + partition
    return


def mapping_pcornet_to_fhir(table_cd_file_list, output_path, partition):
    mapping_file = 'mapping/pcornet_to_fhir.csv'
    map_df = pd.read_csv(mapping_file, index_col=['table_cd', 'column_cd', 'local_in_cd'])
    supported_table_cds = map_df.index.unique(level=0)
    for table_cd_file in table_cd_file_list:
        base_name = os.path.basename(table_cd_file)
        table_cd = os.path.splitext(base_name)[0]
        if table_cd in supported_table_cds:
            # do the conversion
            subset_df = map_df.loc[table_cd, :]
            if table_cd == 'DEMOGRAPHIC':
                patient_conversion(table_cd_file, subset_df, output_path, partition)

    #print(map_df.loc['DEMOGRAPHIC', 'SEX', 'M'].at['fhir_out_cd'],
    #      map_df.loc['DEMOGRAPHIC', 'SEX', 'M'].at['fhir_out_char'],
    #      map_df.loc['DEMOGRAPHIC', 'SEX', 'M'].at['fhir_out_column'],
    #      map_df.loc['DEMOGRAPHIC', 'SEX', 'M'].at['fhir_system'])
