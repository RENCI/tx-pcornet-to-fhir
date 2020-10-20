import pandas as pd
import os

from utils import bundle, write_fhir_json, get_input_df


def patient_conversion(input_path, map_df, output_path, partition):
    pat_df, subset_map_df = get_input_df("DEMOGRAPHIC", input_path, map_df)
    address_df = pd.read_csv(os.path.join(input_path, "LDS_ADDRESS_HISTORY.csv"), sep='|', index_col=['PATID'],
                             usecols=['ADDRESSID', 'PATID', 'ADDRESS_USE', 'ADDRESS_CITY', 'ADDRESS_STATE',
                                      'ADDRESS_TYPE', 'ADDRESS_ZIP5', 'ADDRESS_ZIP9', 'ADDRESS_PERIOD_START',
                                      'ADDRESS_PERIOD_END'])
    addr_subset_map_df = map_df.loc["LDS_ADDRESS_HISTORY", :]

    # read DEATH to add to patient resource as needed
    death_df = pd.read_csv(os.path.join(input_path, "DEATH.csv"), sep='|', index_col=['PATID'],
                           usecols=['PATID', 'DEATH_SOURCE', 'DEATH_DATE'])

    def map_one_patient(row):
        pat_address_list = []

        def map_one_address(addr_row):
            addr_dict = {
                "use": addr_subset_map_df.loc['ADDRESS_USE', addr_row['ADDRESS_USE']].at['fhir_out_cd'],
                "type": addr_subset_map_df.loc['ADDRESS_TYPE', addr_row['ADDRESS_TYPE']].at['fhir_out_cd'],
                "city": addr_row['ADDRESS_CITY'],
                "postalCode": addr_row['ADDRESS_ZIP9'],
                "period": {
                    "start": addr_row['ADDRESS_PERIOD_START']
                }
            }
            if not pd.isnull(addr_row['ADDRESS_STATE']):
                addr_dict["state"] = addr_row['ADDRESS_STATE']
            if not pd.isnull(addr_row['ADDRESS_PERIOD_END']):
                addr_dict['period']["end"] = addr_row['ADDRESS_PERIOD_END']
            pat_address_list.append(addr_dict)
            return

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

        if row['PATID'] in address_df.index:
            part_addr_df = address_df.loc[row['PATID']]
            if isinstance(part_addr_df, pd.DataFrame):
                part_addr_df.apply(lambda addr_row: map_one_address(addr_row), axis=1)
            else: # it is of type Series
                map_one_address(part_addr_df)
            entry['resource']['address'] = pat_address_list

        if row['PATID'] in death_df.index:
            if not pd.isnull(death_df.loc[row['PATID']].at['DEATH_DATE']):
                entry['resource']['deceasedDateTime'] = death_df.loc[row['PATID']].at['DEATH_DATE']
            else:
                entry['resource']['deceasedBoolean'] = True

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
