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


def write_fhir_json(content, input_path, input_file_name):
    if not os.path.exists(input_path):
        os.makedirs(input_path)
    with open(os.path.join(input_path, input_file_name), 'w') as outfile:
        json.dump(content, outfile, indent=4)


def get_input_df(table_cd, input_path, map_df):
    input_file_name = f'{table_cd}.csv'
    input_file = os.path.join(input_path, input_file_name)
    subset_map_df = map_df.loc[table_cd, :]
    input_df = pd.read_csv(input_file, sep='|')
    return input_df, subset_map_df


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


def medicationrequest_conversion(input_path, map_df, output_path, partition):
    prescribe_df, prescribe_subset_map_df = get_input_df("PRESCRIBING", input_path, map_df)

    def map_one_medicationrequest(row):
        entry = {
            "fullUrl": "https://www.hl7.org/fhir/medicationrequest.html",
            "resource": {
              "resourceType": "MedicationRequest",
              "id": row["PRESCRIBINGID"],
              "intent": "order",
              "medicationCodeableConcept": {
                "coding": [
                  {
                    "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
                    "code": str(row["RXNORM_CUI"]),
                    "display": str(row["RAW_RX_MED_NAME"])
                  }
                ]
              },
              "subject": {
                "reference": "Patient/{}".format(row["PATID"])
              },
              "authoredOn": str(row["RX_ORDER_DATE"]),
              "requester": {
                "agent": {
                  "reference": "Practitioner/{}".format(row["RX_PROVIDERID"])
                }
              },
              "dosageInstruction": [
                {
                  "text": prescribe_subset_map_df.loc['RX_FREQUENCY', row['RX_FREQUENCY']].at['fhir_out_cd'],
                  "asNeededBoolean": prescribe_subset_map_df.loc['RX_PRN_FLAG', row['RX_PRN_FLAG']].at['fhir_out_cd']
                  if not pd.isnull(row['RX_PRN_FLAG']) else 'null',
                  "route": {
                    "coding": [
                      {
                        "code": str(row["RX_ROUTE"])
                      }
                    ]
                  },
                  "doseQuantity": {
                    "value": row["RX_DOSE_ORDERED"],
                    "unit": row["RX_DOSE_ORDERED_UNIT"]
                  }
                }
              ],
              "dispenseRequest": {
                "validityPeriod": {
                  "start": row["RX_START_DATE"],
                  "end": row["RX_END_DATE"]
                }
              },
              "substitution": {
                "allowed": prescribe_subset_map_df.loc['RX_DISPENSE_AS_WRITTEN',
                                                       row['RX_DISPENSE_AS_WRITTEN']].at['fhir_out_cd']
              }
            }
        }
        prescribe_fhir_entries.append(entry)
        return

    i = 0
    partition = int(partition)
    mr_dir = os.path.join(output_path, 'MedicationRequest')

    prescribe_df_len = len(prescribe_df)
    while i < prescribe_df_len:
        prescribe_fhir_entries = []
        file_name = f'{i}.json'
        write_fhir_json(bundle(), mr_dir, file_name)
        part_prescribe_df = prescribe_df.loc[i:i + partition, :]
        part_prescribe_df.apply(lambda row: map_one_medicationrequest(row), axis=1)
        part_prescribe_df_len = len(part_prescribe_df)
        file_name = f'{part_prescribe_df_len}.json'
        write_fhir_json(bundle(entry=prescribe_fhir_entries), mr_dir, file_name)
        i = i + partition
    return


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
    dx_df = pd.read_csv(os.path.join(input_path, "DIAGNOSIS.csv"), sep='|')
    filter_dx_df_col = dx_df.filter(items=['DIAGNOSISID', 'ENCOUNTERID', 'PDX', 'DX_SOURCE'])
    filter_dx_df = filter_dx_df_col.loc[(filter_dx_df_col['PDX'] == 'P')
                                        & (filter_dx_df_col['DX_SOURCE'] == 'DI')]
    join_df = pd.merge(input_df, filter_dx_df, how='left', left_on=['ENCOUNTERID'], right_on=['ENCOUNTERID'])
    i = 0
    partition = int(partition)
    join_df_len = len(join_df)
    enc_dir = os.path.join(output_path, 'Encounter')

    while i < join_df_len:
        enc_fhir_entries = []
        file_name = f'{i}.json'
        write_fhir_json(bundle(), enc_dir, file_name)
        part_pat_df = join_df.loc[i:i+partition, :]
        part_pat_df.apply(lambda row: map_one_encounter(row), axis=1)
        part_pat_df_len = len(part_pat_df)
        file_name = f'{part_pat_df_len}.json'
        write_fhir_json(bundle(entry=enc_fhir_entries), enc_dir, file_name)
        i = i + partition
    return
