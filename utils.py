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


def write_fhir_json(content, input_file_path):
    with open(input_file_path, 'w') as outfile:
        json.dump(content, outfile, indent=4)


def patient_conversion(input_path, map_df, output_path, partition):
    table_cd = "DEMOGRAPHIC"
    input_file_name = f'{table_cd}.csv'
    input_file = os.path.join(input_path, input_file_name)
    subset_map_df = map_df.loc[table_cd, :]
    pat_df = pd.read_csv(input_file, sep='|')
    partition = int(partition)

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
    pat_df_len = len(pat_df)
    pat_dir = os.path.join(output_path, 'Patient')
    if not os.path.exists(pat_dir):
        os.makedirs(pat_dir)
    while i < pat_df_len:
        pat_fhir_entries = []
        file_name = f'{i}.json'
        write_fhir_json(bundle(), os.path.join(pat_dir, file_name))
        part_pat_df = pat_df.loc[i:i+partition, :]
        part_pat_df.apply(lambda row: map_one_patient(row), axis=1)
        part_pat_df_len = len(part_pat_df)
        file_name = f'{part_pat_df_len}.json'
        write_fhir_json(bundle(entry=pat_fhir_entries), os.path.join(pat_dir, file_name))
        i = i + partition
    return


def medicationrequest_conversion(input_path, map_df, output_path, partition):
    prescribe_table = "PRESCRIBING"
    prescribe_input_file_name = f'{prescribe_table}.csv'
    prescribe_input_file = os.path.join(input_path, prescribe_input_file_name)
    prescribe_subset_map_df = map_df.loc[prescribe_table, :]
    prescribe_df = pd.read_csv(prescribe_input_file, sep='|')
    partition = int(partition)

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
    mr_dir = os.path.join(output_path, 'MedicationRequest')
    if not os.path.exists(mr_dir):
        os.makedirs(mr_dir)

    prescribe_df_len = len(prescribe_df)
    while i < prescribe_df_len:
        prescribe_fhir_entries = []
        file_name = f'{i}.json'
        write_fhir_json(bundle(), os.path.join(mr_dir, file_name))
        part_prescribe_df = prescribe_df.loc[i:i + partition, :]
        part_prescribe_df.apply(lambda row: map_one_medicationrequest(row), axis=1)
        part_prescribe_df_len = len(part_prescribe_df)
        file_name = f'{part_prescribe_df_len}.json'
        write_fhir_json(bundle(entry=prescribe_fhir_entries), os.path.join(mr_dir, file_name))
        i = i + partition
    return
