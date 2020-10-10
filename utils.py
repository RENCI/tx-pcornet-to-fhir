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


def get_input_df(table_cd, input_path, map_df, use_cols=None):
    input_file_name = f'{table_cd}.csv'
    input_file = os.path.join(input_path, input_file_name)
    subset_map_df = map_df.loc[table_cd, :]

    if use_cols:
        input_df = pd.read_csv(input_file, sep='|', usecols=use_cols)
    else:
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
                  if not pd.isnull(row['RX_PRN_FLAG']) else None,
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
    dx_df = pd.read_csv(os.path.join(input_path, "DIAGNOSIS.csv"), sep='|',
                        usecols=['DIAGNOSISID', 'ENCOUNTERID', 'PDX', 'DX_SOURCE'])
    filter_dx_df = dx_df.loc[(dx_df['PDX'] == 'P') & (dx_df['DX_SOURCE'] == 'DI')]
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


def condition_conversion(input_path, map_df, output_path, partition):
    def map_one_condition(row):
        entry = {
            "fullUrl": "https://www.hl7.org/fhir/condition.html",
            "resource": {
                "resourceType": "Condition",
                "id": row['CON_IDENTIFIER'],
                "clinicalStatus": row['CON_CLINSTATUS'],
                "subject": {
                    "reference": "Patient/{}".format(row['CON_SUBJECT_REFERENCE'])
                },
                "context": {
                    "reference": "Encounter/{}".format(row['CON_CONTEXT_REFERENCE'])
                }
            }
        }
        if not pd.isnull(row['CON_CATEGORY_CODING_SYST']) or not pd.isnull(row['CON_CATEGORY_CODING_CODE']):
            cat_dict = {}
            code_dict = {}
            if not pd.isnull(row['CON_CATEGORY_CODING_SYST']):
                code_dict['system'] = row['CON_CATEGORY_CODING_SYST']
            if not pd.isnull(row['CON_CATEGORY_CODING_CODE']):
                code_dict['code'] = row['CON_CATEGORY_CODING_CODE']
            cat_dict['coding'] = [code_dict]
            entry['resource']['category'] = [cat_dict]

        if not pd.isnull(row['CON_CODE_CODING_SYST']) or not pd.isnull(row['CON_CODE_CODING_CODE']):
            coding_dict = {}
            code_dict = {}
            if not pd.isnull(row['CON_CODE_CODING_SYST']):
                coding_dict['system'] = row['CON_CODE_CODING_SYST']
            if not pd.isnull(row['CON_CODE_CODING_CODE']):
                coding_dict['code'] = row['CON_CODE_CODING_CODE']
            code_dict['coding'] = [coding_dict]
            entry['resource']['code'] = code_dict

        if not pd.isnull(row['CON_ASSERTER_REFERENCE']):
            entry['resource']['asserter'] = {
                "reference": "Practitioner/{}".format(row['CON_ASSERTER_REFERENCE'])
            }

        if not pd.isnull(row['CON_ASSERT_DATE']):
            entry['resource']['assertedDate'] = row['CON_ASSERT_DATE']

        if not pd.isnull(row['CON_ABATEDATE']):
            entry['resource']['abatementDateTime'] = row['CON_ABATEDATE']

        if not pd.isnull(row['CON_ONSETDATE']):
            entry['resource']['onsetDateTime'] = row['CON_ONSETDATE']

        cond_fhir_entries.append(entry)
        return

    filter_cols = ["CONDITIONID", "PATID", "ENCOUNTERID", "CONDITION_TYPE", "CONDITION", "REPORT_DATE",
                    "RESOLVE_DATE", "ONSET_DATE", 'CONDITION_STATUS', 'CONDITION_SOURCE']
    input_df, subset_map_df = get_input_df("CONDITION", input_path, map_df, use_cols=filter_cols)
    input_df = input_df.loc[input_df['CONDITION_SOURCE'] == 'HC']
    input_df.drop(columns=["CONDITION_SOURCE"])
    input_df.loc[(input_df.CONDITION_TYPE != 'HP') &
                 (input_df.CONDITION_TYPE != 'SM') &
                 (input_df.CONDITION_TYPE != '09') &
                 (input_df.CONDITION_TYPE != '10') &
                 (input_df.CONDITION_TYPE != '11'), 'CONDITION_TYPE'] = None
    input_df.loc[(input_df.CONDITION_TYPE == '09'), 'CONDITION_TYPE'] = 'http://hl7.org/fhir/sid/icd-9-cm'
    input_df.loc[(input_df.CONDITION_TYPE == '10'), 'CONDITION_TYPE'] = 'http://hl7.org/fhir/sid/icd-10-cm'
    input_df.loc[(input_df.CONDITION_TYPE == '11'), 'CONDITION_TYPE'] = 'http://hl7.org/fhir/sid/icd-11-cm'
    input_df.loc[(input_df.CONDITION_TYPE == 'SM'), 'CONDITION_TYPE'] = 'http://snomed.info/sct'
    input_df.loc[(input_df.CONDITION_TYPE == 'HP'), 'CONDITION_TYPE'] = 'https://hpo.jax.org/app/'
    input_df['CONDITION_STATUS'] = input_df['CONDITION_STATUS'].map(
        lambda x: subset_map_df.loc['CONDITION_STATUS', x].at['fhir_out_cd'])
    mapping = {"CONDITIONID": "CON_IDENTIFIER",
               "PATID": "CON_SUBJECT_REFERENCE",
               "ENCOUNTERID": "CON_CONTEXT_REFERENCE",
               "CONDITION_TYPE": "CON_CODE_CODING_SYST",
               "CONDITION": "CON_CODE_CODING_CODE",
               "REPORT_DATE": "CON_ASSERT_DATE",
               "RESOLVE_DATE": "CON_ABATEDATE",
               "ONSET_DATE": "CON_ONSETDATE",
               "CONDITION_STATUS": "CON_CLINSTATUS"}
    input_df.rename(columns=mapping, inplace=True)
    input_df['CON_ASSERTER_REFERENCE'] = None
    input_df['CON_CATEGORY_CODING_SYST'] = 'https://www.hl7.org/fhir/valueset-condition-category'
    input_df['CON_CATEGORY_CODING_CODE'] = 'problem-list-item'
    input_df = input_df.drop_duplicates()

    dx_df = pd.read_csv(os.path.join(input_path, "DIAGNOSIS.csv"), sep='|',
                        usecols=['DIAGNOSISID', 'ENCOUNTERID', 'PATID', 'PROVIDERID', 'DX_TYPE',
                                 'DX', 'ADMIT_DATE'])
    dx_df.loc[(dx_df.DX_TYPE != 'SM') &
              (dx_df.DX_TYPE != '09') &
              (dx_df.DX_TYPE != '10') &
              (dx_df.DX_TYPE != '11'), 'DX_TYPE'] = None
    dx_df.loc[(dx_df.DX_TYPE == '09'), 'DX_TYPE'] = 'http://hl7.org/fhir/sid/icd-9-cm'
    dx_df.loc[(dx_df.DX_TYPE == '10'), 'DX_TYPE'] = 'http://hl7.org/fhir/sid/icd-10-cm'
    dx_df.loc[(dx_df.DX_TYPE == '11'), 'DX_TYPE'] = 'http://hl7.org/fhir/sid/icd-11-cm'
    dx_df.loc[(dx_df.DX_TYPE == 'SM'), 'DX_TYPE'] = 'http://snomed.info/sct'
    mapping = {"DIAGNOSISID": "CON_IDENTIFIER",
               "PATID": "CON_SUBJECT_REFERENCE",
               "ENCOUNTERID": "CON_CONTEXT_REFERENCE",
               "PROVIDERID": "CON_ASSERTER_REFERENCE",
               "DX_TYPE": "CON_CODE_CODING_SYST",
               "DX": "CON_CODE_CODING_CODE",
               "ADMIT_DATE": "CON_ASSERT_DATE"}
    dx_df.rename(columns=mapping, inplace=True)
    dx_df['CON_CATEGORY_CODING_SYST'] = 'https://www.hl7.org/fhir/valueset-condition-category'
    dx_df['CON_CATEGORY_CODING_CODE'] = 'encounter-diagnosis'
    dx_df['CON_CLINSTATUS'] = 'active'
    dx_df['CON_ABATEDATE'] = None
    dx_df['CON_ONSETDATE'] = None
    dx_df = dx_df.drop_duplicates()
    join_df = pd.concat([dx_df, input_df])

    i = 0
    partition = int(partition)
    join_df_len = len(join_df)

    cond_dir = os.path.join(output_path, 'Condition')

    while i < join_df_len:
        cond_fhir_entries = []
        file_name = f'{i}.json'
        write_fhir_json(bundle(), cond_dir, file_name)
        if i + partition >= join_df_len:
            if i == 0:
                part_pat_df = join_df
            else:
                part_pat_df = join_df.loc[i:i + join_df_len, :]
        else:
            part_pat_df = join_df.loc[i:i + partition, :]

        part_pat_df.apply(lambda row: map_one_condition(row), axis=1)
        part_pat_df_len = len(part_pat_df)
        file_name = f'{part_pat_df_len}.json'
        write_fhir_json(bundle(entry=cond_fhir_entries), cond_dir, file_name)
        i = i + partition
    return


def lab_conversion(input_path, map_df, output_path, partition):
    input_df, subset_map_df = get_input_df("LAB_RESULT_CM", input_path, map_df)

    def map_one_lab(row):
        entry = {
            "fullUrl": "https://www.hl7.org/fhir/lab.html",
            "resource": {
                "resourceType": "Observation",
                "id": row['LAB_RESULT_CM_ID'],
                "category": [
                    {
                        "coding": [
                            {
                                "system": "http://hl7.org/fhir/observation-category",
                                "code": "laboratory",
                                "display": "Laboratory"
                            }
                        ]
                    }
                ],
                "code": {
                    "coding": [
                        {
                            "system": "http://loinc.org",
                            "code": row['LAB_LOINC']
                        }
                    ]
                },
            "subject": {
              "reference": "Patient/{}".format(row['PATID'])
            },
            "context": {
              "reference": "Encounter/{}".format(row['ENCOUNTERID'])
            },
            "effectiveDateTime": "{} {}".format(row['SPECIMEN_DATE'], row['SPECIMEN_TIME']),
            "issued": row['RESULT_DATE'],
            "interpretation": {
              "coding": [
                {
                  "system": "http://hl7.org/fhir/ValueSet/observation-interpretation",
                  "code": subset_map_df.loc['ABN_IND', row['ABN_IND']].at['fhir_out_cd']
                }
              ]
            }
          }
        }
        if row['RESULT_QUAL'] != 'NI':
            entry['resource']["valueString"] = '{} {}'.format(row['RESULT_QUAL'], row['RAW_RESULT'])

        if row['RESULT_UNIT'] != 'NI':
            entry['resource']['valueQuantity'] = {
                "code": row['RESULT_UNIT']
            }
            comparator = subset_map_df.loc['RESULT_MODIFIER', row['RESULT_MODIFIER']].at['fhir_out_cd']
            if not pd.isnull(comparator):
                entry['resource']['valueQuantity']["comparator"] = comparator

        if not pd.isnull(row['NORM_RANGE_LOW']) and not pd.isnull(row['NORM_RANGE_HIGH']):
            entry['resource']['referenceRange'] = {
                "low": float(row['NORM_RANGE_LOW']),
                "high": row['NORM_RANGE_HIGH']
            }
        lab_fhir_entries.append(entry)
        return

    i = 0
    partition = int(partition)
    pat_df_len = len(input_df)
    pat_dir = os.path.join(output_path, 'Lab')

    while i < pat_df_len:
        lab_fhir_entries = []
        file_name = f'{i}.json'
        write_fhir_json(bundle(), pat_dir, file_name)
        part_pat_df = input_df.loc[i:i+partition, :]
        part_pat_df.apply(lambda row: map_one_lab(row), axis=1)
        part_pat_df_len = len(part_pat_df)
        file_name = f'{part_pat_df_len}.json'
        write_fhir_json(bundle(entry=lab_fhir_entries), pat_dir, file_name)
        i = i + partition
    return
