import pandas as pd
import os

from utils import bundle, write_fhir_json, get_input_df


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
