import pandas as pd
import os
import json

from utils import bundle, write_fhir_json, get_input_df


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
