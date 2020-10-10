import pandas as pd
import os
import json

from utils import bundle, write_fhir_json, get_input_df


def vital_conversion(input_path, map_df, output_path, partition):
    def map_one_vital(row):
        entry = {
            "fullUrl": "https://www.hl7.org/fhir/vital.html",
            "resource": {
                "resourceType": "Observation",
                "id": row['OBS_IDENTIFIER'],
                "subject": {
                  "reference": "Patient/{}".format(row['OBS_SUBJECT_REFERENCE'])
                },
                "context": {
                  "reference": "Encounter/{}".format(row['OBS_CONTEXT_REFERENCE'])
                },
                "status": row['OBS_STATUS'],
                "effectiveDateTime": row['OBS_EFFECTIVEDATETIME']
              }
        }
        if not pd.isnull(row['OBS_CATEGORY_SYST']) or not pd.isnull(row['OBS_CATEGORY_CODE']):
            cat_dict = {}
            code_dict = {}
            if not pd.isnull(row['OBS_CATEGORY_SYST']):
                code_dict['system'] = row['OBS_CATEGORY_SYST']
            if not pd.isnull(row['OBS_CATEGORY_CODE']):
                code_dict['code'] = row['OBS_CATEGORY_CODE']
            if not pd.isnull(row['OBS_CATEGORY_DISPLAY']):
                code_dict['display'] = row['OBS_CATEGORY_DISPLAY']
            cat_dict['coding'] = [code_dict]
            entry['resource']['category'] = [cat_dict]

        if not pd.isnull(row['OBS_CODE_CODING_SYST']) or not pd.isnull(row['OBS_CODE_CODING_CODE']):
            coding_dict = {}
            code_dict = {}
            if not pd.isnull(row['OBS_CODE_CODING_SYST']):
                coding_dict['system'] = row['OBS_CODE_CODING_SYST']
            if not pd.isnull(row['OBS_CODE_CODING_CODE']):
                coding_dict['code'] = row['OBS_CODE_CODING_CODE']
            if not pd.isnull(row['OBS_CODE_CODING_DISPLAY']):
                coding_dict['display'] = row['OBS_CODE_CODING_DISPLAY']
            code_dict['coding'] = [coding_dict]
            entry['resource']['code'] = code_dict

        if (row['OBS_VALUEQUANTITY_VALUE'] != '' and not pd.isnull(row['OBS_VALUEQUANTITY_VALUE'])) or \
                (row['OBS_VALUEQUANTITY_CODE'] != '' and not pd.isnull(row['OBS_VALUEQUANTITY_CODE'])):
            entry['resource']['valueQuantity'] = {}
            if row['OBS_VALUEQUANTITY_VALUE'] != '' and not pd.isnull(row['OBS_VALUEQUANTITY_VALUE']):
                entry['resource']['valueQuantity']['value'] = row['OBS_VALUEQUANTITY_VALUE']
            if row['OBS_VALUEQUANTITY_CODE'] != '' and not pd.isnull(row['OBS_VALUEQUANTITY_CODE']):
                entry['resource']['valueQuantity']['code'] = row['OBS_VALUEQUANTITY_CODE']
        vital_fhir_entries.append(entry)
        return

    filter_cols = ["VITALID", "PATID", "ENCOUNTERID", "HT", "SYSTOLIC", "MEASURE_DATE", "SMOKING", "TOBACCO",
                   "DIASTOLIC", "ORIGINAL_BMI", "WT"]
    input_df, subset_map_df = get_input_df("VITAL", input_path, map_df, use_cols=filter_cols)
    input_df['OBS_STATUS'] = 'final'
    input_df['OBS_CATEGORY_SYST'] = 'http://hl7.org/fhir/ValueSet/observation-category'
    input_df['OBS_VALUEQUANTITY_COMPARATOR'] = ''
    input_df['OBS_VALUEQUANTITY_SYST'] =  'https://unitsofmeasure.org'
    input_df['OBS_VALUESTRING'] = ''
    input_df['OBS_ISSUED'] = ''
    input_df['OBS_REFRANGE_LOW'] = ''
    input_df['OBS_REFRANGE_HIGH'] = ''
    input_df['OBS_INTERPRETATION_CODE'] = ''
    input_df['OBS_INTERPRETATION_SYST'] = ''

    input_df1 = input_df.loc[(input_df['SMOKING'] == '') & (input_df['TOBACCO'] == '') & (input_df['HT'] != '')]
    input_df1.drop(columns=["SMOKING", "TOBACCO"])
    input_df1['OBS_CODE_CODING_CODE'] = '8302-2'
    input_df1['OBS_CODE_CODING_DISPLAY'] = 'Body height'
    input_df1['OBS_VALUEQUANTITY_CODE'] = '[in_i]'
    input_df1['OBS_CATEGORY_CODE'] = 'vital-signs'
    input_df1['OBS_CATEGORY_DISPLAY'] = 'Vital Signs'
    input_df1['OBS_CODE_CODING_SYST'] = 'http://loinc.org/'
    mapping = {"VITALID": "OBS_IDENTIFIER",
               "PATID": "OBS_SUBJECT_REFERENCE",
               "ENCOUNTERID": "OBS_CONTEXT_REFERENCE",
               "HT": "OBS_VALUEQUANTITY_VALUE",
               "MEASURE_DATE": "OBS_EFFECTIVEDATETIME"
               }
    input_df1.rename(columns=mapping, inplace=True)
    input_df1 = input_df1.drop_duplicates()

    input_df2 = input_df.loc[(input_df['SMOKING'] == '') & (input_df['TOBACCO'] == '') & (input_df['WT'] != '')]
    input_df2.drop(columns=["SMOKING", "TOBACCO"])
    input_df2['OBS_CODE_CODING_CODE'] = '29463-7'
    input_df2['OBS_CODE_CODING_DISPLAY'] = 'Body weight'
    input_df2['OBS_VALUEQUANTITY_CODE'] = '[lb_av]'
    input_df2['OBS_CATEGORY_CODE'] = 'vital-signs'
    input_df2['OBS_CATEGORY_DISPLAY'] = 'Vital Signs'
    input_df2['OBS_CODE_CODING_SYST'] = 'http://loinc.org/'
    mapping = {"VITALID": "OBS_IDENTIFIER",
               "PATID": "OBS_SUBJECT_REFERENCE",
               "ENCOUNTERID": "OBS_CONTEXT_REFERENCE",
               "WT": "OBS_VALUEQUANTITY_VALUE",
               "MEASURE_DATE": "OBS_EFFECTIVEDATETIME"
               }
    input_df2.rename(columns=mapping, inplace=True)
    input_df2 = input_df2.drop_duplicates()

    input_df3 = input_df.loc[(input_df['SMOKING'] == '') & (input_df['TOBACCO'] == '') & (input_df['DIASTOLIC'] != '')]
    input_df3.drop(columns=["SMOKING", "TOBACCO"])
    input_df3['OBS_CODE_CODING_CODE'] = '8462-4'
    input_df3['OBS_CODE_CODING_DISPLAY'] = 'Diastolic blood pressure'
    input_df3['OBS_VALUEQUANTITY_CODE'] = 'mm[Hg]'
    input_df3['OBS_CATEGORY_CODE'] = 'vital-signs'
    input_df3['OBS_CATEGORY_DISPLAY'] = 'Vital Signs'
    input_df3['OBS_CODE_CODING_SYST'] = 'http://loinc.org/'
    mapping = {"VITALID": "OBS_IDENTIFIER",
               "PATID": "OBS_SUBJECT_REFERENCE",
               "ENCOUNTERID": "OBS_CONTEXT_REFERENCE",
               "DIASTOLIC": "OBS_VALUEQUANTITY_VALUE",
               "MEASURE_DATE": "OBS_EFFECTIVEDATETIME"
               }
    input_df3.rename(columns=mapping, inplace=True)
    input_df3 = input_df3.drop_duplicates()

    input_df4 = input_df.loc[(input_df['SMOKING'] == '') & (input_df['TOBACCO'] == '') & (input_df['SYSTOLIC'] != '')]
    input_df4.drop(columns=["SMOKING", "TOBACCO"])
    input_df4['OBS_CODE_CODING_CODE'] = '8480-6'
    input_df4['OBS_CODE_CODING_DISPLAY'] = 'Systolic blood pressure'
    input_df4['OBS_VALUEQUANTITY_CODE'] = 'mm[Hg]'
    input_df4['OBS_CATEGORY_CODE'] = 'vital-signs'
    input_df4['OBS_CATEGORY_DISPLAY'] = 'Vital Signs'
    input_df4['OBS_CODE_CODING_SYST'] = 'http://loinc.org/'
    mapping = {"VITALID": "OBS_IDENTIFIER",
               "PATID": "OBS_SUBJECT_REFERENCE",
               "ENCOUNTERID": "OBS_CONTEXT_REFERENCE",
               "SYSTOLIC": "OBS_VALUEQUANTITY_VALUE",
               "MEASURE_DATE": "OBS_EFFECTIVEDATETIME"
               }
    input_df4.rename(columns=mapping, inplace=True)
    input_df4 = input_df4.drop_duplicates()

    input_df5 = input_df.loc[(input_df['SMOKING'] == '') & (input_df['TOBACCO'] == '') & (input_df['ORIGINAL_BMI'] != '')]
    input_df5.drop(columns=["SMOKING", "TOBACCO"])
    input_df5['OBS_CODE_CODING_CODE'] = '36156-5'
    input_df5['OBS_CODE_CODING_DISPLAY'] = 'Body mass index'
    input_df5['OBS_VALUEQUANTITY_CODE'] = 'kg/m2'
    input_df5['OBS_CATEGORY_CODE'] = 'vital-signs'
    input_df5['OBS_CATEGORY_DISPLAY'] = 'Vital Signs'
    input_df5['OBS_CODE_CODING_SYST'] = 'http://loinc.org/'
    mapping = {"VITALID": "OBS_IDENTIFIER",
               "PATID": "OBS_SUBJECT_REFERENCE",
               "ENCOUNTERID": "OBS_CONTEXT_REFERENCE",
               "ORIGINAL_BMI": "OBS_VALUEQUANTITY_VALUE",
               "MEASURE_DATE": "OBS_EFFECTIVEDATETIME"
               }
    input_df5.rename(columns=mapping, inplace=True)
    input_df5 = input_df5.drop_duplicates()

    input_df6 = input_df.loc[(~ pd.isnull(input_df['SMOKING']) & (~ pd.isna(input_df['SMOKING'])))]
    input_df6['OBS_CATEGORY_CODE'] = 'social-history'
    input_df6['OBS_CATEGORY_DISPLAY'] = 'Social History'
    input_df6['OBS_CODE_CODING_SYST'] = 'http://snomed.info/sct/'
    input_df6['OBS_CODE_CODING_CODE'] = input_df6['SMOKING'].map(
        lambda x: subset_map_df.loc['SMOKING', x].at['fhir_out_cd'])
    input_df6['OBS_CODE_CODING_DISPLAY'] = input_df6['SMOKING'].map(
        lambda x: subset_map_df.loc['SMOKING', x].at['fhir_out_char'])
    input_df6['OBS_VALUEQUANTITY_CODE'] = ''
    input_df6['OBS_VALUEQUANTITY_VALUE'] = ''
    input_df6['OBS_VALUEQUANTITY_SYST'] = ''
    input_df6.drop(columns=["SMOKING", "TOBACCO"])
    mapping = {"VITALID": "OBS_IDENTIFIER",
               "PATID": "OBS_SUBJECT_REFERENCE",
               "ENCOUNTERID": "OBS_CONTEXT_REFERENCE",
               "MEASURE_DATE": "OBS_EFFECTIVEDATETIME"
               }
    input_df6.rename(columns=mapping, inplace=True)
    input_df6 = input_df6.drop_duplicates()

    join_df = pd.concat([input_df1, input_df2, input_df3, input_df4, input_df5, input_df6])

    i = 0
    partition = int(partition)
    join_df_len = len(join_df)

    vital_dir = os.path.join(output_path, 'Vital')

    while i < join_df_len:
        vital_fhir_entries = []
        file_name = f'{i}.json'
        write_fhir_json(bundle(), vital_dir, file_name)
        if i + partition >= join_df_len:
            if i == 0:
                part_pat_df = join_df
            else:
                part_pat_df = join_df.loc[i:i + join_df_len, :]
        else:
            part_pat_df = join_df.loc[i:i + partition, :]

        part_pat_df.apply(lambda row: map_one_vital(row), axis=1)
        part_pat_df_len = len(part_pat_df)
        file_name = f'{part_pat_df_len}.json'
        write_fhir_json(bundle(entry=vital_fhir_entries), vital_dir, file_name)
        i = i + partition
    return
