import pandas as pd
import os

from utils import bundle, write_fhir_json, get_input_df, write_fhir_0_json


def condition_conversion(input_path, map_df, output_path, partition):
    # read DEATH_CAUSE to add to condition resource as needed
    dc_df = pd.read_csv(os.path.join(input_path, "DEATH_CAUSE.csv"), sep='|', index_col=['PATID'],
                        usecols=['PATID', 'DEATH_CAUSE', 'DEATH_CAUSE_CODE'])

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

        if not pd.isnull(row['CON_CODE_CODING_SYST']) or not pd.isnull(row['CON_CODE_CODING_CODE']) or \
                row['PATID'] in dc_df.index:
            coding_dict = {}
            code_dict = {'coding': []}
            if not pd.isnull(row['CON_CODE_CODING_SYST']):
                coding_dict['system'] = row['CON_CODE_CODING_SYST']
            if not pd.isnull(row['CON_CODE_CODING_CODE']):
                coding_dict['code'] = row['CON_CODE_CODING_CODE']
            code_dict['coding'].append(coding_dict)
            if row['CON_SUBJECT_REFERENCE'] in dc_df.index:
                dc_codes = dc_df.loc[row['CON_SUBJECT_REFERENCE']]

                def map_one_dc_code(dc_row):
                    if str(dc_row['DEATH_CAUSE_CODE']) == '09':
                        code_system = 'http://hl7.org/fhir/sid/icd-9-cm'
                    elif str(dc_row['DEATH_CAUSE_CODE']) == '10':
                        code_system = 'http://hl7.org/fhir/sid/icd-10-cm'
                    else:
                        code_system = None

                    if code_system:
                        dc_code_dict = {
                            'system': code_system,
                            'code': dc_row['DEATH_CAUSE']
                        }
                        code_dict['coding'].append(dc_code_dict)
                    return

                if isinstance(dc_codes, pd.DataFrame):
                    dc_codes.apply(lambda dc_row: map_one_dc_code(dc_row), axis=1)
                else:  # it is of type Series
                    map_one_dc_code(dc_codes)

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
    write_fhir_0_json(cond_dir)

    while i < join_df_len:
        cond_fhir_entries = []
        part_pat_df = join_df.iloc[i:i+partition, :]
        part_pat_df.apply(lambda row: map_one_condition(row), axis=1)
        part_pat_df_len = len(part_pat_df)
        file_name = f'{part_pat_df_len}.json'
        write_fhir_json(bundle(entry=cond_fhir_entries), cond_dir, file_name)
        i = i + partition
    return
