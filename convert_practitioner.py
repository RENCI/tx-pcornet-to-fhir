import os

from utils import bundle, write_fhir_json, get_input_df, write_fhir_0_json, get_partition_file_name


def practitioner_conversion(input_path, map_df, output_path, partition):
    filter_cols = ['PROVIDERID', 'PROVIDER_SEX']
    input_df, subset_map_df = get_input_df("PROVIDER", input_path, map_df, use_cols=filter_cols)

    def map_one_row(row):
        entry = {
            "fullUrl": "https://www.hl7.org/fhir/practitioner.html",
            "resource": {
                "resourceType": "Practitioner",
                "id": row['PROVIDERID'],
                "gender": subset_map_df.loc['PROVIDER_SEX', row['PROVIDER_SEX']].at['fhir_out_cd']
            }
        }
        input_fhir_entries.append(entry)
        return

    i = 0
    partition = int(partition)
    input_df_len = len(input_df)
    input_dir = os.path.join(output_path, 'Practitioner')
    write_fhir_0_json(input_dir)
    while i < input_df_len:
        input_fhir_entries = []
        part_input_df = input_df.iloc[i:i+partition, :]
        part_input_df.apply(lambda row: map_one_row(row), axis=1)
        part_input_df_len = len(part_input_df)
        file_name = get_partition_file_name(partition, part_input_df_len, i)
        write_fhir_json(bundle(entry=input_fhir_entries), input_dir, file_name)
        i = i + partition
    return
