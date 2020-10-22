import sys
import pandas as pd

from convert_patient import patient_conversion
from convert_medication_request import medicationrequest_conversion
from convert_encounter import encounter_conversion
from convert_condition import condition_conversion
from convert_lab import lab_conversion
from convert_vital import vital_conversion
from convert_procedure import procedure_conversion
from convert_practitioner import practitioner_conversion
from convert_observation import obs_conversion


def mapping_pcornet_to_fhir(input_path, output_path, partition):
    # the mapping_file directory is set up as a relative path to where the convert.py is located assuming running
    # the convert.py as a standalone app. If this method is called in another directory, make sure to change the
    # mapping_file path accordingly in order to load the mapping file successfully. For example, if this method is
    # called by a module running in an upper directory, it can be set as:
    # mapping_file = 'tx-pcornet-to-fhir/mapping/pcornet_to_fhir.csv'
    mapping_file = 'mapping/pcornet_to_fhir.csv'
    map_df = pd.read_csv(mapping_file, index_col=['table_cd', 'column_cd', 'local_in_cd'])
    patient_conversion(input_path, map_df, output_path, partition)
    medicationrequest_conversion(input_path, map_df, output_path, partition)
    encounter_conversion(input_path, map_df, output_path, partition)
    condition_conversion(input_path, map_df, output_path, partition)
    lab_conversion(input_path, map_df, output_path, partition)
    vital_conversion(input_path, map_df, output_path, partition)
    procedure_conversion(input_path, output_path, partition)
    practitioner_conversion(input_path, map_df, output_path, partition)
    obs_conversion(input_path, map_df, output_path, partition)


if __name__ == '__main__':
    args = sys.argv[1:]
    if len(args) == 3:
        input_path = args[0]
        output_path = args[1]
        partition = args[2]
        mapping_pcornet_to_fhir(input_path, output_path, partition)
        sys.exit(0)
    else:
        print("Run this python pcornet to fhir mapping script by passing "
              "input path that contains input data in pcornet CDM in csv format, "
              "output path that will contain converted data in FHIR resource bundles, "
              "and the partition number that is used to split resource bundles into "
              "multiple partitions: \n"
              "python <input_data_path> <out_data_path> <partition>")
        sys.exit(1)
