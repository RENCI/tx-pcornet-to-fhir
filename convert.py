import sys
from utils import mapping_pcornet_to_fhir


if __name__ == '__main__':
    args = sys.argv[1:]
    if len(args) == 3:
        input_path = args[0]
        output_path = args[1]
        partition = args[2]
        table_cd_file_list = []
        # for dir_name, _, file_list in os.walk(input_path):
        #     for file_name in file_list:
        #         table_cd_file_list.append(file_name)
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
