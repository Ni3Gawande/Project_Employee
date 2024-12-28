##################### for file as target is it even applicable (1% focous)###############################

import pandas as pd
import os
from CommonUtilities.custom_logger import LogGen
from CommonUtilities.defect_file_utilities import SaveFile


class FileToFileValidation:
    savefile=SaveFile()
    log_gen = LogGen()
    logger = log_gen.logger()

    def check_file_exists(self,file_path):
        if os.path.exists(file_path):
            return True
        else:
            raise ValueError(f"File not found at: {file_path}")

    def check_data_exists_in_file(self,file_path, file_type):
        if file_type == 'csv':
            df_actual = pd.read_csv(file_path)

        elif file_type == 'xml':
            df_actual = pd.read_xml(file_path, xpath='.//item')

        elif file_type == 'json':
            df_actual = pd.read_json(file_path)

        else:
            self.logger.error(f"Unsupported file type: {file_type}")
            raise ValueError(f"Unsupported file type: {file_type}")

        if df_actual.empty:
            self.logger.error(f"Data is not present in: {file_type}")
            raise AssertionError(f"Data is not present in: {file_type}")
        else:
            self.logger.info(f"Data is present in: {file_type}")

    def check_duplicate_in_file(self,file_path, file_type, location):
        if file_type == 'csv':
            df_actual = pd.read_csv(file_path)

        elif file_type == 'xml':
            df_actual = pd.read_xml(file_path, xpath='.//item')

        elif file_type == 'json':
            df_actual = pd.read_json(file_path)

        else:
            self.logger.error(f"Unsupported file type: {file_type}")
            raise ValueError(f"Unsupported file type: {file_type}")

        duplicates = df_actual[df_actual.duplicated()]

        if duplicates.empty:
            self.logger.info(f'Duplicates records are not present in file {file_type}')

        else:
            self.logger.error(f"Duplicate records present in {file_path}")
            self.savefile.save_basic_check_defect_file(duplicates, location)
            raise AssertionError(f"Duplicate records present in {file_path}")

