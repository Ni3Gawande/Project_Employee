##################### for file as target is it even applicable (1% focous)###############################

import pandas as pd
from CommonUtilities.custom_logger import logger
from CommonUtilities.defect_file_utilities import save_basic_check_defect_file,save_the_mismatch_to_file
import os


def check_file_exists(file_path):
    if os.path.exists(file_path):
        return True
    else:
        raise ValueError(f"File not found at: {file_path}")


def check_data_exists_in_file(file_path, file_type):
    if file_type == 'csv':
        df_actual = pd.read_csv(file_path)

    elif file_type == 'xml':
        df_actual = pd.read_xml(file_path, xpath='.//item')

    elif file_type == 'json':
        df_actual = pd.read_json(file_path)

    else:
        logger.error(f"Unsupported file type: {file_type}")
        raise ValueError(f"Unsupported file type: {file_type}")


    if df_actual.empty:
        logger.error(f"Data is not present in: {file_type}")
        raise AssertionError(f"Data is not present in: {file_type}")
    else:
        logger.info(f"Data is present in: {file_type}")


def check_duplicate_in_file(file_path,file_type,location):
    if file_type == 'csv':
        df_actual = pd.read_csv(file_path)

    elif file_type == 'xml':
        df_actual = pd.read_xml(file_path, xpath='.//item')

    elif file_type == 'json':
        df_actual = pd.read_json(file_path)

    else:
        logger.error(f"Unsupported file type: {file_type}")
        raise ValueError(f"Unsupported file type: {file_type}")

    duplicates=df_actual[df_actual.duplicated()]

    if duplicates.empty:
        logger.info(f'Duplicates records are not present in file {file_type}')

    else:
        logger.error(f"Duplicate records present in {file_path}")
        save_basic_check_defect_file(duplicates,location)
        raise AssertionError(f"Duplicate records present in {file_path}")
