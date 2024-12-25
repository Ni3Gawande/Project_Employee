import pandas as pd
from CommonUtilities.custom_logger import logger
from CommonUtilities.defect_file_utilities import *

def file_to_db_verify(file_path, file_type, table_name, db_engine, defect_file_path):
    # Read the source file
    if file_type == 'csv':
        logger.info(f"Fetching the data from {file_path}")
        df_expected = pd.read_csv(file_path)
    elif file_type == 'xml':
        logger.info(f"Fetching the data from {file_path}")
        df_expected = pd.read_xml(file_path, xpath='.//item')
    elif file_type == 'json':
        logger.info(f"Fetching the data from {file_path}")
        df_expected = pd.read_json(file_path)
    else:
        logger.error(f"Unsupported file type: {file_type}")
        raise ValueError(f"Unsupported file type: {file_type}")


    logger.info(f"Fetching data from the database table: {table_name}")
    query = f"SELECT * FROM {table_name}"
    logger.info(f"Compairing the data between {file_path} and  {table_name}")
    df_actual = pd.read_sql(query, db_engine)
    defect_file=save_the_mismatch_to_file(df_expected,df_actual,defect_file_path)
    if defect_file.empty:
        logger.info('Data Validation Passed')
    else:
        raise AssertionError('Data validation failed')

def check_data_validation_for_columns(source_df,target_df,engine,defect_file_path):
    logger.info("Validation the data between source and target data frame")
    defect_file = save_the_mismatch_to_file(source_df,target_df,defect_file_path)
    logger.info(f'\n{source_df}')
    logger.info(f'\n{target_df}')
    if defect_file.empty:
        logger.info('Data Validation Passed')
    else:
        raise AssertionError('Data validation failed')


def check_entire_data(source_file,target_query,engine,defect_file_path):
    expected_data=pd.read_csv(source_file).astype(str)
    actual_data=pd.read_sql(target_query,engine).astype(str)
    defect_file = save_the_mismatch_to_file(expected_data,actual_data,defect_file_path)
    if defect_file.empty:
        logger.info('Data Validation Passed')
    else:
        raise AssertionError('Data validation failed')