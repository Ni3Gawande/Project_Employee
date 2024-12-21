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

    # Read data from the database table
    logger.info(f"Fetching data from the database table: {table_name}")
    query = f"SELECT * FROM {table_name}"
    # comparing the data between df_actual and df_expected
    logger.info(f"Compairing the data between {file_path} and  {table_name}")
    df_actual = pd.read_sql(query, db_engine)
    defect_file=save_defect_data_validation_to_file(df_expected,df_actual,defect_file_path)
    if defect_file.empty:
        logger.info('Data Validation Passed')
    else:
        raise AssertionError('Data validation failed')

def file_transformation_database_validation(source_df,target_df,engine,defect_file_path):
    logger.info("Validation the data between source and target data frame")
    defect_file = save_defect_data_validation_to_file(source_df,target_df,defect_file_path)
    logger.info(f'\n{source_df}')
    logger.info(f'\n{target_df}')
    if defect_file.empty:
        logger.info('Data Validation Passed')
    else:
        raise AssertionError('Data validation failed')


