import pandas as pd
from CommonUtilities.custom_logger import logger
from CommonUtilities.defect_file_utilities import *

# def save_basic_check_defect_file(df, path):
#     location = rf"C:\Users\Anshu\Desktop\folder\ETL\ETLFramework2\DefectFiles\{path}"
#     df.to_csv(location, index=False)
#
#
# def save_defect_data_validation_to_file(source_query,target_query,path):
#     defect_file=pd.merge(source_query,target_query,how="outer",indicator=True).query("_merge!='both'")
#     defect_file['_merge']=defect_file['_merge'].replace({"_left_only":"source_table","_right_only":"target_table"})
#     defect_file.rename(columns={"_merge":"table"},inplace=True)
#     location=fr"C:\Users\Anshu\Desktop\folder\ETL\ETLFramework2\DefectFiles\{path}"
#     if not defect_file.empty:
#         defect_file.to_csv(location,index=False)
#         logger.error(f"Defect file is stored at location: {location}")
#     else:
#         logger.info("No defects found. Defect file was not created.")
#     return defect_file


def check_expected_tables_available_in_database(tables, engine):
    for table in tables:
        query = f" select * from information_schema.tables where table_name= '{table}' "
        result = pd.read_sql(query, engine)
        if not result.empty:
            logger.info(f"{table} is present in the  database")
        else:
            logger.info(f"{table} is not present in the database")
            raise ValueError(f"{table} is not present in the databse")


def check_data_available_in_expected_tables(tables, engine):
    for table in tables:
        query = f'selet * from {table}'
        result = pd.read_sql(query, engine)
        if not result.empty:
            logger.info(f'Data is available in {table}')
        else:
            logger.info(f"Data is not available in {table}")
            raise ValueError(f"Data is not available in {table}")


def check_duplicates_records_in_table(table, engine, defect_file):
    logger.info("Executing the query")
    query = f"select * from {table}"
    duplicates = pd.read_sql(query, engine)
    results = duplicates[duplicates.duplicated()]
    if results.empty:
        logger.info(f"{table} is not having duplicate records")
    else:
        save_basic_check_defect_file(results, defect_file)
        raise ValueError(f"{table} is having duplicate records")


def check_null_records_in_table(table, engine, defect_file):
    query = f"select * from {table}"
    null_check = pd.read_sql(query, engine)
    null = null_check[null_check.isna().any(axis=1)]
    if null.empty:
        logger.info(f"{table} does not contains null value")
    else:
        save_basic_check_defect_file(null, defect_file)
        raise ValueError(f"{table} contain null value in it")

def check_record_counts_between_tables(source_table, engine1, target_table, engine2, defect_file):
    # source_count = pd.read_sql(source_query, engine1)
    # target_count = pd.read_sql(target_query, engine2)
    # source_count = source_count.shape[0]
    # target_count = target_count.shape[0]

    source_query=f'select count(*)count from {source_table}'
    target_query=f"Select count(*)count from {target_table}"

    source_count = pd.read_sql(source_query, engine1)['count'][0]
    target_count = pd.read_sql(target_query, engine2)['count'][0]


    difference = source_count - target_count
    if difference != 0:
        count = pd.DataFrame({
            'source_count': [source_count],
            'target_count': [target_count],
            'difference between source and target': [difference]
        })
        save_basic_check_defect_file(count, defect_file)
        raise ValueError(
            f"Source and Target count mismatch as source count: {source_count}, target count: {target_count}")
    else:
        logger.info('Source to target count matches')

def check_duplicate_records_in_columns(query,engine,defect_file):
    duplicates=pd.read_sql(query,engine)
    if not duplicates.empty:
        save_basic_check_defect_file(duplicates,defect_file)
        logger.info('Duplicate is present')
        raise AssertionError(f"Duplicate rocords is found in column of table")
    else:
        logger.info("Duplicate is not present")

def total_number_of_records(query,engine):
    counts=pd.read_sql(query,engine)
    counts=counts.shape[0]
    logger.info(f"Total count is {counts}")
    return counts

def check_data_validation_for_columns(source_query, source_engine, target_query, target_engine, path):
    df_source = pd.read_sql(source_query, source_engine)
    df_actual = pd.read_sql(target_query, target_engine)
    logger.info('Validating data from source to target')
    defect_file=save_defect_data_validation_to_file(df_source, df_actual, path)
    if defect_file.empty:
        logger.info('Data Validation Passed')
    else:
        raise AssertionError('Data validation failed')

def check_data_validation_for_table(source_query, source_engine, target_query, target_engine, path):
    df_source = pd.read_sql(source_query, source_engine)
    df_actual = pd.read_sql(target_query, target_engine)
    logger.info('Validating data from source to target')
    defect_file=save_defect_data_validation_to_file(df_source, df_actual, path)
    if defect_file.empty:
        logger.info('Data Validation Passed')
    else:
        raise AssertionError('Data validation failed')

