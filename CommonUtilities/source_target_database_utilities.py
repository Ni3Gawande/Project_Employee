import pandas as pd
from CommonUtilities.custom_logger import logger
from CommonUtilities.defect_file_utilities import *


# Verify that all the expected tables in the database
def check_expected_tables_available_in_database(tables, engine):
    for table in tables:
        query = f" select * from information_schema.tables where table_name= '{table}' "
        result = pd.read_sql(query, engine)
        if not result.empty:
            logger.info(f"{table} is present in the  database")
        else:
            logger.info(f"{table} is not present in the database")
            raise ValueError(f"{table} is not present in the databse")

# Verify that all the expected tables in the database contain data
def check_data_available_in_expected_tables(tables, engine):
    for table in tables:
        query = f'select * from {table}'
        result = pd.read_sql(query, engine)
        if not result.empty:
            logger.info(f'Data is available in {table}')
        else:
            logger.info(f"Data is not available in {table}")
            raise ValueError(f"Data is not available in {table}")

# Verify the presence of duplicate records in the table
def check_duplicates_records_in_table(table, engine, defect_file):
    logger.info("Executing the query")
    query = f"select * from {table}"
    duplicates = pd.read_sql(query, engine)
    logger.info(f"Checking Duplicates in {table}")
    results = duplicates[duplicates.duplicated()]
    if results.empty:
        logger.info(f"{table} is not having duplicate records")
    else:
        save_basic_check_defect_file(results, defect_file)
        raise ValueError(f"{table} is having duplicate records")

# Verify the presence of null records in the table
def check_null_records_in_table(table, engine, defect_file):
    logger.info("Execute the query")
    query = f"select * from {table}"
    logger.info(f'Check null records in {table}')
    null_check = pd.read_sql(query, engine)
    null = null_check[null_check.isna().any(axis=1)]
    if null.empty:
        logger.info(f"{table} does not contains null value")
    else:
        save_basic_check_defect_file(null, defect_file)
        raise ValueError(f"{table} contain null value in it")

# Verify that the row count of the source table matches the target table.
def check_record_counts_between_tables(source_table, engine1, target_table, engine2, defect_file):
    # source_count = pd.read_sql(source_query, engine1)
    # target_count = pd.read_sql(target_query, engine2)
    # source_count = source_count.shape[0]
    # target_count = target_count.shape[0]
    logger.info(f"Executing the query and checking the count of source_table: {source_table}")
    source_query=f'select count(*)count from {source_table}'
    logger.info(f"Executing the query and checking the count of target_table: {target_table}")
    target_query=f"Select count(*)count from {target_table}"
    source_count = pd.read_sql(source_query, engine1)['count'][0]
    target_count = pd.read_sql(target_query, engine2)['count'][0]
    logger.info(f"Comparing the count between {source_table} and {target_table}")
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

# Verify the presence of duplicate records in the columns
def check_duplicates_by_columns(query,engine,defect_file):
    duplicates=pd.read_sql(query,engine)
    if not duplicates.empty:
        save_basic_check_defect_file(duplicates,defect_file)
        logger.info('Duplicate records are present')
        raise AssertionError(f"Duplicate rocords is found in column of table")
    else:
        logger.info("Duplicate records are not present")

# Check the row count of table
def check_table_row_count(table,engine,location):
    # counts=counts.shape[0]
    logger.info(f'Executing the row count query of {table} table')
    query=f'select count(*) as count from {table}'
    counts=pd.read_sql(query,engine)['count'][0]
    logger.info(f"Total count is {counts}")
    return counts

# Verify the data integrity and consistency within each column in the source and target tables.
def check_data_validation_for_columns(source_query, source_engine, target_query, target_engine, path):
    df_source = pd.read_sql(source_query, source_engine)
    df_actual = pd.read_sql(target_query, target_engine)
    logger.info('Validating data from source to target')
    defect_file=save_the_mismatch_to_file(df_source, df_actual, path)
    if defect_file.empty:
        logger.info('Data Validation Passed')
    else:
        raise AssertionError('Data validation failed')


# Verify that the entire source table matches the corresponding target table in terms of data integrity and completeness.
# def check_data_validation_for_table(source_query, source_engine, target_query, target_engine, path):
#     df_source = pd.read_sql(source_query, source_engine)
#     df_actual = pd.read_sql(target_query, target_engine)
#     logger.info('Validating data from source to target')
#     defect_file=save_the_mismatch_to_file(df_source, df_actual, path)
#     if defect_file.empty:
#         logger.info('Data Validation Passed')
#     else:
#         raise AssertionError('Data validation failed')


# Verify that the filter works as expected in the table.
def check_the_filter_target_table(target_query,engine,defect_file_path):
    filter=pd.read_sql(target_query,engine)
    if filter.empty:
        logger.info('Filter condition passed')
    else:
        logger.error(f'Filter condition failed and unfiltered records are stored in a file at loacation: {defect_file_path}')
        save_basic_check_defect_file(filter,defect_file_path)
        raise AssertionError('Records found that does not match filter condition')


# def check_meta_data_testing(metadatafile,query,engine,loc):
#     expected_schema=pd.read_csv(metadatafile).astype(str)
#     # expected_schema=pd.DataFrame(schema).astype(str)
#     actual_schema=pd.read_sql(query,engine).astype(str)
#     logger.info(f"\n{expected_schema}")
#     logger.info(f"\n{actual_schema}")
#     defect_file=save_the_mismatch_to_file(expected_schema,actual_schema,loc)
#     if not defect_file.empty:
#         raise AssertionError('Data validation failed')
#     else:
#         logger.info('Data Validation Passed')

