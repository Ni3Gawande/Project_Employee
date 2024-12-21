import pandas as pd
from CommonUtilities.custom_logger import logger


def save_basic_check_defect_file(df,path):
    location=rf"C:\Users\Anshu\Desktop\folder\ETL\ETLFramework2\DefectFiles\{path}"
    df.to_csv(location,index=False)

def save_defect_data(df_source, df_target, filepath):
    defect_file = pd.merge(df_source, df_target, how='outer', indicator=True).query("_merge!='both'")
    defect_file['_merge'] = defect_file['_merge'].replace({'left_only': 'source_table', 'right_only': 'target_table'})
    defect_file.rename(columns={'_merge': 'side'}, inplace=True)
    location = fr"C:\Users\Anshu\Desktop\folder\ETL\ETLFramework2\DefectFiles\{filepath}"
    if not defect_file.empty:
        defect_file.to_csv(location, index=False)
        logger.error(f"Defect file is stored at location: {location}")
    else:
        logger.info("No defects found. Defect file was not created.")
    return defect_file

def check_the_table_available_in_database(tables, engine):
    for table in tables:
        query = f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table}'"

        # Execute query and fetch result into a DataFrame
        result = pd.read_sql(query, engine)
        if not result.empty:
            logger.info(f"'{table}' is present in the database.")
        else:
            raise ValueError(f"'{table}' is not present in the database.")


def check_data_available_in_table(tables, engine):
    for table in tables:
        query = f'Select * from {table}'

        # Execute query and check result into a DataFrame
        result = pd.read_sql(query, engine)
        if not result.empty:
            logger.info(f"{table} is having data inside it")
        else:
            raise ValueError(f"data is not available in {table}")


def check_duplicates_in_table(table, engine,defect_file):
    logger.info("Executing the query")
    output = f"select * from {table}"
    duplicates = pd.read_sql(output, engine)
    results = duplicates[duplicates.duplicated()]
    if results.empty:
        logger.info(f"{table} is not having duplicate records")
    else:
        save_basic_check_defect_file(results,defect_file)
        raise ValueError(f"{table} is having duplicate records")


def select(table, engine):
    query = f'select * from {table}'
    df = pd.read_sql(query, engine)
    return print(df)


def check_null_records_db(table, engine,filename):
    query = f"select * from {table}"
    df = pd.read_sql(query, engine)
    check_null = df[df.isnull().any(axis=1)]
    if check_null.empty:
        logger.info(f"{table} is not having any null records")
    else:
        check_null.to_csv(fr"C:\Users\Anshu\Desktop\folder\ETL\ETLFramework2\DefectFiles\{filename}",index=False)
        raise ValueError(f"{table} is having null records")


def meta_data_testing(expected_schema, actual_schema):
    result = pd.merge(expected_schema, actual_schema, how='outer', indicator=True).query("_merge!='both' ")
    result.to_csv(r"C:\Users\Anshu\Desktop\folder\ETL\ETLFramework2\DefectFiles\meta.csv", index=False)
    if result.empty:
        logger.info('Meta data comparison dane')
    else:
        raise ValueError('Meta Data mismatch')


def db_to_db_testing(source_query, engine, target_query, engine2, path):
    df_source = pd.read_sql(source_query, engine)
    df_actual = pd.read_sql(target_query, engine2)
    logger.info('Validating data from source to target')
    defect_file=save_defect_data(df_source, df_actual, path)
    if not defect_file.empty:
        raise AssertionError('Data validation failed')
    else:
        logger.info('Data Validation Passed')


def duplicates_in_column(table, column_name, engine):
    query = f'select * from {table}'
    df = pd.read_sql(query, engine)
    result = df[df[f"{column_name}"].duplicated()]
    assert result.empty, "Duplicate in column "


def duplicates_in_columns(query, engine):
    data = pd.read_sql(query, engine)
    assert data.empty, 'there is duplicates found in table'
