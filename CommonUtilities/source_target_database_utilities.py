from CommonUtilities.custom_logger import LogGen
import pandas as pd
from CommonUtilities.defect_file_utilities import SaveFile
#
#
#
# # Verify that all the expected tables in the database
# def check_expected_tables_available_in_database(tables, engine):
#     for table in tables:
#         query = f" select * from information_schema.tables where table_name= '{table}' "
#         result = pd.read_sql(query, engine)
#         if not result.empty:
#             logger.info(f"{table} is present in the  database")
#         else:
#             logger.info(f"{table} is not present in the database")
#             raise ValueError(f"{table} is not present in the database")
#
#
# # Verify that all the expected tables in the database contain data
# def check_data_available_in_expected_tables(tables, engine):
#     for table in tables:
#         query = f'select * from {table}'
#         result = pd.read_sql(query, engine)
#         if not result.empty:
#             logger.info(f'Data is available in {table}')
#         else:
#             logger.info(f"Data is not available in {table}")
#             raise ValueError(f"Data is not available in {table}")
#
#
# # Verify the presence of duplicate records in the table
# def check_duplicates_records_in_table(table, engine, defect_file):
#     logger.info("Executing the query")
#     query = f"select * from {table}"
#     duplicates = pd.read_sql(query, engine)
#     logger.info(f"Checking Duplicates in {table}")
#     results = duplicates[duplicates.duplicated()]
#     if results.empty:
#         logger.info(f"{table} is not having duplicate records")
#     else:
#         save_basic_check_defect_file(results, defect_file)
#         raise ValueError(f"{table} is having duplicate records")
#
# # Verify the presence of duplicate records in the columns
# def check_duplicates_by_columns(query, engine, defect_file):
#     duplicates = pd.read_sql(query, engine)
#     if not duplicates.empty:
#         save_basic_check_defect_file(duplicates, defect_file)
#         logger.info('Duplicate records are present')
#         raise AssertionError(f"Duplicate rocords is found in column of table")
#     else:
#         logger.info("Duplicate records are not present")
#
#
# # Verify the presence of null records in the table
# def check_null_records_in_table(table, engine, defect_file):
#     logger.info("Execute the query")
#     query = f"select * from {table}"
#     logger.info(f'Check null records in {table}')
#     null_check = pd.read_sql(query, engine)
#     null = null_check[null_check.isna().any(axis=1)]
#     if null.empty:
#         logger.info(f"{table} does not contains null value")
#     else:
#         save_basic_check_defect_file(null, defect_file)
#         raise ValueError(f"{table} contain null value in it")
#
#
# # Verify that the record count of the source table matches the target table.
# def count_check(source_query, engine1, target_query, engine2, defect_file):
#     source_count = pd.read_sql(source_query, engine1)['count'][0]
#     target_count = pd.read_sql(target_query, engine2)['count'][0]
#     logger.info(f"Comparing the count between source and target")
#     difference = source_count - target_count
#     if difference != 0:
#         count = pd.DataFrame({
#             'source_count': [source_count],
#             'target_count': [target_count],
#             'difference between source and target': [difference]
#         })
#         save_basic_check_defect_file(count, defect_file)
#         raise ValueError(
#             f"Source and Target count mismatch as source count: {source_count}, target count: {target_count}")
#     else:
#         logger.info('Source to target count matches')
#
#
#
# # Check the row count of table
# def check_table_row_count(table, engine, location):
#     # counts=counts.shape[0]
#     logger.info(f'Executing the row count query of {table} table')
#     query = f'select count(*) as count from {table}'
#     counts = pd.read_sql(query, engine)['count'][0]
#     logger.info(f"Total count is {counts}")
#     return counts
#
#
# # Verify the data integrity and consistency within each column in the source and target tables.
# def check_data_validation_for_columns(source_query, source_engine, target_query, target_engine, path):
#     df_source = pd.read_sql(source_query, source_engine)
#     df_actual = pd.read_sql(target_query, target_engine)
#     logger.info('Validating data from source to target')
#     defect_file = save_the_mismatch_to_file(df_source, df_actual, path)
#     if defect_file.empty:
#         logger.info('Data Validation Passed')
#     else:
#         raise AssertionError('Data validation failed')
#
#
# # Verify that the filter works as expected in the table.
# def check_the_filter_target_table(target_query, engine, defect_file_path):
#     filter = pd.read_sql(target_query, engine)
#     if filter.empty:
#         logger.info('Filter condition passed')
#     else:
#         logger.error(
#             f'Filter condition failed and unfiltered records are stored in a file at loacation: {defect_file_path}')
#         save_basic_check_defect_file(filter, defect_file_path)
#         raise AssertionError('Records found that does not match filter condition')
#
#
# # verify the meta data of the table
# def check_meta_data_of_table(metadatafile, query, engine, path):
#     logger.info("Expected meta data of the table from mapping document")
#     df_source = pd.read_csv(metadatafile).astype(str)
#     logger.info("Actual meta data of the table")
#     df_actual = pd.read_sql(query, engine).astype(str)
#     logger.info('Validating meta data of the tables')
#     defect_file = save_the_mismatch_to_file(df_source, df_actual, path)
#     if defect_file.empty:
#         logger.info('MetaData validation passed')
#     else:
#         raise AssertionError('MetaData validation failed')
#
#
#
class DatabaseChecks:
    savefile=SaveFile()
    log_gen = LogGen()
    logger = log_gen.logger()
    # Verify that all the expected tables in the database
    def check_expected_tables_available_in_database(self,tables, engine):
        missing_tables=[]
        for table in tables:
            query = f" select * from information_schema.tables where table_name= '{table}' "
            result = pd.read_sql(query, engine)
            if not result.empty:
                self.logger.info(f"{table} is present in the  database")
            else:
                self.logger.info(f"Table {table} is not present in the database")
                missing_tables.append(table)
                # raise ValueError(f"Table {table} is not present in the database")
        if missing_tables:
            raise ValueError(f"The following tables are missing in the database: {', '.join(missing_tables)}")

    # Verify that all the expected tables in the database contain data
    def check_data_available_in_expected_tables(self,tables, engine):
        empty_tables=[]
        for table in tables:
            query = f'select * from {table}'
            result = pd.read_sql(query, engine)
            if not result.empty:
                self.logger.info(f'Data is available in {table}')
            else:
                self.logger.info(f"Data is not available in {table}")
                empty_tables.append(table)
                # raise ValueError(f"Data is not available in {table}")
        if empty_tables:
            raise ValueError(f"The following tables are not having data in it:{', '.join(empty_tables)}")

    # Verify the presence of duplicate records in the table
    def check_duplicates_records_in_table(self,table, engine, defect_file):
        self.logger.info("Executing the query")
        query = f"select * from {table}"
        duplicates = pd.read_sql(query, engine)
        self.logger.info(f"Checking Duplicates in {table}")
        results = duplicates[duplicates.duplicated()]
        if results.empty:
            self.logger.info(f"{table} is not having duplicate records")
        else:
            self.savefile.save_basic_check_defect_file(results, defect_file)
            raise ValueError(f"{table} is having duplicate records")

    # Verify the presence of duplicate records in the columns
    def check_duplicates_by_columns(self,query, engine, defect_file):
        duplicates = pd.read_sql(query, engine)
        if not duplicates.empty:
            self.savefile.save_basic_check_defect_file(duplicates, defect_file)
            self.logger.info('Duplicate records are present')
            raise AssertionError(f"Duplicate records is found in column of table")
        else:
            self.logger.info("Duplicate records are not present")

    # Verify the presence of null records in the table
    def check_null_records_in_table(self,table, engine, defect_file):
        self.logger.info("Execute the query")
        query = f"select * from {table}"
        self.logger.info(f'Check null records in {table}')
        null_check = pd.read_sql(query, engine)
        null = null_check[null_check.isna().any(axis=1)]
        if null.empty:
            self.logger.info(f"{table} does not contains null value")
        else:
            self.savefile.save_basic_check_defect_file(null, defect_file)
            raise ValueError(f"{table} contain null value in it")

    # Verify that the record count of the source table matches the target table.
    def count_check(self,source_query, engine1, target_query, engine2, defect_file):
        source_count = pd.read_sql(source_query, engine1)['count'][0]
        target_count = pd.read_sql(target_query, engine2)['count'][0]
        self.logger.info(f"Comparing the count between source and target")
        difference = source_count - target_count
        if difference != 0:
            count = pd.DataFrame({
                'source_count': [source_count],
                'target_count': [target_count],
                'difference between source and target': [difference]
            })
            self.savefile.save_basic_check_defect_file(count, defect_file)
            raise ValueError(
                f"Source and Target count mismatch as source count: {source_count}, target count: {target_count}")
        else:
            self.logger.info('Source to target count matches')

    # Check the row count of table
    def check_table_row_count(self,table, engine, location):
        # counts=counts.shape[0]
        self.logger.info(f'Executing the row count query of {table} table')
        query = f'select count(*) as count from {table}'
        counts = pd.read_sql(query, engine)['count'][0]
        self.logger.info(f"Total count is {counts}")
        self.savefile.save_information_file(counts,location)
        return counts

    # Verify the data integrity and consistency within each column in the source and target tables.
    def check_data_validation_for_columns(self,source_query, source_engine, target_query, target_engine, path):
        df_source = pd.read_sql(source_query, source_engine)
        df_actual = pd.read_sql(target_query, target_engine)
        self.logger.info('Validating data from source to target')
        defect_file = self.savefile.save_the_mismatch_to_file(df_source, df_actual, path)
        if defect_file.empty:
            self.logger.info('Data Validation Passed')
        else:
            raise AssertionError('Data validation failed')

    # Verify that the filter works as expected in the table.
    def check_the_filter_target_table(self,target_query, engine, defect_file_path):
        filters = pd.read_sql(target_query, engine)
        if filters.empty:
            self.logger.info('Filter condition passed')
        else:
            self.logger.error(
                f'Filter condition failed and unfiltered records are stored in a file at location: {defect_file_path}')
            self.savefile.save_basic_check_defect_file(filters, defect_file_path)
            raise AssertionError('Records found that does not match filter condition')

    # verify the metadata of the table
    def check_meta_data_of_table(self,metadata_file, query, engine, path):
        self.logger.info("Expected meta data of the table from mapping document")
        df_source = pd.read_csv(metadata_file).astype(str)
        self.logger.info("Actual meta data of the table")
        df_actual = pd.read_sql(query, engine).astype(str)
        self.logger.info('Validating meta data of the tables')
        defect_file = self.savefile.save_the_mismatch_to_file(df_source, df_actual, path)
        if defect_file.empty:
            self.logger.info('MetaData validation passed')
        else:
            raise AssertionError('MetaData validation failed')


    def check_null_count_in_table(self,table_name,engine,location):
        query=f"select * from {table_name}"
        read_table=pd.read_sql(query,engine)
        null_count=read_table.isnull().sum().reset_index()
        null_count.columns=['column','count']
        result=null_count[null_count['count'] >= 1]
        if result.empty:
            self.logger.info(f"There is no null value in column for table:{table_name}")
            self.savefile.save_information_file(null_count,location)
        else:
            self.savefile.save_basic_check_defect_file(null_count,location)
            raise ValueError(f"There are null present in columns for table: {table_name}")

