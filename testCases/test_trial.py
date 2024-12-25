import pytest
from CommonUtilities.custom_logger import logger
from CommonUtilities.source_target_database_utilities import database_basics
import pandas as pd

class Test_table:
    database=database_basics()


    def test_required_tables_exist_in_database(self,connect_sqlserverdb_engine2):
        logger.info("TC_01-Check all the required tables exists in database")
        try:
            """ tables=['EMP_DTS_SRC2','EMP_DTS_TRG','Ksbsdhfb7']  # failing testcase by adding k9 as it is not availabe in database"""

            tables = ['EMP_DTS_SRC2', 'EMP_DTS_TRG']
            logger.info("Checking table present in database")
            self.database.check_expected_tables_available_in_database(tables, connect_sqlserverdb_engine2)
            logger.info("Test Case passed all the requied tables exists in database")
        except Exception as e:
            # logger.error(f"TC_01-Failed")
            logger.error(f'Error details: {e}')
            pytest.fail(f"Test case failed: {e}")



    @pytest.mark.smoke
    def test_table_data_exists(self,connect_sqlserverdb_engine2):
        logger.info("TC_02-Check data available in all the required tables")
        try:
            """ tables=['EMP_DTS_SRC2','EMP_DTS_TRG','K9']  # failing testcase by adding k9 as it is not availabe in database"""
            """tables = ['EMP_DTS_SRC2', 'EMP_DTS_TRG','empty'] # dailing testcase as their is no data in empty table"""
            tables = ['EMP_DTS_SRC2','EMP_DTS_TRG']
            logger.info("Checking data present is tables")
            self.database.check_data_available_in_expected_tables(tables, connect_sqlserverdb_engine2)
            logger.info("Test case passed data is available in the tables")
        except Exception as e:
            logger.error(f"Error details: {e}")
            pytest.fail(f"Test case failed: {e}")

    def test_dfas(self):
        sd=pd.read_csv(r"C:\Users\Anshu\Desktop\folder\ETL\ETLFramework2\TestData\metadatak9.csv")
        pass











    # def test_duplicate_records_in_table(self, connect_sqlserverdb_engine):
    #     logger.info("TC_03-Check duplicates in the table")
    #     try:
    #         check_duplicates_records_in_table('k9', connect_sqlserverdb_engine, 'dupe1.csv')
    #         logger.info("Test case passed no duplicates in table")
    #     except Exception as e:
    #         logger.info(f"Test case failed due to {e}")
    #         logger.error(f"Error due to: {e}")
    #         pytest.fail(f"Test case failed due to {e}")
    #
    # def test_null_records_in_table(self, connect_sqlserverdb_engine2):
    #     try:
    #         check_null_records_in_table('EMP_DTS_TRG', connect_sqlserverdb_engine2, 'EMP_DTS_TRG_null.csv')
    #     except Exception as e:
    #         logger.error(f"Error Details: {e}")
    #         pytest.fail(f"Test case failed due to {e}")
    #
    # def test_table_available_in_database(self, connect_sqlserverdb_engine):
    #     logger.info("___________________________________test case____________________")
    #     try:
    #         tables = ['employees', 'k9']
    #         check_expected_tables_available_in_database(tables, connect_sqlserverdb_engine)
    #     except Exception as e:
    #         logger.error(f"Error Detail: {e}")
    #         pytest.fail(f"Test case failed due to : {e}")
    #
    # def test_count_betweenS_source_and_target(self, connect_sqlserverdb_engine2):
    #     logger.info('TC-Checking the counts between two tables')
    #     try:
    #         # source_table_query = "select * from EMP_DTS_SRC1"
    # target_table_query = "select * from EMP_DTS_TRG"
    #     check_record_counts_between_tables('EMP_DTS_SRC1', connect_sqlserverdb_engine2,'EMP_DTS_TRG', connect_sqlserverdb_engine2,
    #                                        'count_SRC1_DTR2.csv')
    #     logger.info("Test case passed")
    # except Exception as e:
    #     logger.error(f"Error details : {e}")
    #     pytest.fail(f"Test case failed due to {e}")

    # def test_duplicate(self,connect_sqlserverdb_engine):
    #     logger.info('-------------------------z-------------------------')
    #     try:
    #         query='Select ename,ecity, count(*)count from k9 group by ename,ecity  having count(*)>1'
    #         check_duplicate_records_in_columns(query,connect_sqlserverdb_engine,'k9.csv')
    #     except Exception as e:
    #         logger.error(f'Error details: {e}')
    #         pytest.fail(f"Tesc case failed due to: {e}")
    #
    # def test_cout(self,connect_sqlserverdb_engine2):
    #     try:
    #         query="select * from EMP_DTS_TRG"
    #         total_number_of_records(query,connect_sqlserverdb_engine2)
    #     except Exception as e:
    #         pytest.fail('TEst case failed')
    #
    # def test_extraction_from_sales_data_CSV_to_sales_staging_MySQL(self,connect_mysqldb_engine):
    #     logger.info("Data extraction from sales_data.csv to staging_sales has started")
    #     try:
    #         file_to_db_verify('Testdata/sales_data.csv', 'csv', 'staging_sales', connect_mysqldb_engine, 'defect_sales.csv')
    #         logger.info("Data extraction from sales_data.csv to sales_staging has completed")
    #     except Exception as e:
    #         logger.error(f"Error occurred during data extraction: {e}")
    #         pytest.fail(f"Test failed due to an error: {e}")

    # def test_some(self,connect_mysqldb_engine):
    #     try:
    #         logger.info("read the data from csv file")
    #         source_df=pd.read_csv(r"C:\Users\Anshu\Desktop\folder\ETL\ETLFramework2\TestData\sales_data.csv")
    #         logger.info("grouping th data of vsv file on region coumn and fiding its max quantity")
    #         source_df=source_df.groupby('region').agg({'quantity':'max'}).reset_index()
    #         logger.info('execution the query from target table')
    #         target_query="select region,max(quantity)quantity from staging_sales group by region"
    #         target_df = pd.read_sql(target_query,connect_mysqldb_engine)
    #         file_transformation_database_validation(source_df, target_df, connect_mysqldb_engine, 'somewere.csv')
    #         logger.info("test case passed")
    #     except Exception as e:
    #         pytest.fail(f'dailed due to {e}')

    # def test_file_present(self):
    #     logger.info('Check file is available or not')
    #     try:
    #         path=r"C:\Users\Anshu\Desktop\folder\ETL\ETLFramework2\DefectFiles\dupe.csv"
    #         check_file_exists(path)
    #         logger.info('Test case passed')
    #     except Exception as e:
    #         logger.error(f"Error details: {e}")
    #         pytest.fail(f'Test case failed due to {e}')

    # def test_filter(self,connect_sqlserverdb_engine2):
    #
    #     try:
    #         query='select * from EMP_DTS_SRC1 where emp_salary < 1000.0'
    #         check_the_filter_target_table(query,connect_sqlserverdb_engine2, 'filter.csv')
    #     except Exception as e:
    #         logger.error(f'Error details: {e}')
    #         pytest.fail(f'test case failed due to {e}')

    # def test_filter_table(self,connect_sqlserverdb_engine2):
    #
    #     try:
    #         query='select * from EMP_DTS_TRG where emp_salary < 1000.0'
    #         check_the_filter_target_table(query,connect_sqlserverdb_engine2, 'targetfilter.csv')
    #     except Exception as e:
    #         logger.error(f'Error details: {e}')
    #         pytest.fail(f'test case failed due to {e}')

    # @pytest.mark.parametrize('query, file_location', [
    #     ("SELECT * FROM EMP_DTS_SRC1 WHERE emp_salary < 1000.0", "filter1.csv"),
    #     ("SELECT * FROM EMP_DTS_SRC2 WHERE emp_salary < 1000.0", "filter2.csv"),
    #     ("SELECT * FROM EMP_DTS_SRC3 WHERE emp_salary < 1000.0", "filter3.csv"),
    #     ])
    # def test_filter(self, connect_sqlserverdb_engine2, query, file_location):
    #     """
    #     Test the filtering of data based on the provided SQL query and save mismatches to a file.
    #     """
    #     try:
    #         check_the_filter_target_table(query, connect_sqlserverdb_engine2, file_location)
    #         logger.info(f"Filter validation passed for query: {query}")
    #     except Exception as e:
    #         logger.error(f"Error details: {e}")
    #         pytest.fail(f"Test case failed for query: {query} with error: {e}")












    # def test_meta_data_testing(self,connect_sqlserverdb_engine):
    #
    #     try:
    #         query="select TABLE_NAME,COLUMN_NAME,CHARACTER_MAXIMUM_LENGTH,ORDINAL_POSITION from information_schema.columns where table_name='k9' "
    #         check_entire_data(r"C:\Users\Anshu\Desktop\folder\ETL\ETLFramework2\TestData\metadatak9.csv",query,connect_sqlserverdb_engine,'check.csv')
    #
    #     except Exception as e:
    #         pytest.fail(f"faild details: {e}")


    # def testfile(self):
    #     try:
    #         check_duplicate_in_file(r"C:\Users\Anshu\Desktop\folder\ETL\ETLFramework2\TestData\sales_data.csv",'csv','dddd.csv')
    #     except Exception as e:
    #         pytest.fail(f"{e}")
    #
    # def test_query(self,connect_sqlserverdb_engine):
    #     query = text("""alter table k9 drop column state""")
    #     connect_sqlserverdb_engine.execute(query)
    #     connect_sqlserverdb_engine.commit()
    #     query2="Select * from information_schema.columns where table_name='k9' and column_name='state'"
    #     if query2 is not None:
    #         logger.info("Test passed")
    #     else:
    #         pytest.fail()

    # def test_query(self, connect_sqlserverdb_engine):
    #     # Query to drop the 'state' column from the 'k9' table
    #     query = text("""ALTER TABLE k9 DROP COLUMN state""")
    #     connect_sqlserverdb_engine.execute(query)  # Execute the drop column query
    #     connect_sqlserverdb_engine.commit()


