# from CommonUtilities.custom_logger import logger
from CommonUtilities.source_target_database_utilities import *
# import pandas as pd
import pytest

class Test_EMP_DTS_SRC2:

    def test_required_tables_exist_in_database(self,connect_sqlserverdb_engine2):
        logger.info("TC_01-Check all the required tables exists in database")
        try:
            """ tables=['EMP_DTS_SRC2','EMP_DTS_TRG','K9']  # failing testcase by adding k9 as it is not availabe in database"""

            tables = ['EMP_DTS_SRC2', 'EMP_DTS_TRG']
            logger.info("Checking table present in database")
            check_expected_tables_available_in_database(tables, connect_sqlserverdb_engine2)
            logger.info("Test Case passed all the requied tables exists in database")
        except Exception as e:
            logger.error(f'Error details: {e}')
            pytest.fail(f"Test case failed: {e}")

    def test_table_data_exists(self,connect_sqlserverdb_engine2):
        logger.info("TC_02-Check data available in all the required tables")
        try:
            """ tables=['EMP_DTS_SRC2','EMP_DTS_TRG','K9']  # failing testcase by adding k9 as it is not availabe in database"""
            """tables = ['EMP_DTS_SRC2', 'EMP_DTS_TRG','empty'] # dailing testcase as their is no data in empty table"""
            tables = ['EMP_DTS_SRC2', 'EMP_DTS_TRG']
            logger.info("Checking data present is tables")
            check_data_available_in_expected_tables(tables, connect_sqlserverdb_engine2)
            logger.info("Test case passed data is available in the tables")
        except Exception as e:
            logger.error(f"Error details: {e}")
            pytest.fail(f"Test case failed: {e}")

    # def test_duplicate_records_in_EMP_DTS_TRG(connect_sqlserverdb_engine):
    def test_duplicate_records_in_EMP_DTS_TRG(self,connect_sqlserverdb_engine2):
        logger.info("TC_03-Check duplicates records in EMP_DTS_TRG table")
        try:
            """check_duplicates_records_in_table('k9',connect_sqlserverdb_engine,'duplicates_EMP_DTS_TRG.csv') #k9 table is from different database and contain duplicate records """
            check_duplicates_records_in_table('EMP_DTS_TRG', connect_sqlserverdb_engine2, 'duplicates_EMP_DTS_TRG.csv')
            logger.info(f"Test case passed no duplicate records found")
        except Exception as e:
            logger.error(f"Error details: {e}")
            pytest.fail(f"Test case failed: {e}")

    def test_null_records_in_EMP_DTS_TRG(self,connect_sqlserverdb_engine2):
        logger.info("TC_04-Check null records in EMP_DTS_TRG")
        try:
            # check_null_records_in_table('EMP_DTS_TRG',connect_sqlserverdb_engine2,'null_EMP_DTS_TRG.csv') # intentionally failed emp_dts_TRG contain null records it it'
            check_null_records_in_table('EMP_DTS_SRC3', connect_sqlserverdb_engine2, 'null_EMP_DTS_TRG.csv')
            logger.info('Test case passed no null records found')
        except Exception as e:
            logger.error(f"Error details: {e}")
            pytest.fail(f"Test case failed: {e}")

    def test_records_count_between_source_and_traget(self,connect_sqlserverdb_engine2):
        logger.info("TC_05-Record count between source table and target table")
        try:

            query1="Select count(*)count from EMP_DTS_SRC1"
            query2="Select count(*)count from EMP_DTS_TRG"
            count_check(query1,connect_sqlserverdb_engine2,query2,connect_sqlserverdb_engine2,'records_counts_src_trg.csv')

            logger.info("Test case passed: source and target have matching records")
        except Exception as e:
            logger.error(f"Error details: {e}")
            pytest.fail(f"Test case failed: {e}")

    def test_row_count_of_target(self,connect_sqlserverdb_engine2):
        logger.info('TC_06-Count of records of target side')
        try:
            check_table_row_count('EMP_DTS_TRG', connect_sqlserverdb_engine2, 'emp_dts,trg_count.csv')
            logger.info("Test case complete")
        except Exception as e:
            logger.error(f"Error details: {e}")
            pytest.fail(f"Test case fail: {e}")

    def test_composite_key(self,connect_sqlserverdb_engine2):
        logger.info("TC-07_Check Composite key")  # These types of records aren't detected as duplicates by pandas
        try:
            # query="select eid,ename,count(*)count from dupes group by eid,ename having count(*)>1"#intentionall failed
            query = "Select EMP_KEY,count(*)count from emp_dts_trg group by emp_key having count(*)>1"
            check_duplicates_by_columns(query, connect_sqlserverdb_engine2, 'eid_name.csv')
        except Exception as e:
            logger.error(f"Error details: {e}")
            pytest.fail(f"Test case failed: {e}")

    def test_count_of_primary_matches_count_of_records(self,connect_sqlserverdb_engine2):
        logger.info("TC_08-Check primary key count and records counts are matching")
        try:
            logger.info("Executing count query")
            query_count = "select count(*)count from EMP_DTS_TRG"
            # query_count="select count(*)count from EMP_DTS_SRC1" #it has less records intentionall fail
            logger.info("Executing primary count query")
            query_primary_count = "Select count(EMP_Key)count from EMP_DTS_TRG"
            check_data_validation_for_columns(query_count, connect_sqlserverdb_engine2, query_primary_count,
                                              connect_sqlserverdb_engine2, 'count_pvt.csv')
            logger.info("Test case passed")
        except Exception as e:
            logger.error(f"Error details: {e}")
            pytest.fail(f"Test case failed: {e}")

    def test_data_check_filter(self,connect_sqlserverdb_engine2):
        logger.info("TC_09-Check the filter transformation logic")
        try:
            logger.info("Executing filter check transformation query on table")
            query = "Select * from EMP_DTS_TRG where EMP_SALARY <= '1000.0' "
            # query="Select * from EMP_DTS_TRG where EMP_SALARY > '1000.0' " #to fail the condition intentoalyy
            check_the_filter_target_table(query, connect_sqlserverdb_engine2, 'filter_emp_dts_trg.csv')
            logger.info("Test case passed")
        except Exception as e:
            logger.error(f"Error details: {e}")
            pytest.fail(f"Test case failed: {e}")

    def test_metadata_of_tabel(self,connect_sqlserverdb_engine2):
        logger.info("TC_10-MetaData Testing of table: EMP_DTS_SRC2")
        try:
            query = "select TABLE_NAME,COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,ORDINAL_POSITION from INFORMATION_SCHEMA.columns where table_name='EMP_DTS_SRC2' "
            check_meta_data_of_table(
                r"C:\Users\Anshu\Desktop\folder\ETL\ETLFramework2\TestData\MetaData_EMP_DTS_SRC2.csv", query,
                connect_sqlserverdb_engine2, 'metadeta_emp_Sts_src2.csv')
            logger.info("MetaData testing passed from table: EMP_DTS_SRC2 ")
        except Exception as e:
            logger.error(f"Error details: {e}")
            pytest.fail(f"Test case failed: {e}")

#########              REMAINING WILL BE COLUMN LEVEL VALIATION CAN BE ACHIVED BY  check_data_validation_for_columns()              ########