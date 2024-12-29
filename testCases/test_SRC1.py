import pytest
from CommonUtilities.custom_logger import LogGen
from CommonUtilities.source_target_database_utilities import DatabaseChecks
import allure


class TestSrc1:
    log_gen = LogGen()
    logger = log_gen.logger()
    database = DatabaseChecks()

    @allure.title("TC-01:Verify all the required tables are present in database")
    @allure.description("Validating all the required tables are present in database")
    def test_required_tables(self, connect_sqlserverdb_engine2):
        self.logger.info("Test case started")
        try:
            tables = ['EMP_DTS_SRC1', 'EMP_DTS_TRG']
            self.logger.info(f"Validating the following tables present in database: {tables}")
            self.database.check_expected_tables_available_in_database(tables, connect_sqlserverdb_engine2)
            self.logger.info("All the required tables are present in database")
            self.logger.info("Test case passed")
        except Exception as e:
            self.logger.error(f"Error details: {e}")
            pytest.fail(f"Test case failed details: {e}")

    @allure.title("TC_02-Verify all the tables have data present inside it")
    def test_data_present_in_tables(self, connect_sqlserverdb_engine2):
        self.logger.info("TC_02-Verify all the tables have data present inside it")
        try:
            tables = ['EMP_DTS_SRC1', 'EMP_DTS_TRG']
            self.logger.info(f"Validating the data present inside following tables: {tables}")
            self.database.check_data_available_in_expected_tables(tables, connect_sqlserverdb_engine2)
            self.logger.info("All the tables have data present inside it")
            self.logger.info('Test case passed')
        except Exception as e:
            self.logger.error(f"Error Details : {e}")
            pytest.fail("fTest case failed : {e}")

    @allure.title("TC_03-Verify the count of null records in table")
    def test_null_count_in_table(self, connect_sqlserverdb_engine2):
        self.logger.info("TC_03-Verify the count of null records in table")
        try:
            self.logger.info(f"Validating the count of null records in table")
            table_name = 'EMP_DTS_SRC1'
            engine = connect_sqlserverdb_engine2
            location = 'EMP_DTS_SRC1_null_records.csv'
            self.database.check_null_records_in_table(table=table_name, engine=engine, file_location=location)
            self.logger.info(f"Null records not present in any column for table: {table_name}")
            self.logger.info("test case passed")
        except Exception as e:
            self.logger.error(f"Error details: {e}")
            pytest.fail(f"Test case failed: {e}")

    @allure.title("TC_04-Verify the count of null records in each column of table")
    def test_null_count_in_columns(self, connect_sqlserverdb_engine2):
        self.logger.info("TC_04-Verify the count of null records in each column of table")
        try:
            self.logger.info(f"Validating the count of null records in each column of table")
            table_name = 'EMP_DTS_SRC1'
            engine = connect_sqlserverdb_engine2
            location = 'EMP_DTS_SRC1_null_columns.csv'
            self.database.check_null_count_of_columns_in_table(table_name=table_name, engine=engine,
                                                               file_location=location)
            self.logger.info(f"Null records not present in any column for table: {table_name}")
            self.logger.info("test case passed")
        except Exception as e:
            self.logger.error(f"Error details: {e}")
            pytest.fail(f"Test case failed: {e}")

    @allure.title("TC_05-Check duplicate in target table")
    def test_duplicate_in_table(self, connect_sqlserverdb_engine2):
        self.logger.info("TC_05-Check duplicate in target table")
        try:
            self.logger.info(f"Validate duplicate in target table")
            table = 'EMP_DTS_TRG'
            engine = connect_sqlserverdb_engine2
            location = 'EMP_DTS_TRG_duplicate_records.csv'
            self.database.check_duplicates_records_in_table(table=table, engine=engine, file_location=location)
            self.logger.info(f"Duplicate records not present in table: {table}")
            self.logger.info("test case passed")
        except Exception as e:
            self.logger.error(f"Error details: {e}")
            pytest.fail(f"Test case failed: {e}")

    @allure.title("TC_06-Check duplicate in target table composite key")
    def test_duplicate_composite_key(self, connect_sqlserverdb_engine2):
        self.logger.info("TC_06-Check duplicate in target table composite key")
        try:
            self.logger.info(f"Validate duplicate in target table")
            self.logger.info("Execute the query")
            query = "Select EMP_CODE,emp_gender, count(*)CNT_EMP_CODE from EMP_DTS_TRG Group by EMP_CODE,emp_gender Having COUNT(*) > 1"
            engine = connect_sqlserverdb_engine2
            location = 'EMP_DTS_TRG_duplicate_records.csv'
            self.database.check_duplicates_by_columns(query=query, engine=engine, file_location=location)
            self.logger.info(f"Duplicate records not present in table EMP_DTS_TRG")
            self.logger.info("test case passed")
        except Exception as e:
            self.logger.error(f"Error details: {e}")
            pytest.fail(f"Test case failed: {e}")

    @allure.title("TC_07-Check record counts")
    @pytest.mark.smoke
    def test_recods_count_between_source_and_target(self, connect_sqlserverdb_engine2):
        self.logger.info("TC_07-Check record counts")
        try:
            self.logger.info("Validate the record count between EMP_DTS_SRC and EMP_DTS_TRG")
            self.logger.info("Execute count query to check the records from EMP_DTS_SRC1")
            emp_dts_src1 = "Select count(*)as count from EMP_DTS_SRC1 where EMP_SALARY >= '1000' "
            self.logger.info("Execute count query to check the records from EMP_DTS_TRG")
            emp_dts_trg1 = "Select count(*)count from EMP_DTS_TRG where REFERENCE_TABLE ='EMP_DTS_SRC1'"
            engine1 = connect_sqlserverdb_engine2
            engine2 = connect_sqlserverdb_engine2
            location = "count.csv"
            self.database.record_count_check(source_query=emp_dts_src1, engine1=engine1, target_query=emp_dts_trg1,
                                             engine2=engine2, file_location=location)
            self.logger.info("test case passed")
        except Exception as e:
            self.logger.error(f"Error details: {e}")
            pytest.fail(f"Test case failed: {e}")

    @allure.title("Test case to check filter")
    def test_filter_o_table(self, connect_sqlserverdb_engine2):
        self.logger.info("Test case to check filter")
        try:
            query = "select * from emp_dts_TRG where emp_salary<='1000' and REFERENCE_TABLE ='EMP_DTS_SRC1'"
            engine = connect_sqlserverdb_engine2
            location = 'filter.csv'
            self.database.check_the_filter_target_table(query=query, engine=engine, file_location=location)
            self.logger.info("test case passed")
        except Exception as e:
            self.logger.error(f"Error details: {e}")
            pytest.fail(f"Test case failed: {e}")
