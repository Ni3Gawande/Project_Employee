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
    def test_required_tables(self,connect_sqlserverdb_engine2):
        self.logger.info("Test case started")
        try:
            tables=['EMP_DTS_SRC1', 'EMP_DTS_TRG']
            self.logger.info(f"Validating the following tables present in database: {tables}")
            self.database.check_expected_tables_available_in_database(tables,connect_sqlserverdb_engine2)
            self.logger.info("All the required tables are present in database")
            self.logger.info("Test case passed")
        except Exception as e:
            self.logger.error(f"Error details: {e}")
            pytest.fail(f"Test case failed details: {e}")

    def test_null_count_in_columns(self, connect_sqlserverdb_engine2):
        self.logger.info("TestCase03")
        try:
            self.database.check_null_count_in_table('EMP_DTS_SRC2', connect_sqlserverdb_engine2,
                                                    'EMP_DTS_SRC3_null_columns.csv')
            self.logger.info("test case passed")
        except Exception as e:
            self.logger.error(f"Error details: {e}")
            pytest.fail(f"Test case failed: {e}")
