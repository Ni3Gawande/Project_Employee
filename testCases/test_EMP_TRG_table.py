import pytest
from CommonUtilities.custom_logger import logger
from CommonUtilities.utilities import *
#
#
class Test_EMP_TRG:
#
#     def test_EMP_TRG_table_avability(self, connect_sqlserverdb_engine):
#         logger.info("TC_01- Check the avability of employees table in a database ")
#         try:
#             tables = ['employees', 'k14']
#             check_the_table_available_in_database(tables, connect_sqlserverdb_engine)
#             logger.info("Test case passed")
#         except Exception as e:
#             logger.info('Test case failed')
#             logger.error(f"error is due to : {e}")
#             pytest.fail(f"Test case failed due to {e}")
#
#     def test_EMP_TRG_table_avability_of_data_in_tables(self, connect_sqlserverdb_engine):
#         logger.info("TC_02-Check the avability of data in a table")
#         try:
#             tables = ['employees', 'k14']
#             check_data_available_in_table(tables, connect_sqlserverdb_engine)
#             logger.info('Test Case passed')
#         except Exception as e:
#             logger.info('Test Case Failed')
#             logger.error(f'Error due to: {e}')
#             pytest.fail(f"test cased failed due to {e}")
#
    def test_duplicate_records_in_table(self, connect_sqlserverdb_engine):
        logger.info("TC_03-Check duplicates in the table")
        try:
            check_duplicates_in_table('k9', connect_sqlserverdb_engine,'dupe.csv')
            logger.info("Test case passed no duplicates in table")
        except Exception as e:
            logger.info(f"Test case failed due to {e}")
            logger.error(f"Error due to: {e}")
            pytest.fail(f"Test case failed due to {e}")

#
#     def test_null_records_in_tables(self, connect_sqlserverdb_engine):
#         logger.info("TC_04 - Check null records in table")
#         try:
#             check_null_records_db('table3', connect_sqlserverdb_engine,'table3.csv')
#             logger.info("Test cas passed")
#         except Exception as e:
#             logger.error(f"Error due to: {e}")
#             pytest.fail(f"Test case failed due to {e}")
#
#
#     def test_emp_name_column(self, connect_sqlserverdb_engine):
#         logger.info("TC_05-validate data from source to target for emp_name column")
#         try:
#             source_query = "Select employee_id,first_name from employees"
#             target_query = "select employee_id,first_name from employees"
#             db_to_db_testing(source_query, connect_sqlserverdb_engine, target_query, connect_sqlserverdb_engine,'defect.csv')
#             logger.info("test case passed")
#         except Exception as e:
#             logger.error(f"Test case failed")
#             logger.error(f"Error due to {e}")
#             pytest.fail(f"test case failed due to {e}")
#
#     def test_data(self, connect_sqlserverdb_engine):
#         select('employees', connect_sqlserverdb_engine)
#
#     def test_meta_data_validation(self, connect_sqlserverdb_engine):
#         logger.info(' TC_06-MEta data testing')
#         schema = {
#             'Table_name': ['employees'] * 11,
#             'Column_name': ['EMPLOYEE_ID', 'FIRST_NAME', 'LAST_NAME', 'EMAIL', 'PHONE_NUMBER', 'HIRE_DATE', 'JOB_ID',
#                             'SALARY', 'COMMISSION_PCT', 'MANAGER_ID', 'DEPARTMENT_ID'],
#             'Data_type': ['int', 'varchar', 'varchar', 'varchar', 'varchar', 'date', 'varchar', 'decimal', 'decimal',
#                           'numeric', 'numeric'],
#             'CHARACTER_MAXIMUM_LENGTH': [None, 20, 25, 25, 20, None, 10, None, None, None, None],
#             'ORDINAL_POSITION': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
#         }
#
#         expected_schema = pd.DataFrame(schema).astype(str)
#         query="Select table_name,column_name,Data_type,Character_maximum_length,ordinal_position from information_schema.columns where table_name='employees' "
#         actual_schema=pd.read_sql(query,connect_sqlserverdb_engine).astype(str)
#         logger.info(f"{expected_schema}")
#         logger.info(f"{actual_schema}")
#         print(actual_schema)
#         try:
#             meta_data_testing(actual_schema,expected_schema)
#             logger.info('Test case passed')
#         except Exception as e:
#             logger.info('Test case failed')
#             logger.error(f"error Due to: {e}")
#             pytest.fail(f"TEst case faile due to {e}")


