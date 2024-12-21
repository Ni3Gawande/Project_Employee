import pandas as pd
from CommonUtilities.custom_logger import logger
from CommonUtilities.utilities import *
import pytest


class Test_EMPDTSSRC1_vs_EMPDTSTRG:
    logger.info("TC_01-Check the duplicate records in the table")

    def test_EMP_Key(self, connect_sqlserverdb_engine2):
        try:
            check_duplicates_in_table('EMP_DTS_TRG', connect_sqlserverdb_engine2)
            logger.info(' Test Case passed')
        except Exception as e:
            logger.info("Test case Failed")
            logger.error((f"Error du to: {e}"))
            pytest.fail(f"Test case failed due to {e}")

    def test_key(self, connect_sqlserverdb_engine2):
        logger.info("TC_02-Check the duplicate records in emp_key column")
        try:
            duplicates_in_column('EMP_DTS_TRG', 'EMP_Key', connect_sqlserverdb_engine2)
            logger.info('Test case passed')
        except Exception as e:
            logger.info("test case failed")
            logger.error(f"Test case failed due to {e}")
            pytest.fail(f"Test case failed due to {e}")

    def test_duplicates_in_columns(self, connect_sqlserverdb_engine2):
        logger.info("TC_03-Check duplicates in EMP_Key and EMP_Code columns")
        try:
            query = "select emp_key,emp_code,count(*)count from EMP_DTS_TRG group by EMP_Key,EMP_CODE having count(*)>1"
            duplicates_in_columns(query, connect_sqlserverdb_engine2)
            logger.info("Test case passed")
        except Exception as e:
            logger.info("Test case failed")
            logger.error(f"Error ue to {e}")
            pytest.fail(f"Test case faied due to {e}")

    def test_direct_mapping_column(self, connect_sqlserverdb_engine2):
        logger.info("TC_04-Check the data matches between emp_code from emp_DTS_SCR1 with EMP_DTS_TRG")
        try:
            query_source = """ select emp_code from EMP_DTS_SRC1 where EMP_SALARY > 1000 """
            query_target = """ select emp_code from EMP_DTS_TRG where REFERENCE_TABLE='EMP_DTS_SRC1' """
            db_to_db_testing(query_source, connect_sqlserverdb_engine2, query_target, connect_sqlserverdb_engine2,
                             'emp_code.csv')
            logger.info("Test case passed")
        except Exception as e:
            logger.info("TEst case Failed")
            logger.error(f"error due to: {e}")
            pytest.fail(f"test failed  due to: {e}")

    def test_direct_mapping(self, connect_sqlserverdb_engine2):
        logger.info(
            "TC_05-Check the To validate 1:1 transformations of 'EMP_CODE','EMP_DOB','HIRE_DATE','Created_On','Modified_On' columns")
        try:
            logger.info("Execute the source query")
            query_source = "Select EMP_CODE,EMP_DOB,HIRE_DATE,Created_On,Modified_On  from EMP_DTS_SRC1 Where EMP_SALARY >= 1000 "
            logger.info("Execute the target query")
            query_target = "Select EMP_CODE,EMP_DOB,HIRE_DATE,Created_On,Modified_On  from EMP_DTS_TRG Where REFERENCE_TABLE = 'EMP_DTS_SRC1'"
            db_to_db_testing(query_source, connect_sqlserverdb_engine2, query_target, connect_sqlserverdb_engine2,
                             '1:1.csv')
            logger.info("Test Case Completed")
        except Exception as e:
            logger.info("Test case Failed")
            logger.error(f"Error details: {e}")
            pytest.fail(f"Test case failed due to: {e}")

    def test_concatination(self,connect_sqlserverdb_engine2):
        logger.info("TC_06- concatenate  First_Name, Middle_Name, Last_Name column of 'EMP_DTS_SRC1' table into 'EMP_Name' column of 'EMP_DTS_TRG' table Format : (Last_Name First_Name Middle_Name)")
        try:
            logger.info("Fetching the data from source table")
            source_query="Select EMP_CODE,EMP_DOB,HIRE_DATE,Created_On,Modified_On  from EMP_DTS_TRG Where REFERENCE_TABLE = 'EMP_DTS_SRC1'"
            logger.info("Fetching the data from target table")
            target_query="Select EMP_CODE, (LAST_NAME + ' ' + FIRST_NAME + ' ' + ISNull (MIDDLE_NAME, '')) EMP_NAME from EMP_DTS_SRC1 Where EMP_SALARY >= 1000;"
            db_to_db_testing(source_query,connect_sqlserverdb_engine2,target_query,connect_sqlserverdb_engine2,'Name.csv')
            logger.info("Test case passed")

        except Exception as e:
            logger.error('TEst case failed')
            logger.error(f"Error details : {e}")
            pytest.fail(f'Test case failed due to {e}')




    def test_Gender_transformation(self, connect_sqlserverdb_engine2):
        logger.info("TC_07-To validate data conversion transformation of 'EMP_GENDER' column when 'EMP_GENDER' ='MALE' then 'M' when emp_gender=Femaele then 'f")
        try:
            logger.info("Fetching the source query")
            query_source = "select case when emp_gender='Male' then 'M' when emp_gender='Female' then 'F' else Null end emp_gender from EMP_DTS_SRC1 where EMP_SALARY>=1000"
            logger.info("Fetching the target query")
            target_query = "select emp_gender from EMP_DTS_TRG where REFERENCE_TABLE='EMP_DTS_SRC1'"
            db_to_db_testing(query_source,connect_sqlserverdb_engine2,target_query,connect_sqlserverdb_engine2,'Gender.csv')
        except Exception as e:
            logger.info(' Test case failde')
            logger.error(f"Error details: {e}")
            pytest.fail(f"Test case failed due to {e}")


    @pytest.mark.xfail
    def test_Gender_transformation_2(self, connect_sqlserverdb_engine2):
        logger.info("TC_08-To validate data conversion transformation of 'EMP_GENDER' column when 'EMP_GENDER' ='MALE' then 'M' when emp_gender=Femaele then 'f")
        try:
            logger.info("Fetching the source query")
            query_source = "select emp_code, emp_gender from EMP_DTS_SRC2 where EMP_SALARY>=1000"
            logger.info("Fetching the target query")
            target_query = "select emp_code,emp_gender from EMP_DTS_TRG where REFERENCE_TABLE='EMP_DTS_SRC2'"
            db_to_db_testing(query_source,connect_sqlserverdb_engine2,target_query,connect_sqlserverdb_engine2,'Gender.csv')
        except Exception as e:
            logger.info(' Test case failde')
            logger.error(f"Error details: {e}")
            pytest.fail(f"Test case failed due to {e}")

    # @pytest.mark.smoke
    # def test_null_records(self,connect_sqlserverdb_engine2):
    #     try:
    #         check_null_records_db('EMP_DTS_TRG',connect_sqlserverdb_engine2,'null.csv')
    #     except Exception as e:
    #         logger.error(f"Error Details: {e}")
    #         pytest.fail(f"Fail due to {e}")
    @pytest.mark.smoke
    def test_validate_experinece(self,connect_sqlserverdb_engine2):
        logger.info("Validate total years of experience from source to target")
        try:
            source_query="select emp_code,datediff(YEAR,hire_date,getdate())total_exp from EMP_DTS_SRC1 where EMP_SALARY>1000"
            target_query="select emp_code, TOTAL_EXP from EMP_DTS_TRG where REFERENCE_TABLE='EMP_DTS_SRC1'"
            db_to_db_testing(source_query,connect_sqlserverdb_engine2,target_query,connect_sqlserverdb_engine2,'year_exp.csv')
        except Exception as e:
            logger.error("Test case fail")
            logger.error(f"Error Details: {e}")
            pytest.fail(f'TEst casefail due to {e}')