import pytest
from sqlalchemy import create_engine
from Configurations.config import *

@pytest.fixture()
def connect_sqlserverdb_engine():
    engine = create_engine(
        'mssql+pyodbc://LAPTOP-J82A4UMN/fun?driver=SQL+Server+Native+Client+11.0&trusted_connection=yes')
    sqlserver = engine.connect()
    yield sqlserver
    sqlserver.close()

@pytest.fixture()
def connect_mysqldb_engine():
    engine = create_engine(
        f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')

    mysql_engine=engine.connect()
    yield mysql_engine
    mysql_engine.close()

@pytest.fixture()
def connect_sqlserverdb_engine2():
    engine = create_engine(
        'mssql+pyodbc://LAPTOP-J82A4UMN/EMP_ETL?driver=SQL+Server+Native+Client+11.0&trusted_connection=yes')
    sqlserver = engine.connect()
    yield sqlserver
    sqlserver.close()
