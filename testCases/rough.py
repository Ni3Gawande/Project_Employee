import pandas as pd
from sqlalchemy import create_engine,text
engine = create_engine(
    'mssql+pyodbc://LAPTOP-J82A4UMN/fun?driver=SQL+Server+Native+Client+11.0&trusted_connection=yes')

# current=pd.read_csv("../TestData/metadatak9.csv")
# print(current)
# query="Select TABLE_NAME,COLUMN_NAME,CHARACTER_MAXIMUM_LENGTH,ORDINAL_POSITION from information_schema.columns where table_name='k9'"
# ex=pd.read_sql(query,engine)
#
# print(ex)
#
# result=pd.merge(current,ex,how='outer',indicator=True).query("_merge!='both'")
# print(result)











###### DML AND DQL in this way we can perform ##################

#
# def query():
#     # Query to drop the 'state' column from the 'k9' table
#     query = text("""ALTER TABLE k9 DROP COLUMN age""")
#
#     # query = text("""ALTER TABLE {table_name} DROP COLUMN {column_name}""")
#
#     mysqlserver = engine.connect()
#     mysqlserver.execute(query)  # Execute the drop column query
#     mysqlserver.commit()
#
# query()

#
# def query2():
#     # Query to drop the 'state' column from the 'k9' table
#     query = text("""ALTER TABLE k9 add age int,state varchar(30)""")
#
#     # query = text("""ALTER TABLE {table_name} DROP COLUMN {column_name}""")
#
#     mysqlserver = engine.connect()
#     mysqlserver.execute(query)  # Execute the drop column query
#     mysqlserver.commit()
#
#
# query2()
#
# query="select * from information_schema.columns where table_name='k9'"
# result=pd.read_sql(query,engine)
# print(result)
# print(result['COLUMN_NAME'])

# def query2():
#     # Query to drop the 'state' column from the 'k9' table
#     query = text("""insert into k9 values(51,'Meena','Pune',28,'MAh'),(52,'Amit','awar','29','Mah')""")
#
#     # query = text("""ALTER TABLE {table_name} DROP COLUMN {column_name}""")
#
#     mysqlserver = engine.connect()
#     mysqlserver.execute(query)  # Execute the drop column query
#     mysqlserver.commit()
#
#
# query2()

# def query2():
#     # Query to drop the 'state' column from the 'k9' table
#     query = text("""
#     create table empty2(eid int,ename varchar(20))
#
#     """)
#
#     # query = text("""ALTER TABLE {table_name} DROP COLUMN {column_name}""")
#
#     mysqlserver = engine.connect()
#     mysqlserver.execute(query)  # Execute the drop column query
#     mysqlserver.commit()
#
#
# query2()
