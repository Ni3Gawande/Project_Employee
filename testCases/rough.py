import pandas as pd
from sqlalchemy import create_engine
engine = create_engine(
    'mssql+pyodbc://LAPTOP-J82A4UMN/fun?driver=SQL+Server+Native+Client+11.0&trusted_connection=yes')
current=pd.read_csv("../TestData/metadatak9.csv")
print(current)
query="Select TABLE_NAME,COLUMN_NAME,CHARACTER_MAXIMUM_LENGTH,ORDINAL_POSITION from information_schema.columns where table_name='k9'"
ex=pd.read_sql(query,engine)

print(ex)

result=pd.merge(current,ex,how='outer',indicator=True).query("_merge!='both'")
print(result)