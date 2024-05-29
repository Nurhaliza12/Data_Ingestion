import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Float, String

def get_dataframe():
    df = pd.read_csv("../dataset/brands.csv", sep=",")
    return df

def get_manipulate_data(df):
    df.dropna(inplace=True)
    df['brand_id'] = df['brand_id'].astype('int')
    df['name'] = df['name'].astype('string')
    return df

def get_postgres_conn():
    user = 'postgres'
    password = 'admin'
    host = 'localhost'
    database = 'mydb'
    port = 5433
    
    conn_string = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    engine = create_engine(conn_string)
    return engine

def load_to_postgres(engine, clean_data):
    df_schema = {
        'brand_id': Float,
        'name': String,
    }
    clean_data.to_sql(name='brands', con=engine, if_exists='replace', index=False, schema='public', dtype=df_schema, method=None, chunksize=5000)

df = get_dataframe()
print('-------------------------------------')
clean_data = get_manipulate_data(df)

postgres_conn = get_postgres_conn()
print(postgres_conn)
load_to_postgres(postgres_conn, clean_data)
