import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import String, DateTime, Integer, Float

def get_dataframe():
    df = pd.read_csv("../dataset/products.csv", sep=",")
    return df

def get_manipulate_data(df):
    df.dropna(inplace=True)
    df['product_id'] = df['product_id'].astype('int')
    df['brand_id'] = df['brand_id'].astype('float')
    df['name'] = df['name'].astype('string')
    df['price'] = df['price'].astype('float')
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
        'product_id': Integer,
        'brand_id' : Float,
        'name' : String,
        'price' : Float
    }
    clean_data.to_sql(name='products', con=engine, if_exists='replace', index=False, schema='public', dtype=df_schema, method=None, chunksize=5000)

df = get_dataframe()
print('-------------------------------------')
clean_data = get_manipulate_data(df)

postgres_conn = get_postgres_conn()
print(postgres_conn)
load_to_postgres(postgres_conn, clean_data)
