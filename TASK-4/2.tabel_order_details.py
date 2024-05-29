import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import String, DateTime, Integer

def get_dataframe():
    df = pd.read_csv("../dataset/order_details.csv", sep=",")
    return df

def get_manipulate_data(df):
    df.dropna(inplace=True)
    df['order_detail_id'] = df['order_detail_id'].astype('int')
    df['order_id'] = df['order_id'].astype('int')
    df['product_id'] = df['product_id'].astype('int')
    df['quantity'] = df['quantity'].astype('int')
    dr['price'] = df['price'].astype('int')
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
        'order_detail_id': Integer,
        'order_id' : Integer,
        'product_id' : integer,
        'price' : Float
    }
    clean_data.to_sql(name='order_details', con=engine, if_exists='replace', index=False, schema='public', dtype=df_schema, method=None, chunksize=5000)

df = get_dataframe()
print('-------------------------------------')
clean_data = get_manipulate_data(df)

postgres_conn = get_postgres_conn()
print(postgres_conn)
load_to_postgres(postgres_conn, clean_data)
