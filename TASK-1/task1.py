import pandas as pd
#1. membaca data
# df  = pd.read_parquet('../dataset/yellow_tripdata_2023-01.parquet', engine="pyarrow")
df = pd.read_csv("../dataset/yellow_tripdata_2020-07.csv", sep=",")
#2. rename kolom tabel
print(df.dtypes)
df = df.rename(columns={"VendorID": "vendor_ID", "RatecodeID":"rate_code_ID", "PULocationID":"PU_location_ID", "DOLocationID": "DO_location_ID"})
print(df.dtypes)
#3. menampilkan top 10
top_10_passenger_count=df.nlargest(10, "passenger_count")[["vendor_ID","passenger_count","trip_distance", 
    "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
    "total_amount"]]
print(top_10_passenger_count)