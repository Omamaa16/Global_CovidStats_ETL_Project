import requests
import json
import pandas as pd
from google.cloud import storage as st

url = "https://covid-19-statistics.p.rapidapi.com/regions"

headers = {
	"x-rapidapi-key": "98d444ebfcmshf6699d6fcfbb51dp1020b0jsn71f45312e125",
	"x-rapidapi-host": "covid-19-statistics.p.rapidapi.com"
}

response = requests.get(url, headers=headers)
my_response = response.json()
#print(my_response)

#extracting pretty print
my_prettyresponse = json.dumps(my_response, indent=4)
#print(my_prettyresponse)

#extracting the useful data
iso = []
name = []
for i in range(len(my_response["data"])):
    iso_data = my_response["data"][i]["iso"]
    iso.append(iso_data)
    name_data = my_response["data"][i]["name"]
    name.append(name_data)

#transforming into the dataframe
df_data = pd.DataFrame({"iso":iso, "name":name})

#cleaning the data
print(df_data.isnull().sum()) #to check for the null values
print(df_data.duplicated()) #to check for the duplicates
df_data["iso"] = df_data["iso"].astype(str)
df_data["name"] = df_data["name"].astype(str)
print(df_data.dtypes)

#dump into the csv format
region_info = df_data.to_csv("region_data.csv", index=False)
print("succesffuly dumped the data")

#uploading the data to the GCS
client = st.Client()
name_bucket = "metadata_covid"
source = "region_data.csv"
destination = "region_data.csv"

bucket = client.bucket(name_bucket)

blob = bucket.blob(destination)
blob.upload_from_filename(source)

print("uploaded to the GCS")









    