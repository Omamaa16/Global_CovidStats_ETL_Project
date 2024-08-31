from google.cloud import storage as st
import requests
import json
import pandas as pd
from google.cloud import storage as st

countries = [{"iso": "CHN", "name": "China"},
            {"iso": "JPN", "name": "Japan"},
            {"iso": "CAN", "name": "Canada"},
            {"iso": "FRA", "name": "France"},
            {"iso": "DEU", "name": "Germany"},
            {"iso": "MEX", "name": "Mexico"},
            {"iso": "ITA", "name": "Italy"},
            {"iso": "ARE", "name": "United Arab Emirates"},
            {"iso": "NPL", "name": "Nepal"},
            {"iso": "AUS", "name": "Australia"},
            {"iso": "CZE", "name": "Czechia"},
            {"iso": "QAT", "name": "Qatar"},
            {"iso": "UKR", "name": "Ukraine"},
            {"iso": "HUN", "name": "Hungary"},
            {"iso": "PRT", "name": "Portugal"},
            {"iso": "JAM", "name": "Jamaica"},
            {"iso": "IRL", "name": "Ireland"},
            {"iso": "PER", "name": "Peru"},
            {"iso": "BOL", "name": "Bolivia"},
            {"iso": "POL", "name": "Poland"},
            {"iso": "PSE", "name": "West Bank and Gaza"},
            {"iso": "ZAF", "name": "South Africa"},
            {"iso": "MLT", "name": "Malta"},
            {"iso": "CYP", "name": "Cyprus"},
            {"iso": "TUR", "name": "Turkey"},
            {"iso": "CUB", "name": "Cuba"},
            {"iso": "KEN", "name": "Kenya"},
            {"iso": "GHA", "name": "Ghana"},
            {"iso": "CAF", "name": "Central African Republic"},
            {"iso": "SYR", "name": "Syria"},
            {"iso": "LBY", "name": "Libya"},
            {"iso": "ALB", "name": "Albania"},
            {"iso": "TGO", "name": "Togo"},
            {"iso": "VAT", "name": "Holy See"}]

combined_data = pd.DataFrame()

#fetch data
def fetch_data(country_iso, country_name):
    url = "https://covid-19-statistics.p.rapidapi.com/reports"
    querystring = {"iso": country_iso, "region_name": country_name}
    headers = {
        "x-rapidapi-key": "98d444ebfcmshf6699d6fcfbb51dp1020b0jsn71f45312e125",
        "x-rapidapi-host": "covid-19-statistics.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers, params=querystring)
    myresponse = response.json()
    
    region = []
    province = []
    confirmed_cases = []
    deaths = []
    recovered = []
    fatality_rate = []

    for i in range(len(myresponse["data"])):
        region.append(myresponse["data"][i]["region"]["iso"]),
        province.append(myresponse["data"][i]["region"]["province"]),
        confirmed_cases.append(myresponse["data"][i]["confirmed"]),
        deaths.append(myresponse["data"][i]["deaths"]),
        recovered.append(myresponse["data"][i]["recovered"]),
        fatality_rate.append(myresponse["data"][i]["fatality_rate"])

    data_df = pd.DataFrame({
        "country": country_name,
        "region": region,
        "province": province,
        "confirmed_cases": confirmed_cases,
        "deaths": deaths,
        "recovered": recovered,
        "fatality_rate": fatality_rate
    })
    return data_df

#combining the data
for country in countries:
    country_data = fetch_data(country["iso"], country["name"])
    combined_data = pd.concat([combined_data, country_data], ignore_index=True)


#data cleaning
print(combined_data.isnull().sum())
print(combined_data.duplicated())
combined_data[["country", "region", "province", "confirmed_cases", "deaths", "recovered", "fatality_rate"]] = combined_data[["country", "region", "province", "confirmed_cases", "deaths", "recovered", "fatality_rate"]].astype(str)
print(combined_data.dtypes)


#save as a csv
covid_csv = combined_data.to_csv("covid_data.csv", index=False)
print("Data combined and saved as csv")

#uploading to gcs
client = st.Client()
name_bucket = "covid-data-countries"
source = "covid_data.csv"
destination = "covid_data.csv"

bucket = client.bucket(name_bucket)
blob = bucket.blob(destination)
blob.upload_from_filename(source)
print("Uploaded to the GCS")
