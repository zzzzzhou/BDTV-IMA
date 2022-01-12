import requests
import pandas as pd 
import json
import time
from pymongo import MongoClient
import urllib

#CSV file with state names and their codes
state_code = pd.read_csv("State_code.csv") 
#Extracting the 'state' and 'code' columns
code_dict = state_code[["State", "Code"]].to_dict('index')

#CSV file with names of county in each state
df = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/UID_ISO_FIPS_LookUp_Table.csv')
#Extracting the 'state' and 'county' column and relevant rows
county_df = df.iloc[1070:].reset_index()[["Admin2", "Province_State"]] 

'''
Since the county dataframe only contains the state and county name, 
while the API requires state code and county name,
a list of state code is created and appended to the county dataframe.
'''
#Empty list to store the state code associated with each county
code_list = []
#looping through the state of each county and appending the state code 
for state in county_df['Province_State']:
    for i in range(len(code_dict)):
        if state == code_dict[i]['State']:
            code_list.append(code_dict[i]['Code'])

#Joining state code to county dataframe            
code_df = pd.DataFrame({'code': code_list})
county_df = county_df.join(code_df)
county_dict = county_df.to_dict('index')

response_list = [] #list to store API responses

#Looping through each county to generate API endpoints, then storing it in the response list 
response_list = []
for i in range(len(county_dict)):
    if i != 321: #exclude DC
        covid_county_code_url = "https://localcoviddata.com/covid19/v1/cases/jhu?state=" + county_dict[i]["code"] + "&daysInPast=7&county=" + county_dict[i]["Admin2"]
        r = requests.get(covid_county_code_url)
        response_list.append(r.json())
	
#Connecting to mongodb client
con_str = "mongodb+srv://zzd:zhouzhedi0928@cluster0.lae6q.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
client = MongoClient(con_str)
newdb = client["US_covid"]
newcollect = newdb["covid_cases_county_level"]

#Uploading data onto mongodb
for key, county in enumerate(response_list):
    newcollect.insert_one({str(key): county})