import requests
import pandas as pd 
import json
import time
from pymongo import MongoClient
import urllib

state_code = pd.read_csv("State_code.csv") #CSV file with state names and their codes

response_list = [] #list to store API responses


#Looping through each state to generate API endpoints, then storing it in the response list 
for i in range(len(state_code)):
    if i != 8: #exclude DC
        covid_state_code_url = "https://localcoviddata.com/covid19/v1/cases/jhu?state=" + state_code["Code"][i] + "&daysInPast=7"
        r = requests.get(covid_state_code_url)
        response_list.append(r.json())
	
#Connecting to mongodb client
con_str = "mongodb+srv://zzd:zhouzhedi0928@cluster0.lae6q.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
client = MongoClient(con_str)
newdb = client["US_covid"]
newcollect = newdb["covid_cases_state_level"]

#Uploading data onto mongodb
for key, state in enumerate(response_list):
    newcollect.insert_one({str(key): state})