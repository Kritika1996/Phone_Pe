# importing importing libraries
import json
import pandas as pd
import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine
import requests
import subprocess
import os
from urllib.parse import quote

#1 Aggregated State
path_1 = r"/Users/varun/pulse/data/aggregated/transaction/country/india/state/"
Aggregated_state_list = os.listdir(path_1)

#capitalizing the state
Aggregated_state_list_C = list(map(str.capitalize, Aggregated_state_list))
Aggregated_state_list_C

# Aggregated_transaction
col = {'State':[], 'Year':[], 'Quarter':[], 'Transaction_type':[], 'Transaction_count':[], 'Transaction_amount':[] }
for state in Aggregated_state_list_C:
    cur_state=path_1+state+'/'
    Aggregated_state_list_C=os.listdir(cur_state)

    for year in Aggregated_state_list_C:
       cur_year=cur_state+year+'/'
       agg_file_list=os.listdir(cur_year)

       for file in agg_file_list:
           cur_file=cur_year+file
           data=open(cur_file,'r')
           A=json.load(data)

           for i in A['data']['transactionData']:
               Name=i['name']
               count=i['paymentInstruments'][0]['count']
               amount=i['paymentInstruments'][0]['amount']
               col['Transaction_type'].append(Name)
               col['Transaction_count'].append(count)
               col['Transaction_amount'].append(amount)
               col['State'].append(state)
               col['Year'].append(year)
               col['Quarter'].append(int(file.strip('.json')))

Aggregated_transaction = pd.DataFrame(col)  

# Aggregated_user

path2 = r"/Users/varun/pulse/data/aggregated/user/country/india/state/"
Aggregated_user_list = os.listdir(path2)
col2 = {"State":[],"Year":[],"Quarter":[],"Brands":[],"Transaction_count":[],"Percentage":[]}

for state in Aggregated_user_list:
    cur_states = path2+state+"/"
    agg_year_list = os.listdir(cur_states)

    for year in agg_year_list:
        cur_years = cur_states+year+"/"
        agg_file_list = os.listdir(cur_years)

        for file in agg_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            C = json.load(data)

            try:

                for i in C["data"]["usersByDevice"]:
                    brand = i["brand"]
                    count = i["count"]
                    percentage = i["percentage"]
                    col2["Brands"].append(brand)
                    col2["Transaction_count"].append(count)
                    col2["Percentage"].append(percentage)
                    col2["State"].append(state)
                    col2["Year"].append(year)
                    col2["Quarter"].append(int(file.strip(".json")))

            except:
                pass

Aggregated_user = pd.DataFrame(col2)

# Map_user

path3 = r"C:\\Users\\varun\\pulse\\data\\map\\user\\hover\\country\\india\\state\\"
Map_user_list = os.listdir(path3)

col3 = {"State":[], "Year":[], "Quarter":[], "Districts":[], "RegisteredUser":[], "AppOpens":[]}

for state in Map_user_list:
    cur_states = path3+state+"/"
    map_year_list = os.listdir(cur_states)

    for year in map_year_list:
        cur_years = cur_states+year+"/"
        map_file_list = os.listdir(cur_years)

        for file in map_file_list:
            cur_files = cur_years+file
            
            try:
                with open(cur_files,"r") as data:
                    F = json.load(data)

                    for i in F['data']['hoverData'].items():
                        district = [0]
                        # converting list to string
                        district_string = ", ".join(map(str,district))
                        registereduser = i[1]["registeredUsers"]
                        appopens = i[1]["appOpens"]
                        col3["Districts"].append(district_string)
                        col3["RegisteredUser"].append(registereduser)
                        col3["AppOpens"].append(appopens)
                        col3["State"].append(state)
                        col3["Year"].append(year)
                        col3["Quarter"].append(int(file.strip(".json")))
            except (FileNotFoundError, json.JSONDecodeError,KeyError) as e:
                print(f"Error processing file{cur_files}:{e}")
                
            
Map_user = pd.DataFrame(col3)

# Top_transaction

path4 = r"C:\\Users\\varun\\pulse\\data\\top\\transaction\\country\\india\\state\\"
Top_tran_list = os.listdir(path4)

col4 = {"State":[], "Year":[], "Quarter":[], "Pincodes":[], "Transaction_count":[], "Transaction_amount":[]}
for state in Top_tran_list:
    cur_states = path4+state+"/"
    top_year_list = os.listdir(cur_states)
    
    for year in top_year_list:
        cur_years = cur_states+year+"/"
        top_file_list = os.listdir(cur_years)
        
        for file in top_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            H = json.load(data)

            for i in H["data"]["pincodes"]:
                entityName = i["entityName"]
                count = i["metric"]["count"]
                amount = i["metric"]["amount"]
                col4["Pincodes"].append(entityName)
                col4["Transaction_count"].append(count)
                col4["Transaction_amount"].append(amount)
                col4["State"].append(state)
                col4["Year"].append(year)
                col4["Quarter"].append(int(file.strip(".json")))

Top_transaction = pd.DataFrame(col4)

# Top users

path5 = r"C:\\Users\\varun\\pulse\\data\\top\\user\\country\\india\\state\\"
Top_user_list = os.listdir(path5)

col5 = {"State":[], "Year":[], "Quarter":[], "Pincodes":[], "RegisteredUser":[]}

for state in Top_user_list:
    cur_states = path5+state+"/"
    top_year_list = os.listdir(cur_states)

    for year in top_year_list:
        cur_years = cur_states+year+"/"
        top_file_list = os.listdir(cur_years)

        for file in top_file_list:
            cur_files = cur_years+file
            data = open(cur_files,'r')
            U = json.load(data)

            for i in U["data"]["pincodes"]:
                name = i["name"]
                registeredusers = i["registeredUsers"]
                col5["Pincodes"].append(name)
                col5["RegisteredUser"].append(registereduser)
                col5["State"].append(state)
                col5["Year"].append(year)
                col5["Quarter"].append(int(file.strip(".json")))

Top_user = pd.DataFrame(col5)

# connecting to sql
mydb = mysql.connector.connect(
    host = "localhost",
    user = "root",
    database = "Phone_pe",
    password = "Root@123",
    autocommit = True
)
# creating cursor
cursor = mydb.cursor()

# closing the cursor and database
cursor.close()
mydb.close()

# url-code the password
password = quote('Root@123')
connection_string = f'mysql+mysqlconnector://root:{password}@localhost/phone_pe'
engine = create_engine(connection_string, echo=False)

# Testing connection
try:
    with engine.connect() as connection:
        print("sqlalchemy connection successful")
except Exception as e:
    print(f"sqlalchemy connection failed:{e}")

# Inserting data using sqlalchemy into sql database
# 1.Aggregated_transaction
Aggregated_transaction.to_sql('aggregated_transaction', engine,if_exists='replace', index = False,
                              dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                                       'Year': sqlalchemy.types.Integer, 
                                       'Quater': sqlalchemy.types.Integer, 
                                       'Transaction_type': sqlalchemy.types.VARCHAR(length=50), 
                                       'Transaction_count': sqlalchemy.types.Integer,
                                       'Transaction_amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})

# 2. Aggregated_user
Aggregated_user.to_sql('aggregated_user', engine, if_exists='replace',index = False,
                       dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                                 'Year': sqlalchemy.types.Integer, 
                                 'Quater': sqlalchemy.types.Integer,
                                 'Brands': sqlalchemy.types.VARCHAR(length=50), 
                                 'User_Count': sqlalchemy.types.Integer, 
                                 'User_Percentage': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})

# 3. Map_user
Map_user.to_sql('map_user', engine,if_exists='replace', index =False,
                dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                        'Year': sqlalchemy.types.Integer, 
                        'Quater': sqlalchemy.types.Integer, 
                        'District': sqlalchemy.types.TEXT(), 
                        'Registered_User': sqlalchemy.types.Integer,
                        'AppOpens':sqlalchemy.types.Integer,})

# 4. Top_transaction
Top_transaction.to_sql('top_transaction',engine,if_exists='replace',index=False,
                       dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                                'Year': sqlalchemy.types.Integer, 
                                'Quater': sqlalchemy.types.Integer,   
                                'District_Pincode': sqlalchemy.types.Integer,
                                'Transaction_count': sqlalchemy.types.Integer, 
                                'Transaction_amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})

# 5. Top_user
Top_user.to_sql('top_user',engine,if_exists='replace',index=False,
                dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                          'Year': sqlalchemy.types.Integer, 
                          'Quater': sqlalchemy.types.Integer,                           
                          'District_Pincode': sqlalchemy.types.Integer, 
                          'Registered_User': sqlalchemy.types.Integer,})