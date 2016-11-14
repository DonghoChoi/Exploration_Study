# -*- coding: utf-8 -*-
"""
Created on Tue Apr 26 10:52:46 2016

@author: donghochoi
"""
import sys
sys.path.append("../configs/")
import fitbit_credentials # two arrays: key, token
import fitbit_used
import server_config

import fitbit
from datetime import date,timedelta

import pandas as pd
from sshtunnel import SSHTunnelForwarder # for SSH connection
#from configs import server_config #import info1_server # server configuration class
#from .configs/server_config import study_2015_db # fitbit database configuration class
import pymysql.cursors # MySQL handling API

# fitbit app - rusensor
RUSENSOR_KEY= fitbit_credentials.rusensor_key
RUSENSOR_SECRET = fitbit_credentials.rusensor_secret

#print(RUSENSOR_KEY)

resource_steps = 'activities/steps'
resource_heart = 'activities/heart'
resource_sleep_timeInBed = 'sleep/timeInBed'
resource_sleep_minutesAsleep ='sleep/minutesAsleep'
resource_sleep_efficiency = 'sleep/efficiency'

# Load credentionals of ten devices 
USER_KEYS = fitbit_credentials.keys
USER_TOKENS = fitbit_credentials.tokens
#print(user_key[0])

# Dates: begin date is the second day of that batch, and end date is the day before the lab session.
batch_1_begin = date(2015,2,27)
batch_1_end = date(2015,3,11)
batch_2_begin = date(2015,3,24)
batch_2_end = date(2015,4,5)
batch_3_begin = date(2015,4,9)
batch_3_end = date(2015,4,21)
batch_4_begin = date(2015,7,23)
batch_4_end = date(2015,8,4)
batch_4_begin_008 = date(2015,7,27)
batch_4_end_008 = date(2015,8,8)
batch_4_begin_009 = date(2015,7,24)
batch_4_end_009 = date(2015,8,5)

#unauth_client = fitbit.Fitbit('<consumer_key>', '<consumer_secret>')
#unauth_client = fitbit.Fitbit(rusensor_key, rusensor_secret)
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)
              
# server tunneling
              
server = SSHTunnelForwarder(
        (server_config.info1_server['host'], 22),
        ssh_username = server_config.info1_server['user'],
        ssh_password = server_config.info1_server['password'],
        remote_bind_address =('127.0.0.1',3306))
        
server.start() 

connection = pymysql.connect(host='127.0.0.1', 
                             port=server.local_bind_port, 
                             user=server_config.study_2015_db['user'], 
                             password=server_config.study_2015_db['password'], 
                             db=server_config.study_2015_db['database'])
cursor = connection.cursor()


#------------------------- Steps data ------------------------------------

# Drop table if it already exist using execute() method.
cursor.execute("DROP TABLE IF EXISTS fitbit_step")
sql ="CREATE TABLE fitbit_step (batch int, deviceID int, date varchar(255), steps int);"
cursor.execute(sql)
cursor.execute("DROP TABLE IF EXISTS fitbit_intraday_step")
sql ="CREATE TABLE fitbit_intraday_step (batch int, deviceID int, date varchar(255), time varchar(255), steps int);"
cursor.execute(sql)


# Batch 1 - fitbit_step & fitbit_intraday_step
participants_num = fitbit_used.batch_1[0];
for i in range(participants_num):
    cur_id = fitbit_used.batch_1[i+1]
    temp_key = USER_KEYS[cur_id]
    temp_token = USER_TOKENS[cur_id]
    authd_client = fitbit.Fitbit(RUSENSOR_KEY,RUSENSOR_SECRET,resource_owner_key=temp_key,resource_owner_secret=temp_token)
    start_date = batch_1_begin
    end_date = batch_1_end
    #deviceID = "SS00" + str(cur_id)
    for cur_date in daterange(start_date,end_date):
        result = authd_client.intraday_time_series(resource=resource_steps, detail_level='1min', base_date=cur_date)
        date = result['activities-steps'][0]['dateTime']
        steps = int(result['activities-steps'][0]['value'])
        sql = "INSERT INTO fitbit_step VALUES ('1','"+ str(cur_id) + "','" + str(date) + "'," + str(steps) + ");"       
        cursor.execute(sql)  
        for j in range(1440):
            time = result['activities-steps-intraday']['dataset'][j]['time']
            min_steps = int(result['activities-steps-intraday']['dataset'][j]['value'])
            sql = "INSERT INTO fitbit_intraday_step VALUE('1','"+str(cur_id)+"','"+str(cur_date)+"','"+str(time)+"',"+str(min_steps)+");"
            cursor.execute(sql)
     
# Batch 2 - fitbit_step & fitbit_intraday_step
participants_num = fitbit_used.batch_2[0];
for i in range(participants_num):
    cur_id = fitbit_used.batch_2[i+1]
    temp_key = USER_KEYS[cur_id]
    temp_token = USER_TOKENS[cur_id]
    authd_client = fitbit.Fitbit(RUSENSOR_KEY,RUSENSOR_SECRET,resource_owner_key=temp_key,resource_owner_secret=temp_token)
    start_date = batch_2_begin
    end_date = batch_2_end
    for cur_date in daterange(start_date,end_date):
        result = authd_client.intraday_time_series(resource=resource_steps, detail_level='1min', base_date=cur_date)
        date = result['activities-steps'][0]['dateTime']
        steps = int(result['activities-steps'][0]['value'])
        sql = "INSERT INTO fitbit_step VALUES ('2','"+ str(cur_id) + "','" + str(date) + "'," + str(steps) + ");"       
        cursor.execute(sql)  
        for j in range(1440):
            time = result['activities-steps-intraday']['dataset'][j]['time']
            min_steps = int(result['activities-steps-intraday']['dataset'][j]['value'])
            sql = "INSERT INTO fitbit_intraday_step VALUE('2','"+str(cur_id)+"','"+str(cur_date)+"','"+str(time)+"',"+str(min_steps)+");"
            cursor.execute(sql)
            
# Batch 3 - fitbit_step & fitbit_intraday_step
participants_num = fitbit_used.batch_3[0];
for i in range(participants_num):
    cur_id = fitbit_used.batch_3[i+1]
    temp_key = USER_KEYS[cur_id]
    temp_token = USER_TOKENS[cur_id]
    authd_client = fitbit.Fitbit(RUSENSOR_KEY,RUSENSOR_SECRET,resource_owner_key=temp_key,resource_owner_secret=temp_token)
    start_date = batch_3_begin
    end_date = batch_3_end
    for cur_date in daterange(start_date,end_date):
        result = authd_client.intraday_time_series(resource=resource_steps, detail_level='1min', base_date=cur_date)
        date = result['activities-steps'][0]['dateTime']
        steps = int(result['activities-steps'][0]['value'])
        sql = "INSERT INTO fitbit_step VALUES ('3','"+ str(cur_id) + "','" + str(date) + "'," + str(steps) + ");"       
        cursor.execute(sql)  
        for j in range(1440):
            time = result['activities-steps-intraday']['dataset'][j]['time']
            min_steps = int(result['activities-steps-intraday']['dataset'][j]['value'])
            sql = "INSERT INTO fitbit_intraday_step VALUE('3','"+str(cur_id)+"','"+str(cur_date)+"','"+str(time)+"',"+str(min_steps)+");"
            cursor.execute(sql)
            
# Batch 4 - fitbit_step & fitbit_intraday_step
participants_num = fitbit_used.batch_4[0];
for i in range(participants_num):
    cur_id = fitbit_used.batch_4[i+1]
    temp_key = USER_KEYS[cur_id]
    temp_token = USER_TOKENS[cur_id]
    authd_client = fitbit.Fitbit(RUSENSOR_KEY,RUSENSOR_SECRET,resource_owner_key=temp_key,resource_owner_secret=temp_token)
    if cur_id == 8:
        start_date = batch_4_begin_008
        end_date = batch_4_end_008
    elif cur_id == 9:
        start_date = batch_4_begin_009
        end_date = batch_4_end_009
    else:
        start_date = batch_4_begin
        end_date = batch_4_end
    for cur_date in daterange(start_date,end_date):
        result = authd_client.intraday_time_series(resource=resource_steps, detail_level='1min', base_date=cur_date)
        date = result['activities-steps'][0]['dateTime']
        steps = int(result['activities-steps'][0]['value'])
        sql = "INSERT INTO fitbit_step VALUES ('4','"+ str(cur_id) + "','" + str(date) + "'," + str(steps) + ");"       
        cursor.execute(sql)  
        for j in range(1440):
            time = result['activities-steps-intraday']['dataset'][j]['time']
            min_steps = int(result['activities-steps-intraday']['dataset'][j]['value'])
            sql = "INSERT INTO fitbit_intraday_step VALUE('4','"+str(cur_id)+"','"+str(cur_date)+"','"+str(time)+"',"+str(min_steps)+");"
            cursor.execute(sql) 
            
# Close ssh server connection    
server.stop() 