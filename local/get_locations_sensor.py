#!/usr/bin/python
# Author: Dongho Choi

import json
import sqlite3 as lite
import time
import pandas as pd
from collections import defaultdict
import glob
from time import localtime, strftime
import string
import os.path
import datetime
import sys
from sshtunnel import SSHTunnelForwarder # for SSH connection
import pymysql.cursors # MySQL handling API

import sys
sys.path.append("./configs/")
import server_config # (1) info2_server (2) exploration_db
import directory_config # (1) base_directory (2) raw_data_directory


device_imei = {} # a set: device_imei[device] = IMEI
device_imei_list = []
num_device = 0

def get_imei_list(con):

    cur1 = con.cursor()
    cur1.execute("PRAGMA temp_store = 2")

    data_imei = cur1.execute("SELECT device, value FROM data WHERE probe LIKE '%Hardware%'")

    for x in data_imei:
        j = json.loads(x[1])  # x[1]: content in value column from the query results
        #print(j)
        if 'deviceId' in j:
            imei_num = j['deviceId']
            device_imei[x[0]] = imei_num  # create an array of (1) device (number) and (2) imei number
        #print("device number:%s, imei number:%s" % (x[0],imei_num))
    num_device = len(device_imei)
    #print (len(device_imei)) # length of the set 'device_imei'


if __name__ == "__main__":


    # Server connection
    server = SSHTunnelForwarder(
        (server_config.info2_server['host'], 22),
        ssh_username=server_config.info2_server['user'],
        ssh_password=server_config.info2_server['password'],
        remote_bind_address=('127.0.0.1', 3306))

    server.start()
    #print(server.local_bind_port)

    connection = pymysql.connect(host='127.0.0.1',
                                 port=server.local_bind_port,
                                 user=server_config.exploration_db['user'],
                                 password=server_config.exploration_db['password'],
                                 db=server_config.exploration_db['database'])
    connection.autocommit(True)
    cursor = connection.cursor()
    print("MySQL connection established.")

    all_data_db = directory_config.base_directory + "1114to1115/data/all_data.db"
    print(all_data_db)

    # Check if the all_data database file exists
    if (os.path.isfile(all_data_db) == False):
        print("All_data.db does not exist. Please run the funf scripts first.")
        sys.exit()

    else:
        # connecting to the database file
        main_con = lite.connect(all_data_db)

        # extract imei list from the database
        get_imei_list(main_con) # will get the set 'device_imei'
        #print ("number of imei: %d" % len(device_imei)) # length of the set 'device_imei'
        #print(device_imei)
        device_imei_list = list(device_imei.values())
        #for i in len(device_imei_list):
        #    print ("imei: %s" % (device_imei_list[i]))
        #    i+=1

        # import the database that contains location data only as a dataframe
        df = pd.read_sql_query("""SELECT device, timestamp, value FROM data WHERE probe LIKE '%Location%';""", main_con)
        print("number of rows in database: %d" % len(df.index))

        # add imei data as one column in the dataframe
        df["imei"]=""

        for i in range(0,len(df.index)):
            df.at[i,"imei"] = device_imei[df.at[i,"device"]]


        # add additional columns realted to time:
        df = pd.concat([df,pd.DataFrame(columns=['date_time','year','month','day','hour','minute','second','provider','latitude','longitude','speed'])])
        for i in range(0,len(df.index)):
            df.at[i, "date_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(df.at[i,"timestamp"]))
            df.at[i, "year"] = int(time.strftime("%Y",time.localtime(df.at[i,"timestamp"])))
            df.at[i, "month"] = int(time.strftime("%m", time.localtime(df.at[i, "timestamp"])))
            df.at[i, "day"] = int(time.strftime("%d", time.localtime(df.at[i, "timestamp"])))
            df.at[i, "hour"] = int(time.strftime("%H", time.localtime(df.at[i, "timestamp"])))
            df.at[i, "minute"] = int(time.strftime("%M", time.localtime(df.at[i, "timestamp"])))
            df.at[i, "second"] = int(time.strftime("%S", time.localtime(df.at[i, "timestamp"])))

            j = json.loads(df.at[i, "value"])
            df.at[i, "provider"] = j['mProvider']
            df.at[i, "latitude"] = float(j['mLatitude'])
            df.at[i, "longitude"] = float(j['mLongitude'])
            df.at[i, "speed"] = float(j['mSpeed'])

            df.at[i, "timestamp"] = float(df.at[i, "timestamp"])

        # pick the first data point from each hour
        for i in range(0,len(device_imei_list)):
            current_imei = device_imei_list[i]
            temp_df = df[(df['imei'] == current_imei) & (df['month'] >=  11)]
            print("current imei: %s, number of rows: %d" % (current_imei, len(temp_df.index)))
            if ((current_imei=="867290026305689") | (current_imei=="867290026305697")):
                current_userID = 21
            else:
                sql = ("SELECT userID FROM recruits WHERE imei = '"+str(current_imei)+"';")
                cursor.execute(sql)
                result = cursor.fetchone()
                print("userID: %d" % (result))
                current_userID = result[0]

            temp_df = temp_df.sort_values(['timestamp']) # sort by ascending timestamp

            if(len(temp_df.index)==0):
                continue

            target_df = temp_df.iloc[[0]]
            #print(target_df)
            current_month = target_df.iloc[0]['month']
            current_day = target_df.iloc[0]['day']
            current_hour = target_df.iloc[0]['hour']
            current_minute = target_df.iloc[0]['minute']
            #print("current month:%d, day:%d, hour:%d, minute:%d" % (current_month,current_day,current_hour,current_minute))

            if (len(temp_df.index)>0):
                for j in range(1,len(temp_df.index)):
                    temp_month = temp_df.iloc[j]['month']
                    temp_day = temp_df.iloc[j]['day']
                    temp_hour = temp_df.iloc[j]['hour']
                    temp_minute = temp_df.iloc[j]['minute']
                    #print(temp_month,temp_day,temp_hour,temp_minute)
                    #if((temp_month>current_month) | (temp_day>current_day) | (temp_hour>current_hour) | (temp_minute>current_minute)):
                    if((temp_month>current_month) | (temp_day>current_day) | (temp_hour>current_hour)):

                        current_month = temp_month
                        current_day = temp_day
                        current_hour = temp_hour
                        current_minute = temp_minute
                        target_df = target_df.append(temp_df.iloc[[j]])
                        #print("update")

                print("number of rows in target for current imei(%s):%d" % (current_imei,len(target_df.index)))
                #print(target_df)

            for k in range(0,len(target_df.index)):
                m_device = target_df.iloc[k]['device']
                m_timestamp = target_df.iloc[k]['timestamp']
                m_date_time = target_df.iloc[k]['date_time']
                m_year = target_df.iloc[k]['year']
                m_month = target_df.iloc[k]['month']
                m_day = target_df.iloc[k]['day']
                m_hour = target_df.iloc[k]['hour']
                m_minute = target_df.iloc[k]['minute']
                m_provider = target_df.iloc[k]['provider']
                m_latitude = target_df.iloc[k]['latitude']
                m_longitude = target_df.iloc[k]['longitude']
                m_speed = target_df.iloc[k]['speed']
                m_imei_timestamp = str(current_imei)+'_'+str(m_timestamp)
                m_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                sql = "INSERT INTO locations_all (inserted_time,imei_timestamp,imei,userID,device,timestamp,date_time,year,month,day,hour,minute,provider,latitude,longitude,speed) " \
                      "VALUES ('"+str(m_now)+"','"+str(m_imei_timestamp)+"','"+str(current_imei)+"','"+str(current_userID)+"','"+str(m_device)+"','"+\
                      str(m_timestamp)+"','"+str(m_date_time)+"','"+str(m_year)+"','"+str(m_month)+"','"+str(m_day)+"','"+str(m_hour)+"','"+\
                      str(m_minute)+"','"+str(m_provider)+"','"+str(m_latitude)+"','"+str(m_longitude)+"','"+str(m_speed)+\
                      "') ON DUPLICATE KEY UPDATE device = '"+str(m_device)+"';"
                print(sql)
                cursor.execute(sql)

                # insert dataframe into MySQL DB

        print("end")

    server.stop()




