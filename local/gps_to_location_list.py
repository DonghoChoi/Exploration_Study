#!/usr/bin/python
# Author: Dongho Choi

import os.path
import datetime
import math
import time
import sys
import itertools
import pandas as pd
import numpy as np
from sshtunnel import SSHTunnelForwarder # for SSH connection
import pymysql.cursors # MySQL handling API
from geopy.distance import vincenty
import sys
sys.path.append("./configs/")
#sys.path.append("/Users/donghochoi/Documents/Work/Exploration_Study/Dissertation/Code/local/configs/")
import server_config # (1) info2_server (2) exploration_db
import matplotlib.pyplot as plt
import seaborn as sns

datetimeFormat = '%Y-%m-%d %H:%M:%S'
close_distance_cut = 40

def is_location_close(location_a, location_b):
    #print("Distance:", vincenty(location_a, location_b).meters)
    if (vincenty(location_a, location_b).meters <= close_distance_cut):
        return True
    else:
        return False

def find_location(current_location,df_user_location_list): # Return -1 when no close location existing, otherwise the location
    for i in range(0, len(df_user_location_list)):
        #print("index in df_location_list", i)
        temp_location = (df_user_location_list.iloc[i]['latitude'],df_user_location_list.iloc[i]['longitude'])
        if (is_location_close(current_location, temp_location) == True):
            #print("FOUND ONE CLOSE LOCATION")
            return df_user_location_list.iloc[i]['locationID']
    #print("No match, returning -1")
    return -1

def get_center_of_mass(user_location_list):
    x = 0
    y = 0
    visit_sum = user_location_list['visit_times'].sum()
    for i in range(0,len(user_location_list)):
        x = x + user_location_list.iloc[i]['visit_times'] * user_location_list.iloc[i]['latitude']
        y = y + user_location_list.iloc[i]['visit_times'] * user_location_list.iloc[i]['longitude']

    x = x/visit_sum
    y = y/visit_sum

    return [x, y]

if __name__ == "__main__":

    # READ DATA FROM SERVER
    #read_Data_from_Server()
    # Server connection
    server = SSHTunnelForwarder(
        (server_config.info2_server['host'], 22),
        ssh_username=server_config.info2_server['user'],
        ssh_password=server_config.info2_server['password'],
        remote_bind_address=('127.0.0.1', 3306))

    server.start()

    connection = pymysql.connect(host='127.0.0.1',
                                 port=server.local_bind_port,
                                 user=server_config.exploration_db['user'],
                                 password=server_config.exploration_db['password'],
                                 db=server_config.exploration_db['database'])
    connection.autocommit(True)
    cursor = connection.cursor()
    print("MySQL connection established.")

    # Get individual data
    df_individual_data = pd.read_sql('SELECT * FROM individual_data', con=connection)
    print("Individual data READ")

    # Get the participants list from the table of 'final_participants'
    df_participants = pd.read_sql('SELECT * FROM final_participants', con=connection)
    print("Participants Table READ")

    # Get locations_all table: importing all locations that participants visited.
    df_locations_all = pd.read_sql(
        "SELECT userID,date_time,latitude,longitude,timestamp FROM locations_all WHERE (userID!=5001)",
        con=connection)
    print("Locations Table READ")

    # READ AND FILL THE PARTICIPANTS LIST WITH COMBINATIONS
    participants_list = df_participants['userID'].tolist()
    num_participants = len(participants_list) # number of participants
    print('number of participants:{0}'.format(num_participants))

    ## POPULATE VISITS OF PARTICIPANTS
    for i in range(0, num_participants-1):
    #for i in range(0,1):
        df_user_visits = pd.DataFrame(columns=('userID', 'visit_start', 'visit_end', 'dwellTime', 'latitude', 'longitude'))
        current_userID = participants_list[i]
        df_temp_locations = df_locations_all.loc[df_locations_all['userID'] == current_userID] # location list of a particular user
        df_temp_locations = df_temp_locations.sort_values(by='timestamp')
        current_location = (df_temp_locations.iloc[0]['latitude'],df_temp_locations.iloc[0]['longitude']) # the first line of the list
        visit_start = datetime.datetime.strptime(df_temp_locations.iloc[0]['date_time'],datetimeFormat)
        #print("visit_start:",visit_start)
        visit_end = datetime.datetime.strptime(df_temp_locations.iloc[0]['date_time'], datetimeFormat)
        #print("visit_end:",visit_end)
        for j in range(1,len(df_temp_locations)-1):
            if (visit_start + datetime.timedelta(minutes=55) >  datetime.datetime.strptime(df_temp_locations.iloc[j]['date_time'], datetimeFormat)): # when time interval until next record is too small..
                #print("too close")
                continue
            else:
                temp_location = (df_temp_locations.iloc[j]['latitude'],df_temp_locations.iloc[j]['longitude'])
                #print("distance:",vincenty(current_location, temp_location).meters)
                if (vincenty(current_location, temp_location).meters <= close_distance_cut): # When seen the user stays nearby
                    #print("SAME LOCATION")
                    visit_end = datetime.datetime.strptime(df_temp_locations.iloc[j]['date_time'],datetimeFormat)
                    #print("visit_end update:",visit_end)
                else:
                    #print("MOVED TO NEW LOCATION")
                    df_temp_visits = pd.DataFrame(columns=('userID', 'visit_start', 'visit_end', 'dwellTime', 'latitude', 'longitude'))
                    df_temp_visits.set_value(0,'userID', current_userID)
                    df_temp_visits.set_value(0,'visit_start', visit_start)
                    df_temp_visits.set_value(0,'visit_end', visit_end)
                    df_temp_visits.set_value(0,'dwellTime', visit_end-visit_start)
                    df_temp_visits.set_value(0,'latitude', current_location[0])
                    df_temp_visits.set_value(0,'longitude', current_location[1])
                    df_user_visits  = df_user_visits.append(df_temp_visits)
                    current_location = (df_temp_locations.iloc[j]['latitude'], df_temp_locations.iloc[j]['longitude'])
                    visit_start = datetime.datetime.strptime(df_temp_locations.iloc[j]['date_time'],
                                                             datetimeFormat)
                    visit_end = datetime.datetime.strptime(df_temp_locations.iloc[j]['date_time'], datetimeFormat)

        #print(df_user_visits)

        df_user_location_list = pd.DataFrame(columns=('userID','locationID','latitude','longitude','visit_times','spent_time'))

        df_temp_location = pd.DataFrame(columns=('userID', 'locationID', 'latitude', 'longitude'))
        df_temp_location.set_value(0, 'userID', current_userID)
        df_temp_location.set_value(0, 'locationID', 0)
        df_temp_location.set_value(0, 'latitude', df_user_visits.iloc[0]['latitude'])
        df_temp_location.set_value(0, 'longitude', df_user_visits.iloc[0]['longitude'])
        df_temp_location.set_value(0, 'visit_times', 1)
        df_temp_location.set_value(0, 'spent_time', df_user_visits.iloc[0]['dwellTime'])
        df_user_location_list = df_user_location_list.append(df_temp_location)

        for k in range(1,len(df_user_visits)): # To populate the user's location list with visit_times and spent_time
            current_location = (df_user_visits.iloc[k]['latitude'],df_user_visits.iloc[k]['longitude'])
            same_location = find_location(current_location,df_user_location_list)
            #print("value of same location:",same_location)
            if (same_location == -1): # if there is no place close to the current place
                #print("same_location = -1")
                df_temp_location = pd.DataFrame(columns=('userID','locationID','latitude','longitude'))
                df_temp_location.set_value(0, 'userID', current_userID)
                df_temp_location.set_value(0, 'locationID', len(df_user_location_list))
                df_temp_location.set_value(0, 'latitude', current_location[0])
                df_temp_location.set_value(0, 'longitude', current_location[1])
                df_temp_location.set_value(0, 'visit_times', 1)
                df_temp_location.set_value(0, 'spent_time', df_user_visits.iloc[k]['dwellTime'])
                df_user_location_list = df_user_location_list.append(df_temp_location)

            else: # when current location can be perceived as the found 'same location'
                #print("same_location = :",same_location)
                val_visit_times = df_user_location_list.iloc[same_location]['visit_times']
                val_spent_time = df_user_location_list.iloc[same_location]['spent_time']
                #print("previous visit_times of locationID {0}: {1} becomes to {2}".format(same_location, val_visit_times, val_visit_times + 1))
                df_user_location_list.iloc[same_location, df_user_location_list.columns.get_loc('visit_times')]= val_visit_times + 1
                df_user_location_list.iloc[same_location, df_user_location_list.columns.get_loc('spent_time')] = val_spent_time + df_user_visits.iloc[k]['dwellTime']

        #print(df_user_location_list)
        # Calculating the location diversity and loyalty
        print("total visits in df_user_location_list:",len(df_user_location_list))
        df_user_location_list = (df_user_location_list.loc[df_user_location_list['spent_time'] > datetime.timedelta(seconds=0)]).sort_values(by='visit_times', ascending=False)
        for i in range(0, len(df_user_location_list)):
            #spent_in_seconds = (df_user_location_list.iloc[i]['spent_time']).total_seconds()
            sql = "INSERT INTO user_location_list (userID,locationID,latitude,longitude,visit_times,spent_time) VALUES (" + str(
                df_user_location_list.iloc[i]['userID']) + "," + str(df_user_location_list.iloc[i]['locationID']) + "," + str(
                df_user_location_list.iloc[i]['latitude']) + "," + str(df_user_location_list.iloc[i]['longitude']) + "," + str(
                df_user_location_list.iloc[i]['visit_times']) + "," + str((df_user_location_list.iloc[i]['spent_time']).total_seconds()) + ");"
            print(sql)
            cursor.execute(sql)

    server.stop()

    print("End")