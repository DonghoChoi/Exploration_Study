#!/usr/bin/python
# Author: Dongho Choi
'''
This script
(1) read TH behavior data
(2) read TH coordinates data
(3) calculate radius of gyration

'''

import os.path
import datetime
import math
import time
import itertools
import pandas as pd
from sshtunnel import SSHTunnelForwarder # for SSH connection
import pymysql.cursors # MySQL handling API
import sys
import matplotlib.pyplot as plt
import numpy as np
from sklearn import datasets, linear_model
import statsmodels.api as sm
import csv
#sys.path.append("./configs/")
sys.path.append("/Users/donghochoi/Documents/Work/Exploration_Study/Dissertation/Code/local/configs/")
import server_config # (1) info2_server (2) exploration_db

infoPatch_threshold = 2
number_k = 2

def get_center_of_mass(df_TH_visit_list):
    x = 0
    y = 0
    z = 0
    visit_sum = df_TH_visit_list['label_count'].sum()
    for i in range(0,len(df_TH_visit_list)):
        x = x + df_TH_visit_list.iloc[i]['label_count'] * df_TH_visit_list.iloc[i]['x']
        y = y + df_TH_visit_list.iloc[i]['label_count'] * df_TH_visit_list.iloc[i]['y']
        z = x + df_TH_visit_list.iloc[i]['label_count'] * df_TH_visit_list.iloc[i]['z']

    x = x/visit_sum
    y = y/visit_sum
    z = z/visit_sum

    return np.array([x, y, z])

if __name__ == "__main__":

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
    print("MySQL connection established")

    # Get list of users from 'user_TH_merged_results'
    df_participants = pd.read_sql("SELECT DISTINCT userID FROM user_TH_merged_results;", con=connection)
    TH_userID_list = df_participants['userID'].tolist()
    print("TH user list imported: total {0}".format(len(TH_userID_list)))

    # Get list of users who finished task 1
    df_participants = pd.read_sql("SELECT userID,observed_time FROM user_TH_merged_results WHERE (behavior='T1_done')", con=connection)
    TH_task1_complete_user_list = df_participants['userID'].tolist()
    print("{0} users completed task 1".format(len(TH_task1_complete_user_list)))

    # GET TH coordinates
    df_TH_coords = pd.read_sql('SELECT InfoPatch,x_coord as x,y_coord as y,z_coord as z FROM TH_InfoPatch_coords', con=connection)
    print("TH coordinates Table imported.")

    # TH behavior data for the whole session
    #for i in range(0,1):
    for i in range(0,len(TH_userID_list)): # i - current userID
        current_userID = TH_userID_list[i]
        print("current user: {0}".format(current_userID))
        # time when the participant finished task 1
        sql = "SELECT observed_time FROM user_TH_merged_results WHERE (behavior='T1_done' AND userID={0});".format(current_userID)
        temp_time = pd.read_sql(sql,con=connection)
        if len(temp_time)==0:
            T1_done_time = -1
        elif len(temp_time)==1:
            T1_done_time = temp_time.iloc[0,0]
        # visitation frequency and duration time of infomation patches for the whole session
        '''
        sql = "SELECT observed_time,duration,label From user_TH_merged_results WHERE (behavior='InfoPatch' AND userID={0})".format(current_userID)
        df_TH_whole = pd.read_sql(sql,con=connection)
        '''
        sql = "SELECT label,COUNT(label) as label_count,SUM(duration) as sum_duration FROM user_TH_merged_results WHERE(userID={0} AND behavior='InfoPatch' AND duration>{1}) GROUP BY label ORDER BY COUNT(label) DESC, SUM(duration) DESC".format(current_userID,infoPatch_threshold)
        df_TH_whole = pd.read_sql(sql,con=connection)
        df_TH_visit_list = pd.merge(df_TH_whole, df_TH_coords, left_on='label', right_on='InfoPatch')
        #print(df_TH_visit_list)

        # REGARDING TOTAL VISITS
        # r_cm: the center of mass of the individual
        center_of_mass = get_center_of_mass(df_TH_visit_list)
        print("center_of_mass: ({0}, {1}, {2})".format(center_of_mass[0], center_of_mass[1], center_of_mass[2]))
        # total radius of gyration
        gyration_sum = 0
        for l in range(0, len(df_TH_visit_list)):
            current_location = np.array([df_TH_visit_list.iloc[l]['x'], df_TH_visit_list.iloc[l]['y'], df_TH_visit_list.iloc[l]['z']])
            gyration_sum = gyration_sum + df_TH_visit_list.iloc[l]['label_count'] * pow(np.linalg.norm(current_location-center_of_mass), 2)
        total_visits = df_TH_visit_list['label_count'].sum()
        total_r_g = math.sqrt(gyration_sum / total_visits)
        print("total_r_g: {0}, gyration_sum: {1}, total_visits: {2}".format(total_r_g,gyration_sum,total_visits))

        # REGARDING TOP-K VISITS
        # subset of visits
        df_TH_visit_list_k = df_TH_visit_list[:number_k]
        center_of_mass_k = get_center_of_mass(df_TH_visit_list_k)
        print("center_of_mass_k: ({0}, {1}, {2})".format(center_of_mass_k[0], center_of_mass_k[1], center_of_mass_k[2]))
        # k-radius of gyration
        gyration_sum_k= 0
        for l in range(0, len(df_TH_visit_list_k)):
            current_location = np.array(
                [df_TH_visit_list_k.iloc[l]['x'], df_TH_visit_list_k.iloc[l]['y'], df_TH_visit_list_k.iloc[l]['z']])
            gyration_sum_k = gyration_sum_k + df_TH_visit_list.iloc[l]['label_count'] * pow(
                np.linalg.norm(current_location - center_of_mass_k), 2)
        total_visits_k = df_TH_visit_list_k['label_count'].sum()
        r_g_k = math.sqrt(gyration_sum_k / total_visits)
        print("r_g_k: {0}, gyration_sum: {1}, total_visits: {2}".format(r_g_k, gyration_sum_k, total_visits_k))

        # CALCULATING s_k ratio
        s_k_ratio = r_g_k/total_r_g

        sql = "INSERT INTO TH_mobility_data (userID,visited_locations,k,visited_locations_k,gyration_all,gyration_k,s_k) VALUES ({0},{1},{2},{3},{4},{5},{6})".\
              format(current_userID,total_visits,number_k,total_visits_k,total_r_g,r_g_k,s_k_ratio)
        cursor.execute(sql)

    server.stop()


