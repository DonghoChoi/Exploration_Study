#!/usr/bin/python
# Author: Dongho Choi
'''
This script
(1) read TH data relate to the task 1
(2) calculate features
- visits in the first one/two/three sessions (session means period staying at a floor)
- time spent 

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
    df_participants = pd.read_sql("SELECT DISTINCT userID FROM user_TH_merged_results_1_round;", con=connection)
    TH_userID_list = df_participants['userID'].tolist()
    print("TH user list imported: total {0}".format(len(TH_userID_list))) #29 ppl

    # Get list of users who finished task 1
    df_participants = pd.read_sql("SELECT userID,observed_time FROM user_TH_merged_results_1_round WHERE (behavior='T1_done')", con=connection)
    TH_task1_complete_user_list = df_participants['userID'].tolist()
    print("{0} users completed task 1".format(len(TH_task1_complete_user_list))) #19 ppl

    # Get list of users who did not finish task 1
    TH_task1_fail_user_list = list(set(TH_userID_list)-set(TH_task1_complete_user_list))
    print("{0} users failed task 1".format(len(TH_task1_fail_user_list))) #10

    # TH behavior data for the completed users
    for i in range(0,len(TH_task1_complete_user_list)): # i - current userID
        current_userID = TH_task1_complete_user_list[i]
        print("current user: {0}".format(current_userID))
        # time when the participant finished task 1
        sql = "SELECT observed_time FROM user_TH_merged_results_1_round WHERE (behavior='T1_done' AND userID={0});".format(current_userID)
        temp_time = pd.read_sql(sql,con=connection)
        T1_done_time = temp_time.iloc[0,0]
        print("T1 finished: {0}".format(T1_done_time))
        # visitation frequency and duration time of infomation patches for the whole session
        #sql = "SELECT label,COUNT(label) as label_count,SUM(duration) as sum_duration FROM user_TH_merged_results_1_round WHERE(userID={0} AND behavior='InfoPatch' AND duration>{1} AND observed_time<{2}) GROUP BY label ORDER BY COUNT(label) DESC, SUM(duration) DESC".format(current_userID,infoPatch_threshold,T1_done_time)
        sql = "SELECT * FROM user_TH_merged_results_1_round WHERE(userID={0} AND behavior='InfoPatch' AND duration>{1} AND observed_time<{2}) ORDER BY observed_time ASC".format(current_userID,infoPatch_threshold,T1_done_time)
        df_TH_task1 = pd.read_sql(sql,con=connection)
        #df_TH_visit_list = pd.merge(df_TH_task1, df_TH_coords, left_on='label', right_on='InfoPatch')
        print(df_TH_task1)

        # get (1) number of floor visits, (2) number of infopatch on each floor visit (3) total time spent on each floor visit
        current_floor = 2
        total_visits = 0
        total_duration = 0
        sequence = 1
        for j in range(0,len(df_TH_task1)):
            temp_floor = df_TH_task1.iloc[j]['floor']
            temp_duration = df_TH_task1.iloc[j]['duration']
            print("temp_floor: {0}".format(temp_floor))
            # when the user confronts the first infopatch on this floor
            if(current_floor!=temp_floor):
                # update visits information that happenend on previous floor
                print("Total visits:{0}, total duration:{1}, on floor({2})".format(total_visits,total_duration,current_floor))
                sql = "INSERT INTO TH_floor_visit_duration (userID,sequence,floor,visits,duration) VALUES ({0},{1},{2},{3},{4})".\
                    format(current_userID,sequence,current_floor,total_visits,total_duration)
                print(sql)
                cursor.execute(sql)
                current_floor = temp_floor
                sequence = sequence + 1
                total_visits = 0
                total_duration = 0

            total_visits = total_visits + 1
            total_duration = total_duration + temp_duration

            if(j==len(df_TH_task1)-1):
                print("Total visits:{0}, total duration:{1}, on floor({2})".format(total_visits,total_duration,current_floor))
                sql = "INSERT INTO TH_floor_visit_duration (userID,sequence,floor,visits,duration) VALUES ({0},{1},{2},{3},{4})".\
                    format(current_userID,sequence,current_floor,total_visits,total_duration)
                print(sql)
                cursor.execute(sql)

    # TH behavior data for the failed users
    for i in range(0, len(TH_task1_fail_user_list)):  # i - current userID
        current_userID = TH_task1_fail_user_list[i]
        print("current user: {0}".format(current_userID))
        # visitation frequency and duration time of infomation patches for the whole session
        # sql = "SELECT label,COUNT(label) as label_count,SUM(duration) as sum_duration FROM user_TH_merged_results_1_round WHERE(userID={0} AND behavior='InfoPatch' AND duration>{1} AND observed_time<{2}) GROUP BY label ORDER BY COUNT(label) DESC, SUM(duration) DESC".format(current_userID,infoPatch_threshold,T1_done_time)
        sql = "SELECT * FROM user_TH_merged_results_1_round WHERE(userID={0} AND behavior='InfoPatch' AND duration>{1} AND observed_time<{2}) ORDER BY observed_time ASC".format(
            current_userID, infoPatch_threshold, 1200)
        df_TH_task1 = pd.read_sql(sql, con=connection)
        print(df_TH_task1)

        # get (1) number of floor visits, (2) number of infopatch on each floor visit (3) total time spent on each floor visit
        current_floor = 2
        total_visits = 0
        total_duration = 0
        sequence = 1
        for j in range(0, len(df_TH_task1)):
            temp_floor = df_TH_task1.iloc[j]['floor']
            temp_duration = df_TH_task1.iloc[j]['duration']
            print("temp_floor: {0}".format(temp_floor))
            # when the user confronts the first infopatch on this floor
            if (current_floor != temp_floor):
                # update visits information that happenend on previous floor
                print("Total visits:{0}, total duration:{1}, on floor({2})".format(total_visits, total_duration,
                                                                                   current_floor))
                sql = "INSERT INTO TH_floor_visit_duration (userID,sequence,floor,visits,duration) VALUES ({0},{1},{2},{3},{4})". \
                    format(current_userID, sequence, current_floor, total_visits, total_duration)
                print(sql)
                cursor.execute(sql)
                current_floor = temp_floor
                sequence = sequence + 1
                total_visits = 0
                total_duration = 0

            total_visits = total_visits + 1
            total_duration = total_duration + temp_duration

            if (j == len(df_TH_task1)-1):
                print("Total visits:{0}, total duration:{1}, on floor({2})".format(total_visits, total_duration,
                                                                                   current_floor))
                sql = "INSERT INTO TH_floor_visit_duration (userID,sequence,floor,visits,duration) VALUES ({0},{1},{2},{3},{4})". \
                    format(current_userID, sequence, current_floor, total_visits, total_duration)
                print(sql)
                cursor.execute(sql)



    server.stop()


