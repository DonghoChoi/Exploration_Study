#!/usr/bin/python
# Author: Dongho Choi
'''
This script
(1) import coding data from BORIS, which is located in the table named 'user_TH_Boris_results'
(2) calculate the time spent on each information patch
(3) import coding data from vCode, which is located in the table named 'user_TH_vCode_results'
(4) since there are some changes/differences in vCode data format (e.g., time unit is millisecond, and labels for the location and information patch were updated after coding of user7 and user8's data), these changes should be considered in this process

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
import csv
#sys.path.append("./configs/")
sys.path.append("/Users/donghochoi/Documents/Work/Exploration_Study/Dissertation/Code/local/configs/")
import server_config # (1) info2_server (2) exploration_db

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
    print("MySQL connection establis")

    # import Boris coding data
    df_boris_data = pd.read_sql('SELECT * FROM user_TH_Boris_results', con=connection)
    print("Boris data imported.")

    # import vCode coding data
    df_vcode_data = pd.read_sql('SELECT * FROM user_TH_vCode_results', con=connection)
    print("vCode data imported.")

    # select userID whose data is coded by Boris
    df_boris_userID = pd.read_sql("SELECT * FROM final_participants WHERE TH = 'Boris'", con=connection)
    userID_boris = df_boris_userID['userID'].tolist()

    for i in userID_boris:
        userID = i
        print("Current userID: {0}".format(userID))
        df_current_user_boris_data = df_boris_data[df_boris_data.userID==userID] #subset of df for current userID
        df_current_user_boris_data = df_current_user_boris_data.sort_values(['id'])
        df_current_user_infopatch = df_current_user_boris_data[df_current_user_boris_data.behavior == 'InfoPatch']
        df_current_user_rest = df_current_user_boris_data[df_current_user_boris_data.behavior!='InfoPatch'] # Location marks as well as task related marks
        ## Information Patches
        if (len(df_current_user_infopatch)%2 != 0):
            print("########## Information Patch coding has an error! ##########")
        for j in range(0, int(len(df_current_user_infopatch)/2)):
            start_time = df_current_user_infopatch.iloc[2*j+0,df_current_user_infopatch.columns.get_loc('observed_time')]
            end_time = df_current_user_infopatch.iloc[2*j+1,df_current_user_infopatch.columns.get_loc('observed_time')]

            label_0 = df_current_user_infopatch.iloc[2*j+0,df_current_user_infopatch.columns.get_loc('label')]
            label_1 = df_current_user_infopatch.iloc[2*j+1,df_current_user_infopatch.columns.get_loc('label')]

            behavior = 'InfoPatch'

            if (label_0 != label_1):
                print("--------- LABEL MISMATCH -----------")

            duration_time = end_time-start_time

            sql = "INSERT INTO user_TH_merged_results (userID,observed_time,duration,label,behavior) VALUES ({0},{1},{2},'{3}','{4}')".\
                format(str(userID),str(start_time),str(duration_time),str(label_0),str(behavior))
            print(sql)
            cursor.execute(sql)

        ## Rest marks
        for k in range(0,len(df_current_user_rest)):
            cur_time = df_current_user_rest.iloc[k,df_current_user_rest.columns.get_loc('observed_time')]
            label = df_current_user_rest.iloc[k,df_current_user_rest.columns.get_loc('label')]
            behavior = df_current_user_rest.iloc[k,df_current_user_rest.columns.get_loc('behavior')]

            sql = "INSERT INTO user_TH_merged_results (userID,observed_time,duration,label,behavior) VALUES ({0},{1},{2},'{3}','{4}')".\
                format(str(userID),str(cur_time),str(0),str(label),str(behavior))
            print(sql)
            cursor.execute(sql)

    # select userID whose data is coded by Boris
    df_vcode_userID = pd.read_sql("SELECT * FROM final_participants WHERE TH = 'vCode'", con=connection)
    userID_vcode = df_vcode_userID['userID'].tolist()

    for l in userID_vcode:
        userID = l
        print("Current userID: {0}".format(userID))
        df_current_user_vcode_data = df_vcode_data[df_vcode_data.userID==userID]
        df_current_user_vcode_data = df_current_user_vcode_data.sort_values(['observed_time'])
        for m in range(0,len(df_current_user_vcode_data)):
            cur_time = (df_current_user_vcode_data.iloc[m,df_current_user_vcode_data.columns.get_loc('observed_time')])/1000
            duration_time = (df_current_user_vcode_data.iloc[m,df_current_user_vcode_data.columns.get_loc('duration')])/1000
            label = df_current_user_vcode_data.iloc[m,df_current_user_vcode_data.columns.get_loc('label')]
            if label.startswith('L'):
                behavior = 'Location'
            elif label.startswith('I'):
                behavior = 'InfoPatch'
                if ((label=='I373') or (label=='I374')):
                    print("------- I373 & I374 => I372 -------")
                    label = 'I372'
                if label=='I364':
                    print("------- I364 => I363 -------")
                    label = 'I363'
                if label=='I362':
                    print("------- I362 => I361 -------")
                    label = 'I361'
                if label=='I386':
                    print("------- I386 => I331 -------")
                    label = 'I331'
            elif label.startswith('T'):
                behavior = label
                label = ''
            sql = "INSERT INTO user_TH_merged_results (userID,observed_time,duration,label,behavior) VALUES ({0},{1},{2},'{3}','{4}')".\
                format(str(userID),str(cur_time),str(duration_time),str(label),str(behavior))
            print(sql)
            cursor.execute(sql)


    server.stop()