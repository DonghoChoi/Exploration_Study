#!/usr/bin/python
# Author: Dongho Choi
'''
This script
(1) read 'pages' data of a user
(2) calculate dwell time of each page
(3) retrieve fixation data corresponding to the dwell time
(4) calculate eye dwell time in which fixations are within the effective range of the screen

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

x_max = 0.80
x_min = 0.00
y_min = 0.09
y_max = 0.96
fixation_list_stored = True
eye_duration_stored = True

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
                                 db=server_config.exploration_db['database'],
                                 use_unicode=True,
                                 charset="utf8"
                                 )
    connection.autocommit(True)
    cursor = connection.cursor()
    print("MySQL connection established")

    '''
    # Get the participants list from the table of 'final_participants'
    df_participants = pd.read_sql('SELECT * FROM final_participants', con=connection)
    print("Participants Table READ")
    userID_list = df_participants['userID'].tolist()
    '''
    #userID_list = [2,10,22,7]
    #userID_list = [8,11,12,20,21,25]
    userID_list = [2,10,22,7,8,11,12,20,21,25,6,14,16,23,42,26,27,32,46,1,3,5,9,28,34,35,37,38,40,45]
    # TH behavior data for the whole session
    #for i in range(0,1):
    for i in range(0,len(userID_list)): # i - current userID
        current_userID = userID_list[i]
        #current_userID = 22
        print("current user: {0}".format(current_userID))

        # pages_lab table order by localTimestamp_int adding an index column named page_index
        sql = "SET @row_num = 0;"
        cursor.execute(sql)
        sql="SELECT @row_num:=@row_num+1 as page_index,pageID,userID,stageID,query,url,questionID,title,localTimestamp_int,date,time FROM pages_lab WHERE (userID={0} AND stageID=31 AND questionID=1) ORDER BY localTimestamp_int ASC;".format(current_userID)
        df_user_pages_lab = pd.read_sql(sql,con=connection)
        print("user_pages_lab length: {0}".format(len(df_user_pages_lab)))

        # create a dataframe that will store the page_index, start_time and end_time - number of rows will be len(df_user_pages_lab) - 1
        df_user_page_duration = pd.DataFrame(columns=('page_index','pageID','start_time','end_time'))

        for j in range(0,len(df_user_pages_lab)-1):
                temp_page_index = df_user_pages_lab.iloc[j]['page_index']
                temp_pageID = df_user_pages_lab.iloc[j]['pageID']
                temp_start_time = df_user_pages_lab.iloc[j]['localTimestamp_int']
                temp_end_time = df_user_pages_lab.iloc[j+1]['localTimestamp_int']
                #print(temp_page_index,temp_start_time,temp_end_time)
                sql="INSERT INTO WS_page_start_end_time (userID,pageID,page_index,start_time,end_time) VALUES ({0},{1},{2},{3},{4})".\
                    format(str(current_userID),str(temp_pageID),str(temp_page_index),str(temp_start_time),str(temp_end_time))
                print(sql)
                cursor.execute(sql)
                '''
                row = [{'page_index':temp_page_index,'pageID':temp_pageID,'start_time':temp_start_time,'end_time':temp_end_time}]
                temp_row = pd.DataFrame(row)
                #print(temp_row)
                sql="SELECT localTimestamp_int,FPOGX,FPOGY,FPOGS,FPOGD,FPOGID FROM WS_eye_fixations_timeStamp WHERE (userID={0} AND localTimestamp_int>{1} AND localTimestamp_int<{2}) ORDER BY localTimestamp_int ASC;".format(current_userID,temp_start_time,temp_end_time)
                df_user_fixations = pd.read_sql(sql,con=connection)
                print("page_index:{0}".format(temp_page_index))
                print(df_user_fixations['FPOGID'])
                duration_time = 0
                
                for k in range(0,len(df_user_fixations)):
                    # store: userID,pageID,page_index,FPOGX,FPOGY,FPOGS,FPOGD,FPOGID
                    fpogx = float(df_user_fixations.iloc[k]['FPOGX'])
                    fpogy = float(df_user_fixations.iloc[k]['FPOGY'])
                    fpogs = float(df_user_fixations.iloc[k]['FPOGS'])
                    fpogd = float(df_user_fixations.iloc[k]['FPOGD'])
                    fpogid = df_user_fixations.iloc[k]['FPOGID']
                    stored_fixations = pd.read_sql("SELECT COUNT(id) as instances FROM WS_eye_fixation_per_page WHERE userID= {0} AND pageID={1};".format(current_userID,temp_pageID),con=connection)
                    #if(stored_fixations.iloc[0]['instances']==0):
                    #print("NEW EYE_FIXATION_PER_PAGE DATA IMPORTING")
                    sql="INSERT INTO WS_eye_fixation_per_page (userID,pageID,page_index,FPOGX,FPOGY,FPOGS,FPOGD,FPOGID) VALUES ({0},{1},{2},{3},{4},{5},{6},{7})".\
                        format(str(current_userID),str(temp_pageID),str(temp_page_index),str(fpogx),str(fpogy),str(fpogs),str(fpogd),str(fpogid))
                    print(sql)
                    cursor.execute(sql)
                    #
                    if (fpogx<x_max and fpogx>x_min and fpogy<y_max and fpogy>y_min):
                        duration_time = duration_time + fpogd
                
                stored_durations = pd.read_sql("SELECT COUNT(id) as instances FROM WS_eye_duration_per_page WHERE userID={0} AND pageID={1};".format(current_userID,temp_pageID),con=connection)
                if (stored_durations.iloc[0]['instances']==0):
                    print("NEW EYE_DURATION_PER_PAGE DATA IMPORTING")
                    sql="INSERT INTO WS_eye_duration_per_page (userID,pageID,page_index,duration) VALUES ({0},{1},{2},{3})".\
                        format(str(current_userID),str(temp_pageID),str(temp_page_index),str(duration_time))
                    print(sql)
                    cursor.execute(sql)
                
                print("pageID: {0}, page_index:{1}, duration_time:{2}".format(temp_pageID,temp_page_index,duration_time))
                '''
                #df_user_page_duration=df_user_page_duration.append(temp_row)


    server.stop()


