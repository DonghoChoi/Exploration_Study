#!/usr/bin/python
# Author: Dongho Choi
'''
This script

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
        #sql = "SET @row_num = 0;"
        #cursor.execute(sql)
        sql="SELECT DISTINCT url FROM WS_eye_duration_per_page_with_url WHERE (userID={0});".format(current_userID)
        df_user_pages_lab = pd.read_sql(sql,con=connection)
        print("distinct url length: {0}".format(len(df_user_pages_lab)))

        distinct_url_index = 1
        for j in range(0,len(df_user_pages_lab)):
                temp_url = df_user_pages_lab.iloc[j]['url']
                sql="INSERT INTO WS_url_list (userID,url,distinct_url_index) VALUES ({0},'{1}',{2})".\
                    format(str(current_userID),str(temp_url),str(distinct_url_index))
                print(sql)
                cursor.execute(sql)
                distinct_url_index = distinct_url_index + 1

    server.stop()


