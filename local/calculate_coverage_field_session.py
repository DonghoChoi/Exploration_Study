#!/usr/bin/python
# Author: Dongho Choi

import math
import pandas as pd
from math import log
from sshtunnel import SSHTunnelForwarder # for SSH connection
import pymysql.cursors # MySQL handling API
import sys
import datetime
import time
sys.path.append("./configs/")
sys.path.append("/Users/donghochoi/Documents/Work/Exploration_Study/Dissertation/Code/local/configs/")
import server_config # (1) info2_server (2) exploration_db


if __name__ == "__main__":

    # READ DATA FROM SERVER
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
                                 charset="utf8")
    connection.autocommit(True)
    cursor = connection.cursor()
    print("MySQL connection established.")

    # Get the participants list from the table of 'final_participants'
    df_participants = pd.read_sql('SELECT * FROM final_participants', con=connection)
    print("Participants Table READ")

    # READ AND FILL THE PARTICIPANTS LIST WITH COMBINATIONS
    participants_list = df_participants['userID'].tolist()
    num_participants = len(participants_list) # number of participants
    print('number of participants:{0}'.format(num_participants))

    #for i in range(0,1):
    for i in range(0, num_participants):
        current_userID = participants_list[i]
        print("current userID - ",current_userID)

        # current user's browsing history with sessionID
        sql = 'SELECT * FROM user_field_search_session WHERE userID = ' + str(current_userID)  + ';'
        df_user_pages = pd.read_sql(sql, con=connection)
        print(len(df_user_pages))

        num_session = len(df_user_pages.sessionID.unique())

        for j in range(0,num_session):
            df_session_pages = df_user_pages[df_user_pages.sessionID==j]
            df_session_only_webpages = df_session_pages.loc[(df_session_pages['query']=='')&(df_session_pages['dwell_time']>=1000)]
            df_dwell_time_sum = df_session_only_webpages.groupby('url')['dwell_time'].sum()
            coverage = len(df_dwell_time_sum)
            useful_coverage = len(df_dwell_time_sum[df_dwell_time_sum>30000])
            if coverage == 0:
                use_ratio = -1
            else:
                use_ratio = useful_coverage/coverage
            print("coverage: {0}, useful coverage: {1}, use_ratio: {2}".format(coverage,useful_coverage,use_ratio))

            sql = "INSERT INTO user_field_session_coverage (userID,sessionID,Coverage,UsefulCoverage,Use_ratio) VALUES ({0},{1},{2},{3},{4})". \
                format(str(current_userID), str(j), str(coverage), str(useful_coverage),str(use_ratio))
            print(sql)
            cursor.execute(sql)

    server.stop()