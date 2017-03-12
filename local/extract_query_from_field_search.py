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
import server_config # (1) info2_server (2) exploration_db
from urllib.parse import urlparse,parse_qs

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

    # Start date and end date of field session for each participant
    df_user_field_session_period = pd.read_sql('SELECT * FROM user_field_session_period', con=connection)
    print("Field session period imported.")

    # Field browsing history table
    #df_pages = pd.read_sql('SELECT * FROM pages', con=connection)
    #print('Pages table read')

    #for i in range(0, 1):
    # completed: 8,7
    for i in range(0, num_participants):
        current_userID = participants_list[i]
        print("current userID - ",current_userID)

        # current user's browsing history for his/her field study period
        df_current_user_field_session_period = df_user_field_session_period.loc[df_user_field_session_period['userID'] == current_userID]
        start_epochtime = ((datetime.datetime(2016,df_current_user_field_session_period['start_month'],df_current_user_field_session_period['start_day']))-datetime.datetime(1970,1,1)).total_seconds()*1000
        end_epochtime = ((datetime.datetime(2016,df_current_user_field_session_period['end_month'],df_current_user_field_session_period['end_day'])+datetime.timedelta(days=2))-datetime.datetime(1970,1,1)).total_seconds()*1000
        print("start_epochtime:{0}, end_epochtime:{1}".format(start_epochtime,end_epochtime))
        sql = 'SELECT userID,url,query,localTimestamp_int AS epoch_time FROM pages WHERE userID = ' + str(current_userID) + ' AND localTimestamp_int > ' + str(start_epochtime) + ' AND localTimestamp_int < ' + str(end_epochtime) + ';'
        df_user_pages = pd.read_sql(sql, con=connection)
        print(len(df_user_pages))

        # extract query if exists
        for j in range(0, len(df_user_pages)):
            epoch_time = df_user_pages.iloc[j,df_user_pages.columns.get_loc('epoch_time')]
            current_url = df_user_pages.iloc[j,df_user_pages.columns.get_loc('url')]
            url_parse_result = urlparse(current_url)
            query =''
            print("{0} th url - {1}: netloc = {2}, path = {3}".format(j,current_url,url_parse_result.netloc,url_parse_result.path))
            if url_parse_result.netloc == 'www.google.com' and url_parse_result.path == '/search':
                try:
                    query = parse_qs(url_parse_result.query)['q'][0]
                    print("query: {0}".format(query))
                except KeyError:
                    print("Key Error")
            if url_parse_result.netloc=='www.bing.com' and url_parse_result.path=='/search':
                try:
                    query = parse_qs(url_parse_result.query)['q'][0]
                    print("query: {0}".format(query))
                except KeyError:
                    print("Key Error")
            if url_parse_result.netloc=='search.yahoo.com' and url_parse_result.path=='/search':
                try:
                    query = parse_qs(url_parse_result.query)['p'][0]
                    print("query: {0}".format(query))
                except KeyError:
                    print("Key Error")
            df_user_pages.iloc[j,df_user_pages.columns.get_loc('query')] = query
            #sql = 'INSERT INTO pages_field_session (userID,epoch_time,url,query) VALUES (' + str(current_userID) + ',' + str(epoch_time) + ',"' + str(current_url) + '","' + str(query) +'");'
            sql = "INSERT INTO pages_field_session (userID,epoch_time,url,query) VALUES ({0},{1},'{2}','{3}')".format(str(current_userID),str(epoch_time),str(connection.escape_string(current_url)),str(connection.escape_string(query)))
            print(sql)
            cursor.execute(sql)

    server.stop()
