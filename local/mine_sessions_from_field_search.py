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

from urllib.parse import urlparse

navigation_search = {'google','yahoo','bing','baidu','aol','excite','duckduckgo','wolframalpha','yandex',\
                     'facebook','twitter','linkedin','google+','youtube','instagram','tumblr','vine','snapchat','reddit','pinterest','flickr','foursquare','kik','yik yak',\
                     'cnn','nyt','nbc','washington post','new york times','wsj','abc news','bbc','bbc news','usa today','la times',\
                     'amazon','wikipedia','ebay','msn','wordpress','microsoft','netflix','stack overflow','imgur','github','whatsapp','espn','walmart','apple','samsung'}
secured_url = {'gmail.com','mail.google.com','docs.google.com','sheets.google.com','accounts.google.com','drive.google.com',\
               'www.facebook.com','www.youtube.com','youtube.com',\
               'cas.rutgers.edu','sakai.rutgers.edu','sims.rutgers.edu','dn.rutgers.edu'}

def get_last_query (df, start_id, end_id):
    temp_sub_session = df[(df.id >= start_id) & (df.id <= end_id)]
    temp_sub_session_query = temp_sub_session.loc[temp_sub_session['query'] != '']
    last_query = temp_sub_session_query.iloc[len(temp_sub_session_query)-1,temp_sub_session_query.columns.get_loc('query')]
    return last_query

def get_first_query (df, start_id, end_id):
    temp_sub_session = df[(df.id >= start_id) & (df.id <= end_id)]
    temp_sub_session_query = temp_sub_session.loc[temp_sub_session['query'] != '']
    first_query = temp_sub_session_query.iloc[0,temp_sub_session_query.columns.get_loc('query')]
    return first_query

def get_time(df, input_id):
    temp = df[df.id == input_id]
    return temp.iloc[0,temp.columns.get_loc('epoch_time')]

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

        # current user's browsing history for his/her field study period
        sql = 'SELECT * FROM pages_field_session WHERE userID = ' + str(current_userID)  + ';'
        df_user_pages = pd.read_sql(sql, con=connection)
        print(len(df_user_pages))

        # calculate dwell time
        #df_user_pages = df_user_pages.assign('dwell_time')
        df_user_pages['dwell_time'] = 0
        for h in range(0,len(df_user_pages)-1):
            dwell_time = df_user_pages.iloc[h+1,df_user_pages.columns.get_loc('epoch_time')] - df_user_pages.iloc[h,df_user_pages.columns.get_loc('epoch_time')]
            df_user_pages.iloc[h,df_user_pages.columns.get_loc('dwell_time')] = dwell_time
        #print(df_user_pages)

        # get session range - id of first and last
        sessions_list = []
        id_first_query = -1
        session_started = False
        for j in range (1,len(df_user_pages)-1):
            current_query = df_user_pages.iloc[j,df_user_pages.columns.get_loc('query')]
            current_epoch_time = df_user_pages.iloc[j,df_user_pages.columns.get_loc('epoch_time')]
            previous_epoch_time = df_user_pages.iloc[j-1,df_user_pages.columns.get_loc('epoch_time')]
            current_id = df_user_pages.iloc[j,df_user_pages.columns.get_loc('id')]
            current_url = df_user_pages.iloc[j,df_user_pages.columns.get_loc('url')]
            if session_started == True:
                if (current_epoch_time - previous_epoch_time> 1800000):
                    session_started = False
                    #id_last = current_id
                    sessions_list.append([id_first_query,current_id-1])
                    id_first_query = -1
                    #print("Session end: idle")
                elif (current_query != '' and current_query in navigation_search):
                    session_started = False
                    #id_last = current_id
                    sessions_list.append([id_first_query,current_id-1])
                    id_first_query = -1
                    #print("Session end: navigation search - {0}".format(current_query))
                elif (urlparse(current_url).hostname in secured_url):
                    session_started = False
                    #id_last = current_id
                    sessions_list.append([id_first_query,current_id-1])
                    id_first_query = -1
                    #print("Session end: secured url - {0}".format(urlparse(current_url).hostname))
            else: # session_started == False
                if current_query != '': # when meeting query
                    if current_query in navigation_search:
                        print("NAVIGATION")
                    else: # meaningful query
                        #print("[Session start]query:{0}".format(current_query))
                        session_started = True
                        id_first_query = current_id
                        #session_start_time = current_epoch_time

        print(sessions_list)
        print("number of sessions:{0}".format(len(sessions_list)))

        # Merge sub-sessions
        session_id = 0
        df_user_pages_with_sessionID = pd.DataFrame(columns=('id','userID','epoch_time','url','query','sessionID'))
        first_sub_session = df_user_pages[(df_user_pages.id>=sessions_list[0][0]) & (df_user_pages.id<=sessions_list[0][1])]
        first_sub_session['sessionID'] = session_id
        df_user_pages_with_sessionID = first_sub_session
        #session_id += 1
        for k in range(1,len(sessions_list)-1):
            previous_start_id = sessions_list[k-1][0]
            previous_end_id = sessions_list[k-1][1]
            current_start_id = sessions_list[k][0]
            current_end_id = sessions_list[k][1]

            current_sub_session = df_user_pages[(df_user_pages.id>=current_start_id) & (df_user_pages.id<=current_end_id)]

            previous_sub_session_last_query = get_last_query(df_user_pages, previous_start_id, previous_end_id)
            current_sub_session_first_query = get_first_query(df_user_pages, current_start_id, current_end_id)

            if (current_sub_session_first_query == previous_sub_session_last_query) and (get_time(df_user_pages,current_start_id)-get_time(df_user_pages,previous_end_id)<1800000):
                current_sub_session['sessionID'] = session_id
                df_user_pages_with_sessionID = df_user_pages_with_sessionID.append(current_sub_session)
            else:
                session_id += 1
                current_sub_session['sessionID'] = session_id
                df_user_pages_with_sessionID = df_user_pages_with_sessionID.append(current_sub_session)
        print(len(df_user_pages_with_sessionID))

        for m in range(0,len(df_user_pages_with_sessionID)):
            userID = df_user_pages_with_sessionID.iloc[m,df_user_pages_with_sessionID.columns.get_loc('userID')]
            epoch_time = df_user_pages_with_sessionID.iloc[m,df_user_pages_with_sessionID.columns.get_loc('epoch_time')]
            url = df_user_pages_with_sessionID.iloc[m, df_user_pages_with_sessionID.columns.get_loc('url')]
            query = df_user_pages_with_sessionID.iloc[m, df_user_pages_with_sessionID.columns.get_loc('query')]
            dwell_time = df_user_pages_with_sessionID.iloc[m, df_user_pages_with_sessionID.columns.get_loc('dwell_time')]
            sessionID = df_user_pages_with_sessionID.iloc[m, df_user_pages_with_sessionID.columns.get_loc('sessionID')]

            sql = "INSERT INTO user_field_search_session (userID,sessionID,epoch_time,dwell_time,query,url) VALUES ({0},{1},{2},{3},'{4}','{5}')".\
                format(str(current_userID),str(sessionID),str(epoch_time),str(dwell_time),str(connection.escape_string(query)),str(connection.escape_string(url)))
            print(sql)
            cursor.execute(sql)

        num_sessions = df_user_pages_with_sessionID.iloc[len(df_user_pages_with_sessionID)-1,df_user_pages_with_sessionID.columns.get_loc('sessionID')]+1
        print("number of sesions:{0}".format(num_sessions))

        for l in range(0,num_sessions):
            current_sub_session = df_user_pages_with_sessionID[df_user_pages_with_sessionID.sessionID == l]
            current_sub_session_query = current_sub_session.loc[current_sub_session['query']!='']
            query_list = current_sub_session_query['query'].tolist()
            query_set = set(query_list)
            print("sessionID {0}: query_issued - {1}, unique queries - {2}".format(l,len(query_list),len(query_set)))
            sql = "INSERT INTO user_field_queries (userID,sessionID,query_issued,distinct_query_issued) VALUES ({0},{1},{2},{3})". \
                format(str(current_userID), str(l), str(len(query_list)), str(len(query_set)),)
            print(sql)
            cursor.execute(sql)

        # Check sessions


    server.stop()