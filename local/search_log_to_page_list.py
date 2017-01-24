#!/usr/bin/python
# Author: Dongho Choi

import os.path
import datetime
import math
import time
import sys
import itertools
import pandas as pd
from urllib.parse import urlparse
import numpy as np
from math import log
from sshtunnel import SSHTunnelForwarder # for SSH connection
import pymysql.cursors # MySQL handling API
from geopy.distance import vincenty
import sys
#sys.path.append("./configs/")
sys.path.append("/Users/donghochoi/Documents/Work/Exploration_Study/Dissertation/Code/local/configs/")
import server_config # (1) info2_server (2) exploration_db

def find_domain(current_domain,df_distinct_visit_list): # Return -1 when no domain_query existing, otherwise the location
    for i in range(0, len(df_distinct_visit_list)):
        if current_domain == df_distinct_visit_list.iloc[i]['domain']:
            #print("if found same domain: i = {0}, domainID = {1}".format(i,df_distinct_visit_list.iloc[i]['domainID']))
            return df_distinct_visit_list.iloc[i]['domainID']
    return -1

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

    # Get pages_all table: importing all pages that participants visited.
    #df_pages_all = pd.read_sql("SELECT userID,url,localTimestamp FROM pages WHERE (userID!=5001)",con=connection)
    df_pages_all = pd.read_sql("SELECT userID,url,epochTime FROM pages_test WHERE (userID!=5001)", con=connection)
    df_pages_all = df_pages_all.rename(columns={'epochTime': 'localTimestamp'})
    print("Pages Table READ")
    print("Length of pages_all is ",len(df_pages_all))

    # Get the participants list from the table of 'final_participants'
    df_participants = pd.read_sql('SELECT * FROM final_participants', con=connection)
    print("Participants Table READ")

    # READ AND FILL THE PARTICIPANTS LIST WITH COMBINATIONS
    participants_list = df_participants['userID'].tolist()
    num_participants = len(participants_list) # number of participants
    print('number of participants:{0}'.format(num_participants))
    '''
    # URL parsing
    df_domains_all = pd.DataFrame(index=range(0,len(df_pages_all)),columns=('userID','domain','path','query','localTimestamp'))
    for i in range(0, len(df_pages_all)):
        current_userID = df_pages_all.iloc[i]['userID']
        domain = urlparse(df_pages_all.iloc[i]['url']).netloc
        path = urlparse(df_pages_all.iloc[i]['url']).path
        query = urlparse(df_pages_all.iloc[i]['url']).query
        localTimestamp = df_pages_all.iloc[i]['localTimestamp']

        df_domains_all.iloc[i, df_domains_all.columns.get_loc('userID')] = current_userID
        df_domains_all.iloc[i, df_domains_all.columns.get_loc('domain')] = domain
        df_domains_all.iloc[i, df_domains_all.columns.get_loc('path')] = path
        df_domains_all.iloc[i, df_domains_all.columns.get_loc('query')] = query
        df_domains_all.iloc[i, df_domains_all.columns.get_loc('localTimestamp')] = localTimestamp
    print("URL parsing DONE")
    '''
    # Domain visit list
    df_merged_user_visit_list = pd.DataFrame(columns=('userID','domain__query','spent_seconds'))
    for i in range(0, num_participants): # i - current userID
    #for i in range(0,1):
        current_userID = participants_list[i]
        df_user_pages_all = df_pages_all.loc[df_pages_all['userID']==current_userID]

        # URL parsing for this user
        df_user_domains_all = pd.DataFrame(index=range(0,len(df_user_pages_all)),columns=('userID','domain','path','query','localTimestamp'))
        for j in range(0, len(df_user_domains_all)):
            domain = urlparse(df_user_pages_all.iloc[j]['url']).netloc
            path = urlparse(df_user_pages_all.iloc[j]['url']).path
            query = urlparse(df_user_pages_all.iloc[j]['url']).query
            localTimestamp = df_user_pages_all.iloc[j]['localTimestamp']

            df_user_domains_all.iloc[j, df_user_domains_all.columns.get_loc('userID')] = current_userID
            df_user_domains_all.iloc[j, df_user_domains_all.columns.get_loc('domain')] = domain
            df_user_domains_all.iloc[j, df_user_domains_all.columns.get_loc('path')] = path
            df_user_domains_all.iloc[j, df_user_domains_all.columns.get_loc('query')] = query
            df_user_domains_all.iloc[j, df_user_domains_all.columns.get_loc('localTimestamp')] = localTimestamp
        print("USER {0} URL PARSE DONE".format(current_userID))

        # Domain sequences
        df_user_domain_sequence = pd.DataFrame(columns=('userID', 'domain', 'spent_milliseconds'))
        df_user_pages = df_user_domains_all.sort_values(by='localTimestamp')
        current_domain = df_user_pages.iloc[0]['domain']
        #current_query = df_user_pages.iloc[0]['query']
        visit_start = df_user_pages.iloc[0]['localTimestamp']
        #visit_end = df_user_pages.iloc[0]['localTimestamp']

        for k in range(1, len(df_user_pages)):
            temp_domain = df_user_pages.iloc[k]['domain']
            #temp_query = df_user_pages.iloc[k]['query']
            if (current_domain == temp_domain):
                #print("Same domain")
                continue
            else:
                df_temp_visits = pd.DataFrame(columns=('userID','domain','spent_milliseconds'))
                df_temp_visits.set_value(0, 'userID', current_userID)
                df_temp_visits.set_value(0, 'domain', current_domain)
                #df_temp_visits.set_value(0, 'domain__query', (current_domain+"__"+current_query))
                #df_temp_visits.set_value(0, 'query', current_query)
                var_spent_time = df_user_pages.iloc[k]['localTimestamp'] - visit_start
                if (var_spent_time > 3600000):
                    var_spent_time = 3600000
                df_temp_visits.set_value(0, 'spent_milliseconds', var_spent_time)
                df_user_domain_sequence = df_user_domain_sequence.append(df_temp_visits)
                current_domain = temp_domain
                #current_query = temp_query
                visit_start = df_user_pages.iloc[k]['localTimestamp']
        print("USER {0} DOMAIN SEQUENCE DONE".format(current_userID))

        df_user_domain_sequence = df_user_domain_sequence.loc[df_user_domain_sequence['spent_milliseconds']>10]

        df_distinct_visit_list = pd.DataFrame(columns=('userID', 'domainID', 'domain', 'visit_times', 'spent_time', 'routine'))
        df_temp_visits = pd.DataFrame(columns=('userID', 'domainID','domain', 'visit_times', 'spent_time', 'routine'))
        df_temp_visits.set_value(0, 'userID', current_userID)
        df_temp_visits.set_value(0, 'domainID', 0)
        df_temp_visits.set_value(0, 'domain', df_user_domain_sequence.iloc[0]['domain'])
        df_temp_visits.set_value(0, 'visit_times', 1)
        df_temp_visits.set_value(0, 'spent_time', df_user_domain_sequence.iloc[0]['spent_milliseconds'])
        df_temp_visits.set_value(0, 'routine', 0)
        df_distinct_visit_list = df_distinct_visit_list.append(df_temp_visits)

        for k in range(1, len(df_user_domain_sequence)):
            current_domain = df_user_domain_sequence.iloc[k]['domain']
            same_domain = find_domain(current_domain, df_distinct_visit_list)

            if (same_domain == -1):
                df_temp_visits = pd.DataFrame(columns=('userID', 'domainID','domain','visit_times', 'spent_time', 'routine'))
                df_temp_visits.set_value(0, 'userID', current_userID)
                df_temp_visits.set_value(0, 'domainID', len(df_distinct_visit_list))
                df_temp_visits.set_value(0, 'domain', current_domain)
                df_temp_visits.set_value(0, 'visit_times', 1)
                df_temp_visits.set_value(0, 'spent_time', df_user_domain_sequence.iloc[k]['spent_milliseconds'])
                df_temp_visits.set_value(0, 'routine', 0)
                df_distinct_visit_list = df_distinct_visit_list.append(df_temp_visits)

            else:
                val_visit_times = df_distinct_visit_list.iloc[same_domain]['visit_times']
                val_spent_time = df_distinct_visit_list.iloc[same_domain]['spent_time']
                df_distinct_visit_list.iloc[
                    same_domain, df_distinct_visit_list.columns.get_loc('visit_times')] = val_visit_times + 1
                df_distinct_visit_list.iloc[
                    same_domain, df_distinct_visit_list.columns.get_loc('spent_time')] = val_spent_time + df_user_domain_sequence.iloc[k]['spent_milliseconds']
        print("USER {0} DISTINCT VISIT LIST DONE".format(current_userID))

        #print(df_distinct_visit_list)

        # DISTINCT PAGE DATA INTO DB
        for i in range(0, len(df_distinct_visit_list)):
            sql = "INSERT INTO user_pages_visit_list (userID,domainID,domain_name,visit_times,spent_time,routine) VALUES (" + str(
                df_distinct_visit_list.iloc[i]['userID']) + "," + str(df_distinct_visit_list.iloc[i]['domainID']) + ",'" + str(
                df_distinct_visit_list.iloc[i]['domain']) + "'," + str(df_distinct_visit_list.iloc[i]['visit_times']) + "," + str(
                df_distinct_visit_list.iloc[i]['spent_time']) + "," + str(df_distinct_visit_list.iloc[i]['routine']) + ");"
            print(sql)
            cursor.execute(sql)

    server.stop()

    print("End")
