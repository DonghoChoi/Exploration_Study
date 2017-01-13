#!/usr/bin/python
# Author: Dongho Choi

import os.path
import datetime
import time
import sys
import itertools
import pandas as pd
import numpy as np
from sshtunnel import SSHTunnelForwarder # for SSH connection
import pymysql.cursors # MySQL handling API
from pandas.io import sql
import mysql.connector
from sqlalchemy import create_engine
import sys
sys.path.append("./configs/")
import server_config # (1) info2_server (2) exploration_db

# Global Variables
dwellTime_cut = 30

participants_list = []
participants_combinations = []
'''
df_participants = pd.DataFrame()
df_pages_lab = pd.DataFrame()
df_queries = pd.DataFrame()
'''
df_query_second_task = pd.DataFrame(
    columns=('userID', 'url', 'time', 'stageID', 'questionID', 'source', 'host', 'query', 'dwellTime'))
#df_visits_second_task = pd.DataFrame(columns=('userID', 'url', 'stageID', 'source', 'host', 'dwellTime'))
df_visits_second_task = pd.DataFrame()
list_universe_visits_second_task = []
list_relevent_universe_visits_second_task = []

def get_participants_list(arg_pd):
    return arg_pd['userID']

def get_Coverage_Individual(user_a):
    df_user_a_visits_second_task = df_visits_second_task.loc[df_visits_second_task['userID'] == user_a]
    total_visits = df_user_a_visits_second_task['url'].drop_duplicates().values.tolist()
    print("Coverage of {0}: {1}".format(user_a, len(total_visits)))
    return len(total_visits)

def get_Unique_Coverage_Individual(user_a):
    df_user_a_visits_second_task = df_visits_second_task.loc[df_visits_second_task['userID'] == user_a]
    df_user_others_visits_second_task = df_visits_second_task.loc[~(df_visits_second_task['userID'] == user_a)]
    set_user_a_visits_second_task = set(df_user_a_visits_second_task['url'].drop_duplicates().values)
    set_user_others_visits_second_task = set(df_user_others_visits_second_task['url'].drop_duplicates().values)
    set_unique_coverage = set_user_a_visits_second_task - set_user_others_visits_second_task
    print("Unique Coverage of {0}: {1}".format(user_a, len(set_unique_coverage)))
    return len(set_unique_coverage)

def get_Useful_Coverage_Individual(user_a):
    df_user_a_visits_second_task = df_visits_second_task.loc[(df_visits_second_task['userID'] == user_a) & (df_visits_second_task['relevance'] == 1)]
    total_visits = df_user_a_visits_second_task['url'].drop_duplicates().values.tolist()
    print("Useful Coverage of {0}: {1}".format(user_a, len(total_visits)))
    return len(total_visits)

def get_Unique_Useful_Coverage_Individual(user_a):
    df_relevant_visits = df_visits_second_task.loc[df_visits_second_task['relevance']==1]
    df_user_a_visits = df_relevant_visits.loc[(df_relevant_visits['userID'] == user_a)]
    df_user_others_visits = df_relevant_visits.loc[~(df_relevant_visits['userID'] == user_a)]
    set_user_a_visits = set(df_user_a_visits['url'].drop_duplicates().values)
    set_user_others_visits = set(df_user_others_visits['url'].drop_duplicates().values)

    set_unique_useful_coverage = set_user_a_visits - set_user_others_visits
    print("Unique Useful Coverage of {0}: {1}".format(user_a, len(set_unique_useful_coverage)))
    return len(set_unique_useful_coverage)

def get_Coverage_Dyad(user_a, user_b):   # arg: both user_a and user_b are integer-type
    df_user_a_visits_second_task = df_visits_second_task.loc[df_visits_second_task['userID'] == user_a]
    df_user_b_visits_second_task = df_visits_second_task.loc[df_visits_second_task['userID'] == user_b]
    total_visits = df_user_a_visits_second_task.append(df_user_b_visits_second_task)['url'].drop_duplicates().values.tolist()
    #print(total_visits)
    #total_visits = total_visits.drop_duplicates('url')
    print("Coverage of {0} and {1}: {2}".format(user_a, user_b, len(total_visits)))
    return len(total_visits)

def get_Useful_Coverage_Dyad(user_a, user_b):  # arg: both user_a and user_b are integer-type
    df_user_a_visits_second_task = df_visits_second_task.loc[(df_visits_second_task['userID'] == user_a) & (df_visits_second_task['relevance'] == 1)]
    df_user_b_visits_second_task = df_visits_second_task.loc[(df_visits_second_task['userID'] == user_b) & (df_visits_second_task['relevance'] == 1)]
    total_visits = df_user_a_visits_second_task.append(df_user_b_visits_second_task)['url'].drop_duplicates().values.tolist()
    #total_visits = total_visits.drop_duplicates('url')
    print("Useful Coverage of {0} and {1}: {2}".format(user_a, user_b, len(total_visits)))
    return len(total_visits)

def get_Unique_Coverage_Dyad(user_a, user_b):    # arg: both user_a and user_b are integer-type
    df_user_a_b_visits_second_task = df_visits_second_task.loc[(df_visits_second_task['userID'] == user_a) | (df_visits_second_task['userID'] == user_b)]
    df_user_others_visits_second_task = df_visits_second_task.loc[~((df_visits_second_task['userID'] == user_a) | (df_visits_second_task['userID'] == user_b))]
    set_user_a_b_visits = set(df_user_a_b_visits_second_task['url'].drop_duplicates().values)
    set_user_others_visits = set(df_user_others_visits_second_task['url'].drop_duplicates().values)

    set_unique_Coverage = set_user_a_b_visits - set_user_others_visits
    print("Unique Coverage of {0} and {1}: {2}".format(user_a, user_b, len(set_unique_Coverage)))
    return len(set_unique_Coverage)

def get_Unique_Useful_Coverage_Dyad(user_a, user_b):
    df_relevant_visits = df_visits_second_task.loc[df_visits_second_task['relevance']==1]
    df_user_a_b_visits = df_relevant_visits.loc[(df_relevant_visits['userID'] == user_a) | (df_relevant_visits['userID'] == user_b)]
    df_user_others_visits = df_relevant_visits.loc[~((df_relevant_visits['userID'] == user_a) | (df_relevant_visits['userID'] == user_b))]
    set_user_a_b_visits = set(df_user_a_b_visits['url'].drop_duplicates().values)
    set_user_others_visits = set(df_user_others_visits['url'].drop_duplicates().values)

    set_unique_useful_coverage = set_user_a_b_visits - set_user_others_visits
    print("Unique Useful Coverage of {0} and {1}: {2}".format(user_a, user_b, len(set_unique_useful_coverage)))
    return len(set_unique_useful_coverage)

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

    # Get the participants list from the table of 'final_participants'
    df_participants = pd.read_sql('SELECT * FROM final_participants', con=connection)
    print("Participants Table READ")

    # Get the pages_lab table: importing all pages that include some pages related to study as well as new tab, etc.
    #df_pages_lab = pd.read_sql("SELECT userID,url,localTime,stageID,questionID,source,host,query FROM pages_lab WHERE (userID!=5001 AND (source NOT LIKE 'peopleanalytics') AND (source NOT LIKE ''))", con=connection)
    df_pages_lab = pd.read_sql("SELECT userID,url,time,stageID,questionID,source,host,query FROM pages_lab WHERE (userID!=5001)", con=connection)
    print("Pages Table READ")

    # Get the copy_data table
    #df_copy_data = pd.read_sql('SELECT * FROM copy_data', con=connection)

    # Get the queries table
    df_queries = pd.read_sql('SELECT * FROM queries WHERE (userID!=5001)', con=connection)

    # Get the demographic survey responses
    #df_demographic = pd.read_sql('SELECT * FROM questionnaire_demographic', con=connection)

    # Get the spatial capability score
    #df_spatial = pd.read_sql('SELECT * FROM spatial_capability', con=connection)

    #server.stop()

    # READ AND FILL THE PARTICIPANTS LIST WITH COMBINATIONS
    participants_list = df_participants['userID'].tolist()
    num_participants = len(participants_list) # number of participants
    print('number of participants:{0}'.format(num_participants))
    participants_combinations = [list(x) for x in itertools.combinations(participants_list, 2)]
    print("participants_combinations:", participants_combinations)
    num_combinations = len(participants_combinations)

    ## CALCULATE DWELL TIME BETWEEN PAGES
    df_pages_with_dwell_time = pd.DataFrame(columns=('userID','url','time','stageID','questionID','source','host','query','dwellTime'))
    for i in range(0, num_participants):
    #for i in range(0,3):
        current_userID = participants_list[i]
        df_temp_pages = df_pages_lab.loc[df_pages_lab['userID']==current_userID] # Data of current user
        second_start = False
        for j in range(0,len(df_temp_pages)-1):
            if (df_temp_pages.iloc[j]['stageID'] == 41) and (second_start == False): # Marking the start time of the exploratory search session
                startTime_second_task = df_temp_pages.iloc[j]['time']
                print("start time of second task of user {0}: {1}".format(current_userID, startTime_second_task))
                second_start = True
            df_temp = df_temp_pages.iloc[j]
            dwellTime = df_temp_pages.iloc[j+1]['time'] - df_temp_pages.iloc[j]['time']
            df_temp['dwellTime'] = dwellTime.total_seconds()
            df_pages_with_dwell_time = df_pages_with_dwell_time.append(df_temp)

        df_temp = df_temp_pages.iloc[len(df_temp_pages)-1]
        dwellTime = (startTime_second_task + datetime.timedelta(minutes=20) - df_temp['time']).total_seconds()
        if (dwellTime > 0):
            df_temp['dwellTime'] = dwellTime
            df_pages_with_dwell_time = df_pages_with_dwell_time.append(df_temp)

    # GETTING RID OF UNNECESSARY PAGES
    df_pages_with_dwell_time = df_pages_with_dwell_time[~df_pages_with_dwell_time['source'].str.contains("peopleanalytics")]
    df_pages_with_dwell_time = df_pages_with_dwell_time[~df_pages_with_dwell_time['url'].str.contains("about:")]
    #print(df_pages_with_dwell_time)
    #print("line:", len(df_pages_with_dwell_time))

    # BUILD EXPLORATORY SEARCH VISIT PAGES
    df_pages_second_task = pd.DataFrame(columns=('userID','url','time','stageID','questionID','source','host','query','dwellTime'))
    for i in range(0,num_participants):
    #for i in range(0, 3):
        #current_userID = participants_list.iloc[i]['userID']
        current_userID = participants_list[i]
        df_temp_pages = df_pages_with_dwell_time.loc[df_pages_with_dwell_time['userID'] == current_userID]

        df_temp_pages_first_task = df_temp_pages.loc[df_temp_pages['stageID']==31] # Visited pages during the first task
        list_pages_first_task = df_temp_pages_first_task['url'].tolist()

        df_temp_pages_second_task = df_temp_pages.loc[df_temp_pages['stageID']==41] # Visited pages during the second task

        for j in range(0,len(df_temp_pages_second_task.index)):
            if (df_temp_pages_second_task.iloc[j]['url'] not in list_pages_first_task):
                df_pages_second_task = df_pages_second_task.append(df_temp_pages_second_task.iloc[j])
                '''
                print("duplicated")
            else:
                print("new in exploratory search")
                df_pages_second_task = df_pages_second_task.append(df_temp_pages_second_task.iloc[j])
                '''
    #print(df_pages_second_task)

    # BUILD SERP/QUERY TABLE and MERGE VISITING PAGES FOR EXPLORATORY TASK
    for i in range(0,num_participants):
    #for i in range(0,3):
        #current_userID = participants_list.iloc[i]['userID']
        current_userID = participants_list[i]
        df_temp_pages = df_pages_second_task.loc[df_pages_lab['userID'] == current_userID]
        '''
        df_temp_query_second_task = df_temp_pages.loc[''(df_temp_pages['query'] == "")]  # SERP during the second task
        df_query_second_task = df_query_second_task.append(df_temp_pages_second_task)
        '''
        df_temp_visits_second_task = df_temp_pages.loc[df_temp_pages['query'] == ""] # Webpages visits
        df_temp_visits_second_task['totaldwellTime'] = df_temp_visits_second_task.groupby('url')['dwellTime'].transform('sum')
        df_temp_visits_second_task.drop(['time','dwellTime','questionID','query'], axis=1, inplace=True)
        df_temp_visits_second_task = df_temp_visits_second_task.drop_duplicates('url')
        df_temp_visits_second_task.rename(columns={'totaldwellTime': 'dwellTime'}, inplace=True)
        df_temp_visits_second_task['relevance'] = 0
        df_temp_visits_second_task.loc[df_temp_visits_second_task['dwellTime'] >= dwellTime_cut,'relevance'] = 1
        df_visits_second_task = df_visits_second_task.append(df_temp_visits_second_task)
    #print("###### web pages in second task #####")
    #print(df_visits_second_task)

    # CREATE UNIVERSE OF COVERAGE
    list_universe_visits_second_task = df_visits_second_task['url'].drop_duplicates().values.tolist()
    print("universe of coverage size:", len(list_universe_visits_second_task))

    list_relevent_universe_visits_second_task = df_visits_second_task.loc[df_visits_second_task['relevance']==1]['url'].drop_duplicates().values.tolist()
    print("universe of relevant coverage size:", len(list_relevent_universe_visits_second_task))

    # QUERY

    # DATA OF INDIVIDUALS
    df_individual_data = pd.DataFrame(index=range(0, num_participants),columns=('userID', 'Coverage', 'UniqueCoverage', 'UsefulCoverage', 'UniqueUsefulCoverage'))
    print(df_individual_data)
    for i in range(0,num_participants):
        #df_temp_individuals_data = pd.DataFrame(columns=('userID', 'Coverage', 'UniqueCoverage', 'RelevantCoverage', 'UniqueRelevantCoverage'))
        current_userID = participants_list[i]
        df_individual_data.set_value(i,'userID', current_userID)
        df_individual_data.set_value(i,'Coverage',get_Coverage_Individual(current_userID))
        df_individual_data.set_value(i,'Coverage',get_Coverage_Individual(current_userID))
        df_individual_data.set_value(i,'UniqueCoverage',get_Unique_Coverage_Individual(current_userID))
        df_individual_data.set_value(i,'UsefulCoverage',get_Useful_Coverage_Individual(current_userID))
        df_individual_data.set_value(i,'UniqueUsefulCoverage', get_Unique_Useful_Coverage_Individual(current_userID))
    print("##### Individual Data ######")
    print(df_individual_data)

    # DATA OF DYADS
    df_dyad_data = pd.DataFrame(index=range(0, num_combinations),columns=('user_a', 'user_b','Coverage', 'UniqueCoverage', 'UsefulCoverage', 'UniqueUsefulCoverage'))
    for i in range(0, num_combinations):
        pair = participants_combinations[i]
        user_a = pair[0]
        user_b = pair[1]
        df_dyad_data.set_value(i,'user_a',user_a)
        df_dyad_data.set_value(i,'user_b',user_b)
        df_dyad_data.set_value(i, 'Coverage', get_Coverage_Dyad(user_a, user_b))
        df_dyad_data.set_value(i, 'UniqueCoverage', get_Unique_Coverage_Dyad(user_a, user_b))
        df_dyad_data.set_value(i, 'UsefulCoverage', get_Useful_Coverage_Dyad(user_a, user_b))
        df_dyad_data.set_value(i, 'UniqueUsefulCoverage', get_Unique_Useful_Coverage_Dyad(user_a, user_b))
    print("##### Dyad Data #####")
    print(df_dyad_data)


    # SAVE DATAFRAMES INTO SERVER

    # INDIVIDUAL DATA
    sql = "DROP TABLE IF EXISTS individual_data;"
    cursor.execute(sql)
    sql = "CREATE TABLE individual_data (userID int(11), Coverage int(11), UniqueCoverage int(11), UsefulCoverage int(11), UniqueUsefulCoverage int(11));"
    cursor.execute(sql)
    for i in range(0,num_participants):
        sql = "INSERT INTO individual_data (userID,Coverage,UniqueCoverage,UsefulCoverage,UniqueUsefulCoverage) VALUES (" + \
            str(df_individual_data.iloc[i]['userID']) + "," +  str(df_individual_data.iloc[i]['Coverage']) + "," + str(df_individual_data.iloc[i]['UniqueCoverage']) + "," + \
            str(df_individual_data.iloc[i]['UsefulCoverage']) + "," + str(df_individual_data.iloc[i]['UniqueUsefulCoverage']) + ");"
        cursor.execute(sql)

    # DYAD DATA
    sql = "DROP TABLE IF EXISTS dyad_data;"
    cursor.execute(sql)
    sql = "CREATE TABLE dyad_data (user_a int(11), user_b int(11), Coverage int(11), UniqueCoverage int(11), UsefulCoverage int(11), UniqueUsefulCoverage int(11));"
    cursor.execute(sql)
    for i in range(0, num_combinations):
        sql = "INSERT INTO dyad_data (user_a,user_b,Coverage,UniqueCoverage,UsefulCoverage,UniqueUsefulCoverage) VALUES (" + \
              str(df_dyad_data.iloc[i]['user_a']) + "," + str(df_dyad_data.iloc[i]['user_b']) + "," +str(df_dyad_data.iloc[i]['Coverage']) + "," + str(
            df_dyad_data.iloc[i]['UniqueCoverage']) + "," + \
              str(df_dyad_data.iloc[i]['UsefulCoverage']) + "," + str(
            df_dyad_data.iloc[i]['UniqueUsefulCoverage']) + ");"
        cursor.execute(sql)

    server.stop()

    print("end")
