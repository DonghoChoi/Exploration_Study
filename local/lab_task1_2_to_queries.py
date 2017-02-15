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
    df_queries = pd.read_sql("SELECT userID,stageID,questionID,query FROM queries WHERE (userID!=5001)", con=connection)
    #df_pages_all = df_pages_all.rename(columns={'epochTime': 'localTimestamp'})
    print("queries Table READ")
    print("Length of queries is ",len(df_queries))

    # Get the participants list from the table of 'final_participants'
    df_participants = pd.read_sql('SELECT * FROM final_participants', con=connection)
    print("Participants Table READ")

    # READ AND FILL THE PARTICIPANTS LIST WITH COMBINATIONS
    participants_list = df_participants['userID'].tolist()
    num_participants = len(participants_list) # number of participants
    print('number of participants:{0}'.format(num_participants))

    # url visit list
    for i in range(0, num_participants): # i - current userID
    #for i in range(0,1):
        current_userID = participants_list[i]
        print("Current User:",current_userID)
        df_user_queries = df_queries.loc[df_queries['userID']==current_userID]    # select rows that contains this specific participant

        df_task1_queries = df_user_queries.loc[df_user_queries['stageID']==31]
        df_task2_queries = df_user_queries.loc[df_user_queries['stageID']==41]

        ###### TASK 1 #######
        # number of questions that the person asked
        num_questions = df_task1_queries['questionID'].max()

        task1_query_list = []

        for j in range(0, num_questions):
            df_question_queries = df_task1_queries.loc[df_task1_queries['questionID']==j+1]
            query_list = df_question_queries['query'].tolist()
            query_set = set(query_list)
            num_a = len(query_set)
            # check if queries that issued for previous questions are included.
            stacked_query_set = set(task1_query_list)
            query_set = query_set - stacked_query_set
            num_b = len(query_set)
            repeated_query = num_a-num_b
            if (repeated_query != 0):
                print("--------------- Query used in previous questions: {0} times".format(repeated_query))
            num_query_issued = len(query_list)-repeated_query
            num_distinct_query_issued = len(query_set)
            print("in question {0}: {1} queries issued, and {2} distinct queries".format(j+1,num_query_issued,num_distinct_query_issued))

            sql = "INSERT INTO user_task1_queries (userID,questionID,query_issued,distinct_query) VALUES (" + str(current_userID) + "," + str(j+1) + "," + str(num_query_issued) + "," + str(num_distinct_query_issued) + ");"
            print(sql)
            cursor.execute(sql)

            task1_query_list = list(set(task1_query_list + query_list))

        ###### TASK 2 ######
        task2_query_list = df_task2_queries['query'].tolist()
        task2_query_set = set(task2_query_list)
        num_c = len(task2_query_set)
        stacked_query_set = set(task1_query_list)
        task2_query_set = task2_query_set - stacked_query_set
        num_d = len(task2_query_set)
        task2_repeated_query = num_c-num_d
        if(task2_repeated_query != 0):
            print("------------ Query in task1 is revisited")
        num_task2_query_issued = len(task2_query_list) - task2_repeated_query
        num_task2_distinct_query_issued = len(task2_query_set)
        print("in task2: {0} queries issued, and {1} distinct queries".format(num_task2_query_issued,num_task2_distinct_query_issued))
        sql = "INSERT INTO user_task2_queries (userID,query_issued,distinct_query_issued) VALUES (" + str(current_userID) + "," + str(num_task2_query_issued) + "," + str(num_task2_distinct_query_issued) + ");"
        print(sql)
        cursor.execute(sql)

    server.stop()

    print("End")
