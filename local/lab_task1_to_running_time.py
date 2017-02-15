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
    df_pages_lab = pd.read_sql("SELECT userID,url,localTimestamp_int,stageID,questionID,query FROM pages_lab WHERE (userID!=5001)", con=connection) # localTimestamp means the epochtime in milliseconds
    #df_pages_all = df_pages_all.rename(columns={'epochTime': 'localTimestamp'})
    print("pages_lab Table READ")
    print("Length of pages_lab is ",len(df_pages_lab))

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
        df_user_pages = df_pages_lab.loc[df_pages_lab['userID']==current_userID]    # select rows that contains this specific participant
        #df_spent_time = pd.DataFrame({"spent_time":np.zeros(len(df_user_pages))},index=range(0,len(df_user_pages)))
        #df_user_pages = pd.concat([df_user_pages,df_spent_time])
        #df_user_pages.loc[:, 'spent_time'] = np.zeros(len(df_user_pages))

        maintask1_url = "http://peopleanalytics.org/ExplorationStudy/instruments/maintask.php"
        maintask2_url = "http://peopleanalytics.org/ExplorationStudy/instruments/maintask2.php"

        df_with_spent_time = pd.DataFrame(index=range(0,len(df_user_pages)-1),columns=('userID','url','visit_time','spent_time','task','questionID','query'))
        # going through the url visit list calculating spent time
        for j in range(0,len(df_user_pages)-1):
            # Calculating spent time
            #df_temp = df_user_pages.iloc[j,:]
            spent_time = df_user_pages.iloc[j+1,df_user_pages.columns.get_loc('localTimestamp_int')] - df_user_pages.iloc[j,df_user_pages.columns.get_loc('localTimestamp_int')]
            df_with_spent_time.iloc[j,df_with_spent_time.columns.get_loc('spent_time')] = spent_time

            # Rest of them
            df_with_spent_time.iloc[j,df_with_spent_time.columns.get_loc('userID')] = current_userID
            df_with_spent_time.iloc[j,df_with_spent_time.columns.get_loc('url')] = df_user_pages.iloc[j,df_user_pages.columns.get_loc('url')]
            df_with_spent_time.iloc[j,df_with_spent_time.columns.get_loc('visit_time')] = df_user_pages.iloc[j,df_user_pages.columns.get_loc('localTimestamp_int')]
            if ((df_user_pages.iloc[j,df_user_pages.columns.get_loc('stageID')]==30) | (df_user_pages.iloc[j,df_user_pages.columns.get_loc('stageID')]==31)):
                df_with_spent_time.iloc[j,df_with_spent_time.columns.get_loc('task')] = 1
            if ((df_user_pages.iloc[j,df_user_pages.columns.get_loc('stageID')]==40) | (df_user_pages.iloc[j,df_user_pages.columns.get_loc('stageID')]==41)):
                df_with_spent_time.iloc[j,df_with_spent_time.columns.get_loc('task')] = 2
            #print("stageID: {0}, task: {1}".format(df_user_pages.iloc[j,df_user_pages.columns.get_loc('stageID')],df_with_spent_time.iloc[j,df_with_spent_time.columns.get_loc('task')]))
            df_with_spent_time.iloc[j,df_with_spent_time.columns.get_loc('questionID')] = df_user_pages.iloc[j,df_user_pages.columns.get_loc('questionID')]
            df_with_spent_time.iloc[j, df_with_spent_time.columns.get_loc('query')] = df_user_pages.iloc[j, df_user_pages.columns.get_loc('query')]

        # number of questions that the person asked
        num_questions = df_with_spent_time['questionID'].max()
        df_question_running_time = pd.DataFrame(index=range(0,num_questions),columns=('questionID','start_time','end_time','running_time'))
        current_questionID = 1
        in_task2 = False
        #question_started = False
        #question_finished = False
        df_question_running_time.iloc[current_questionID - 1, df_question_running_time.columns.get_loc('questionID')] = current_questionID
        df_question_running_time.iloc[current_questionID - 1, df_question_running_time.columns.get_loc('start_time')] = df_with_spent_time.iloc[0, df_with_spent_time.columns.get_loc('visit_time')]
        task_start_time = df_with_spent_time.iloc[0, df_with_spent_time.columns.get_loc('visit_time')]
        for k in range(1,len(df_with_spent_time)-1):
            #question_started = True
            if (df_with_spent_time.iloc[k,df_with_spent_time.columns.get_loc('url')]==maintask1_url):
                print("Maintask 1 url")
                '''
                if (question_started == False): # First visit the the task1 url, which means they start the question of questionID
                    df_question_running_time.iloc[current_questionID-1,df_question_running_time.columns.get_loc('questionID')] = current_questionID
                    df_question_running_time.iloc[current_questionID-1,df_question_running_time.columns.get_loc('start_time')] = df_with_spent_time.iloc[k,df_with_spent_time.columns.get_loc('visit_time')]
                    question_started = True
                '''
                #if ((question_started == True) & (df_with_spent_time.iloc[k+1,df_with_spent_time.columns.get_loc('questionID')] != current_questionID)): # When entering the right answer
                if ((df_with_spent_time.iloc[k+1,df_with_spent_time.columns.get_loc('questionID')] != current_questionID)): # When entering the right answer
                    print("question {0} finished".format(current_questionID))
                    df_question_running_time.iloc[current_questionID-1,df_question_running_time.columns.get_loc('end_time')] = df_with_spent_time.iloc[k,df_with_spent_time.columns.get_loc('visit_time')]
                    df_question_running_time.iloc[current_questionID-1,df_question_running_time.columns.get_loc('running_time')] = df_question_running_time.iloc[current_questionID-1,df_question_running_time.columns.get_loc('end_time')] - df_question_running_time.iloc[current_questionID-1,df_question_running_time.columns.get_loc('start_time')]
                    df_question_running_time.iloc[current_questionID,df_question_running_time.columns.get_loc('start_time')] = df_with_spent_time.iloc[k,df_with_spent_time.columns.get_loc('visit_time')]
                    current_questionID += 1
                    df_question_running_time.iloc[current_questionID - 1, df_question_running_time.columns.get_loc('questionID')] = current_questionID
                    #question_started = False
                if (current_questionID == num_questions):
                    if(df_with_spent_time.iloc[k+1,df_with_spent_time.columns.get_loc('task')]==2):
                        if (df_with_spent_time.iloc[k+1,df_with_spent_time.columns.get_loc('visit_time')]-task_start_time < 1200000):
                            print("question {0} solved".format(current_questionID))
                            df_question_running_time.iloc[current_questionID - 1, df_question_running_time.columns.get_loc('end_time')] = df_with_spent_time.iloc[k+1, df_with_spent_time.columns.get_loc('visit_time')]
                            df_question_running_time.iloc[current_questionID - 1, df_question_running_time.columns.get_loc('running_time')] = df_question_running_time.iloc[current_questionID - 1, df_question_running_time.columns.get_loc('end_time')] - df_question_running_time.iloc[current_questionID - 1, df_question_running_time.columns.get_loc('start_time')]
                        else:
                            print("question {0} not finished".format(current_questionID))
                            df_question_running_time.iloc[current_questionID - 1, df_question_running_time.columns.get_loc('end_time')] = -1
                            df_question_running_time.iloc[current_questionID - 1, df_question_running_time.columns.get_loc('running_time')] = -1

            if (df_with_spent_time.iloc[k,df_with_spent_time.columns.get_loc('task')]==2 and (in_task2 == False)):
                print("TASK 2 STARTED")
                if(pd.isnull(df_question_running_time.iloc[current_questionID - 1, df_question_running_time.columns.get_loc('end_time')]) == True):
                    df_question_running_time.iloc[current_questionID - 1, df_question_running_time.columns.get_loc('end_time')] = -1
                    df_question_running_time.iloc[current_questionID - 1, df_question_running_time.columns.get_loc('running_time')] = -1
                    print("NaN in the last question!!")
                in_task2 = True

        print(df_question_running_time)

        # TIMELINE DATA INTO DB
        for l in range(0, len(df_question_running_time)):
            sql = "INSERT INTO user_task1_answering_time (userID,questionID,start_time,end_time,running_time) VALUES (" + str(current_userID) + "," + str(df_question_running_time.iloc[l]['questionID']) + "," + str(df_question_running_time.iloc[l]['start_time']) + "," + str(df_question_running_time.iloc[l]['end_time']) + "," + str(df_question_running_time.iloc[l]['running_time']) + ");"
            print(sql)
            cursor.execute(sql)

    server.stop()

    print("End")
