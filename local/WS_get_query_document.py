#!/usr/bin/python
# Author: Dongho Choi
'''
File name: WS_get_query_document.py

-
'''

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

    # Get pages_lab table: importing all pages that participants visited during the web search tasks
    df_pages_lab = pd.read_sql("SELECT userID,url,query,localTimestamp_int,stageID,questionID,host FROM pages_lab WHERE (userID!=5001)", con=connection)
    print("pages_lab Table READ")

    # Get the participants list from the table of 'final_participants'
    df_participants = pd.read_sql('SELECT * FROM final_participants', con=connection)
    print("Participants Table READ")
    userID_list = df_participants['userID'].tolist()

    # Get users' performance on task 1
    df_task1_completed_questions = pd.read_sql("SELECT userID, count(questionID) as completed_questions FROM user_task1_answering_time WHERE (running_time>0) GROUP BY userID", con=connection)

    # url visit list
    for i in range(0,len(userID_list)): # i - current userID
    #for i in range(0,1):
        current_userID = userID_list[i]
        #current_userID = i
        print("Current User:",current_userID)

        df_user_pages = df_pages_lab.loc[df_pages_lab['userID']==current_userID].sort_values(['localTimestamp_int'])

        # create a dataframe
        df_with_spent_time = pd.DataFrame(index=range(0,len(df_user_pages)-1),columns=('userID','url','query','spent_time','task','questionID','host'))
        # go through the pages list calculating spent time
        for j in range(0,len(df_user_pages)-1):
            # Calculating spent time
            spent_time = df_user_pages.iloc[j+1,df_user_pages.columns.get_loc('localTimestamp_int')] - df_user_pages.iloc[j,df_user_pages.columns.get_loc('localTimestamp_int')]
            df_with_spent_time.iloc[j,df_with_spent_time.columns.get_loc('spent_time')] = spent_time

            # Rest of them
            df_with_spent_time.iloc[j,df_with_spent_time.columns.get_loc('userID')] = current_userID
            df_with_spent_time.iloc[j,df_with_spent_time.columns.get_loc('url')] = df_user_pages.iloc[j,df_user_pages.columns.get_loc('url')]
            df_with_spent_time.iloc[j, df_with_spent_time.columns.get_loc('query')] = df_user_pages.iloc[j, df_user_pages.columns.get_loc('query')]
            if ((df_user_pages.iloc[j,df_user_pages.columns.get_loc('stageID')]==30) | (df_user_pages.iloc[j,df_user_pages.columns.get_loc('stageID')]==31)):
                df_with_spent_time.iloc[j,df_with_spent_time.columns.get_loc('task')] = 1
            if ((df_user_pages.iloc[j,df_user_pages.columns.get_loc('stageID')]==40) | (df_user_pages.iloc[j,df_user_pages.columns.get_loc('stageID')]==41)):
                df_with_spent_time.iloc[j,df_with_spent_time.columns.get_loc('task')] = 2
            df_with_spent_time.iloc[j,df_with_spent_time.columns.get_loc('questionID')] = df_user_pages.iloc[j,df_user_pages.columns.get_loc('questionID')]
            df_with_spent_time.iloc[j, df_with_spent_time.columns.get_loc('host')] = df_user_pages.iloc[j, df_user_pages.columns.get_loc('host')]
        # delete lines with user study related pages
        df_with_spent_time = df_with_spent_time.loc[(df_with_spent_time['host']!='peopleanalytics.org')&(df_with_spent_time['host']!='')&(df_with_spent_time['url']!='https://www.google.com/')]

        # consider search sessions that were completed
        num_question_completed = int(df_task1_completed_questions.loc[(df_task1_completed_questions['userID']==current_userID),'completed_questions'])

        df_pages_task1 = df_with_spent_time.loc[df_with_spent_time['task']==1]
        #print(df_pages_task1)
        df_pages_task2 = df_with_spent_time.loc[df_with_spent_time['task']==2]

        ### Task 1 ###
        previous_query = set()
        for k in range(0,num_question_completed):
            current_question = k+1
            df_pages_in_question_temp = df_pages_task1.loc[df_pages_task1['questionID']==current_question]
            print(df_pages_in_question_temp)
            df_pages_in_question = pd.DataFrame(columns=('userID','url','query','spent_time','task','questionID','serp'))
            for l in range(0,len(df_pages_in_question_temp)):
                df_current = df_pages_in_question_temp.iloc[l]
                if df_current.query == '':
                    serp = 'no'
                elif df_current.query != '':
                    serp = 'yes'
                df_current['serp'] = serp
                if (l < len(df_pages_in_question_temp)-1):
                    df_next = df_pages_in_question_temp.iloc[l+1]
                    #print("spent time: {0}".format(df_current.spent_time))
                    if ((df_current.spent_time < 500) & (df_current.host == df_next.host)): # in case there is a redirection in the clicked document page
                        print("AUTOMATIC REDIRECTION: {0} to {1} in {2}".format(df_current.url,df_next.url,df_current.spent_time))
                    else:
                        #print("serp is {0}".format(serp))
                        df_pages_in_question = df_pages_in_question.append(df_current)
                else:
                    # just add the df_current
                    print("It's last row. Just add this to the dataframe.")
                    df_pages_in_question = df_pages_in_question.append(df_current)

            print("________ df_pages_in_question_after_filtering________")
            print(df_pages_in_question)

            current_query = ''
            for m in range(0,len(df_pages_in_question)):
                df_current = df_pages_in_question.iloc[m]
                if df_current.serp == 'yes':
                    current_query = df_current.query
                elif df_current.serp == 'no':
                    df_pages_in_question.iloc[m,df_pages_in_question.columns.get_loc('query')] = current_query

            print("_________ df_pages_in_question: with query ____________")
            print(df_pages_in_question)

            query_list = df_pages_in_question['query'].tolist()
            query_set = set(query_list)
            num_a = len(query_set)
            print("number of query: {0}".format(num_a))

            for q in query_set:
                current_query = q
                if (current_query not in previous_query):
                    df_temp_query = df_pages_in_question.loc[df_pages_in_question['query']==current_query]
                    df_assessed_document = df_temp_query.loc[df_temp_query['serp']=='no']
                    #df_assessed_group_by_url = df_assessed_document.groupby(["url"]).sum()
                    # userID,task,questionID,query,serp,spent_time
                    #print("####### group by url ########")
                    #print(df_assessed_group_by_url)
                    num_assessed_document = len(set(df_assessed_document['url'].tolist()))
                    print("query:{0}".format(current_query))
                    print("assessed documents:{0}".format(set(df_assessed_document['url'].tolist())))
                    print("number of assessed documents:{0}".format(num_assessed_document))

                    sql = "INSERT INTO user_WS_query_assessment (userID,task,questionID,query,num_assessment) VALUES ({0},1,{1},'{2}',{3})".\
                        format(str(current_userID),str(current_question),str(connection.escape_string(current_query)),num_assessed_document)
                    print(sql)
                    cursor.execute(sql)

                else:
                    print("EXISTED IN PREVIOUS QUERY SESSION! - {0}".format(current_query))


            previous_query = previous_query | query_set

            # userID, questionID, query, url, total_spent_time

            # userID, questionID, query, num_assessment

        ### TASK 2 ###
        df_pages_in_task2 = pd.DataFrame(columns=('userID', 'url', 'query', 'spent_time', 'task', 'questionID', 'serp'))

        for n in range(0, len(df_pages_task2)):
            df_current = df_pages_task2.iloc[n]
            if df_current.query == '':
                serp = 'no'
            elif df_current.query != '':
                serp = 'yes'
            df_current['serp'] = serp
            if (n < len(df_pages_task2) - 1):
                df_next = df_pages_task2.iloc[n + 1]
                # print("spent time: {0}".format(df_current.spent_time))
                if ((df_current.spent_time < 500) & (
                    df_current.host == df_next.host)):  # in case there is a redirection in the clicked document page
                    print("AUTOMATIC REDIRECTION: {0} to {1} in {2}".format(df_current.url, df_next.url,
                                                                            df_current.spent_time))
                else:
                    # print("serp is {0}".format(serp))
                    df_pages_in_task2 = df_pages_in_task2.append(df_current)
            else:
                # just add the df_current
                print("It's last row. Just add this to the dataframe.")
                df_pages_in_task2 = df_pages_in_task2.append(df_current)

        print("________ df_pages_in_TASK2_after_filtering________")
        print(df_pages_in_task2)

        current_query = ''
        for p in range(0, len(df_pages_in_task2)):
            df_current = df_pages_in_task2.iloc[p]
            if df_current.serp == 'yes':
                current_query = df_current.query
            elif df_current.serp == 'no':
                df_pages_in_task2.iloc[p, df_pages_in_task2.columns.get_loc('query')] = current_query

        print("_________ df_pages_in_TASK2: with query ____________")
        print(df_pages_in_task2)

        query_list = df_pages_in_task2['query'].tolist()
        query_set = set(query_list)
        num_a = len(query_set)
        print("number of query in TASK2: {0}".format(num_a))

        for q in query_set:
            current_query = q
            if (current_query not in previous_query):
                df_temp_query = df_pages_in_task2.loc[df_pages_in_task2['query'] == current_query]
                df_assessed_document = df_temp_query.loc[df_temp_query['serp'] == 'no']
                num_assessed_document = len(set(df_assessed_document['url'].tolist()))
                print("query:{0}".format(current_query))
                print("assessed documents:{0}".format(set(df_assessed_document['url'].tolist())))
                print("number of assessed documents:{0}".format(num_assessed_document))

                sql = "INSERT INTO user_WS_query_assessment (userID,task,questionID,query,num_assessment) VALUES ({0},2,0,'{1}',{2})". \
                    format(str(current_userID), str(connection.escape_string(current_query)),str(num_assessed_document))
                print(sql)
                cursor.execute(sql)

            else:
                print("EXISTED IN PREVIOUS QUERY SESSION! - {0}".format(current_query))


    '''

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
    '''
    server.stop()

    print("End")
