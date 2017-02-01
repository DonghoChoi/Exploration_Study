#!/usr/bin/python
# Author: Dongho Choi

import math
import time
import pandas as pd
from math import log
from sshtunnel import SSHTunnelForwarder # for SSH connection
import pymysql.cursors # MySQL handling API
import matplotlib.pyplot as plt
import sys
import random
sys.path.append("./configs/")
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
                                 db=server_config.exploration_db['database'])


    connection.autocommit(True)
    cursor = connection.cursor()
    print("MySQL connection established.")

    # Get the participants list from the table of 'final_participants'
    df_participants = pd.read_sql('SELECT * FROM final_participants', con=connection)
    participants_list = df_participants['userID'].tolist()
    print("Participants Table READ")

    # Get time preference related responses
    df_time_responses = pd.read_sql_query('SELECT userID,SI21A,SI21B,SI21C,SI21D,SI21E,SI22A,SI22B,SI22C,SI22D,SI22E FROM user_risk_time_preference_responses', con=connection)
    print("Time preference responses data READ")
    print(df_time_responses)

    for i in range(0,len(participants_list)):
        current_userID = participants_list[i]
        df_temp_responses = df_time_responses.loc[df_time_responses['userID'] == current_userID]  # Data of current user
        #print(df_temp_responses)

        # Process - module A
        SI21A = df_temp_responses.iloc[0,df_temp_responses.columns.get_loc('SI21A')]
        SI21B = df_temp_responses.iloc[0,df_temp_responses.columns.get_loc('SI21B')]
        SI21C = df_temp_responses.iloc[0,df_temp_responses.columns.get_loc('SI21C')]
        SI21D = df_temp_responses.iloc[0,df_temp_responses.columns.get_loc('SI21D')]
        SI21E = df_temp_responses.iloc[0,df_temp_responses.columns.get_loc('SI21E')]

        if(SI21E == "Yes"):
            #print("User {0} has negative time discount -> dropping".format(current_userID))
            timeA = -1
        if(SI21C == "$60,000 in 1 year"):
            timeA = 3
        if(SI21C == "$10,000 today"):
            timeA = 4
        if(SI21D == "$20,000 in 1 year"):
            timeA = 1
        if(SI21D == "$10,000 today"):
            timeA = 2

        print("user {0}'s timeA = {1}".format(current_userID,timeA))

        # Process - module B
        SI22A = df_temp_responses.iloc[0,df_temp_responses.columns.get_loc('SI22A')]
        SI22B = df_temp_responses.iloc[0,df_temp_responses.columns.get_loc('SI22B')]
        SI22C = df_temp_responses.iloc[0,df_temp_responses.columns.get_loc('SI22C')]
        SI22D = df_temp_responses.iloc[0,df_temp_responses.columns.get_loc('SI22D')]
        SI22E = df_temp_responses.iloc[0,df_temp_responses.columns.get_loc('SI22E')]

        if(SI22E == "Yes"):
            #print("User {0} has negative time discount -> dropping".format(current_userID))
            timeB = -1
        if(SI22C == "$$100,000 in 5 years"):
            timeB = 3
        if(SI22C == "$10,000 today"):
            timeB = 4
        if(SI22D == "$$20,000 in 5 years"):
            timeB = 1
        if(SI22D == "$10,000 today"):
            timeB = 2

        print("user {0}'s timeB = {1}".format(current_userID,timeB))

        sql = "INSERT INTO user_time_preference (userID,timeA,timeB) VALUES (" + str(current_userID) + "," + str(timeA) + "," + str(timeB) + ");"
        print(sql)
        cursor.execute(sql)

    server.stop()

    print("END")

