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

    # Get risk preference related responses
    df_risk_responses = pd.read_sql_query('SELECT userID,SI01,SI02,SI03,SI04,SI05,SI11,SI12,SI13,SI14,SI15 FROM user_risk_time_preference_responses', con=connection)
    print("Time preference responses data READ")
    print(df_risk_responses)

    for i in range(0,len(participants_list)):
        current_userID = participants_list[i]
        df_temp_responses = df_risk_responses.loc[df_risk_responses['userID'] == current_userID]  # Data of current user
        #print(df_temp_responses)

        # Process - module A
        SI01 = df_temp_responses.iloc[0,df_temp_responses.columns.get_loc('SI01')]
        SI02 = df_temp_responses.iloc[0,df_temp_responses.columns.get_loc('SI02')]
        SI03 = df_temp_responses.iloc[0,df_temp_responses.columns.get_loc('SI03')]
        SI04 = df_temp_responses.iloc[0,df_temp_responses.columns.get_loc('SI04')]
        SI05 = df_temp_responses.iloc[0,df_temp_responses.columns.get_loc('SI05')]

        if (((SI01 == "$8,000 per month")|(SI01 == "Don't know"))&(SI02 == "Still picks option 1")):
            riskA = -1
        elif (SI01 == "$16,000 or $8,000 per month"):
            if (SI03 == "$8,000"):
                if(SI04 == "$8,000"):
                    riskA = 4
                elif (SI04 == "$16,000 or $6,000"):
                    riskA = 3
            elif (SI03 == "$16,000 or $4,000"):
                if(SI05 == "$8,000"):
                    riskA = 2
                elif(SI05 == "$16,000 or $2,000"):
                    riskA = 1

        print("user {0}'s riskA = {1}".format(current_userID, riskA))

        # Process - module B
        SI11 = df_temp_responses.iloc[0, df_temp_responses.columns.get_loc('SI11')]
        SI12 = df_temp_responses.iloc[0, df_temp_responses.columns.get_loc('SI12')]
        SI13 = df_temp_responses.iloc[0, df_temp_responses.columns.get_loc('SI13')]
        SI14 = df_temp_responses.iloc[0, df_temp_responses.columns.get_loc('SI14')]
        SI15 = df_temp_responses.iloc[0, df_temp_responses.columns.get_loc('SI15')]

        if (((SI11 == "$40,000") | (SI11 == "Don't know")) & ((SI12 == "Still picks option 1")|(SI12 == "Don't know"))):
            riskB = -1
        elif (SI11 == "$40,000"):
            if (SI13 == "$40,000"):
                if (SI14 == "$40,000"):
                    riskB = 4
                elif (SI14 == "$80,000 or $20,000"):
                    riskB = 3
            elif (SI13 == "$120,000 or $0"):
                if (SI15 == "$40,000"):
                    riskB = 2
                elif (SI15 == "$160,000 or -$20,000"):
                    riskB = 1

        print("user {0}'s riskB = {1}".format(current_userID, riskB))

        sql = "INSERT INTO user_risk_preference (userID,riskA,riskB) VALUES (" + str(current_userID) + "," + str(riskA) + "," + str(riskB) + ");"
        print(sql)
        cursor.execute(sql)

    server.stop()

    print("END")

