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
    '''
    server = SSHTunnelForwarder(
        (server_config.info2_server['host'], 22),
        ssh_username=server_config.info2_server['user'],
        ssh_password=server_config.info2_server['password'],
        remote_bind_address=('127.0.0.1', 3306))
    '''
    server = SSHTunnelForwarder(
        (server_config.info1_server['host'], 22),
        ssh_username=server_config.info1_server['user'],
        ssh_password=server_config.info1_server['password'],
        remote_bind_address=('127.0.0.1', 3306))

    server.start()
    '''
    connection = pymysql.connect(host='127.0.0.1',
                                 port=server.local_bind_port,
                                 user=server_config.exploration_db['user'],
                                 password=server_config.exploration_db['password'],
                                 db=server_config.exploration_db['database'])
    '''
    connection = pymysql.connect(host='127.0.0.1',
                                 port=server.local_bind_port,
                                 user=server_config.sensorstudy_db['user'],
                                 password=server_config.sensorstudy_db['password'],
                                 db=server_config.sensorstudy_db['database'])

    connection.autocommit(True)
    cursor = connection.cursor()
    print("MySQL connection established.")

    # Get personality responses data
    df_personality_reponses = pd.read_sql('SELECT * FROM user_personality_responses', con=connection)
    print("Personality responses data READ")

    for i in range(0,len(df_personality_reponses)):
        df_temp = df_personality_reponses.iloc[i]
        userID = df_temp['userID']
        Extraversion = (df_temp['Q1']+(6-df_temp['Q6'])+df_temp['Q11']+df_temp['Q16']+(6-df_temp['Q21'])+df_temp['Q26']+(6-df_temp['Q31'])+df_temp['Q36'])/8
        Agreeableness = ((6-df_temp['Q2'])+df_temp['Q7']+(6-df_temp['Q12'])+df_temp['Q17']+df_temp['Q22']+(6-df_temp['Q27'])+df_temp['Q32']+(6-df_temp['Q37'])+df_temp['Q42'])/9
        Conscientiousness = (df_temp['Q3']+(6-df_temp['Q8'])+df_temp['Q13']+(6-df_temp['Q18'])+(6-df_temp['Q23'])+df_temp['Q28']+df_temp['Q33']+df_temp['Q38']+(6-df_temp['Q43']))/9
        Neuroticism = (df_temp['Q4']+(6-df_temp['Q9'])+df_temp['Q14']+df_temp['Q19']+(6-df_temp['Q24'])+df_temp['Q29']+(6-df_temp['Q34'])+df_temp['Q39'])/8
        Openness = (df_temp['Q5']+df_temp['Q10']+df_temp['Q15']+df_temp['Q20']+df_temp['Q25']+df_temp['Q30']+(6-df_temp['Q35'])+df_temp['Q40']+(6-df_temp['Q41'])+df_temp['Q44'])/10
        sql = "INSERT INTO user_personality (userID,Extraversion,Agreeableness,Conscientiousness,Neuroticism,Openness) VALUES (" + str(
            userID) + "," + str(Extraversion) + "," + str(Agreeableness) + "," + str(Conscientiousness) + "," + str(Neuroticism) + "," + str(Openness) + ");"
        print(sql)
        cursor.execute(sql)


    server.stop()

    print("END")