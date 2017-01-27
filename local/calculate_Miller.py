#!/usr/bin/python
# Author: Dongho Choi

import math
import time
import pandas as pd
import numpy as np
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

    # Get personality responses data
    df_Miller_reponses = pd.read_sql('SELECT * FROM user_Miller_responses', con=connection)
    print("Miller responses data READ")

    for i in range(0,len(df_Miller_reponses)):
        df_temp = df_Miller_reponses.iloc[i]
        userID = df_temp['userID']
        m_array = np.zeros(16)
        b_array = np.zeros(16)

        q1 = df_temp['Q1']
        if 'I would ask the dentist exactly' in q1:
            m_array[0] = 1
        if 'I would take a tranquiliser' in q1:
            b_array[0] = 1
        if 'I would try to think about pleasant memories' in q1:
            b_array[1] = 1
        if 'I would want the dentist to tell' in q1:
            m_array[1] = 1
        if 'I would try to sleep' in q1:
            b_array[2] = 1
        if 'I would watch all the' in q1:
            m_array[2] = 1
        if 'I would watch the flow' in q1:
            m_array[3] = 1
        if 'I would do mental' in q1:
            b_array[3] = 1

        q2 = df_temp['Q2']
        if 'I would sit by' in q2:
            b_array[4] = 1
        if 'I would stay alert' in q2:
            m_array[4] = 1
        if 'I would exchange' in q2:
            b_array[5] = 1
        if 'If there was' in q2:
            m_array[5] = 1
        if 'I would watch every' in q2:
            m_array[6] = 1
        if 'I would try to' in q2:
            b_array[6] = 1
        if 'I would think about' in q2:
            b_array[7] = 1
        if 'I would make' in q2:
            m_array[7] = 1

        q3 = df_temp['Q3']
        if 'I would talk to' in q3:
            m_array[8] = 1
        if 'I would review' in q3:
            m_array[9] = 1
        if 'I would go to' in q3:
            b_array[8] = 1
        if 'I would try to remember' in q3:
            m_array[10] = 1
        if 'I would push' in q3:
            b_array[9] = 1
        if 'I would tell' in q3:
            b_array[10] = 1
        if 'I would try to think' in q3:
            m_array[11] = 1
        if 'I would continue' in q3:
            b_array[11] = 1

        q4 = df_temp['Q4']
        if 'I would carefully' in q4:
            m_array[12] = 1
        if 'I would make' in q4:
            b_array[12] = 1
        if 'I would watch' in q4:
            b_array[13] = 1
        if 'I would call' in q4:
            m_array[13] = 1
        if 'I would order' in q4:
            b_array[14] = 1
        if 'I would listen' in q4:
            m_array[14] = 1
        if 'I would talk to' in q4:
            m_array[15] = 1
        if 'I would settle' in q4:
            b_array[15] = 1

        print("userID = {0}, m_array = {1}, b_array = {2}".format(userID,m_array,b_array))
        m_score = int(m_array.sum())
        b_score = int(b_array.sum())
        miller_score = m_score - b_score
        print("m:{0}, b:{1}, s:{2}".format(m_score,b_score,miller_score))

        sql = "INSERT INTO user_Miller (userID,M_score,B_score,Miller_score) VALUES (" + str(
            userID) + "," + str(m_score) + "," + str(b_score) + "," + str(miller_score) + ");"
        print(sql)
        cursor.execute(sql)


    server.stop()

    print("END")