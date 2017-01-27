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

    # Get Williams responses data
    df_Williams_responses = pd.read_sql('SELECT * FROM user_Williams_responses', con=connection)
    print("Williams responses data READ")

    for i in range(0,len(df_Williams_responses)):
        df_temp = df_Williams_responses.iloc[i]
        userID = df_temp['userID']
        bond_scale = (df_temp['Bond_1']+df_temp['Bond_2']+(1-df_temp['Bond_3'])+df_temp['Bond_4']+df_temp['Bond_5']+df_temp['Bond_6']+df_temp['Bond_7']+df_temp['Bond_8']+(1-df_temp['Bond_9'])+df_temp['Bond_10'])/10
        bridge_scale = (df_temp['Bridge_1']+df_temp['Bridge_2']+df_temp['Bridge_3']+df_temp['Bond_4']+df_temp['Bond_5']+df_temp['Bond_6']+df_temp['Bond_7']+df_temp['Bond_8']+df_temp['Bond_9']+df_temp['Bond_10'])/10
        sql = "INSERT INTO user_Williams (userID,Bonding_scale,Bridging_scale) VALUES (" + str(userID) + "," + str(bond_scale) + "," + str(bridge_scale) + ");"
        print(sql)
        cursor.execute(sql)

    server.stop()

    print("END")