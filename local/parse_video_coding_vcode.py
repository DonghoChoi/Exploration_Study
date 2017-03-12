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

    # Read a video code data file
    data_directory = "/Users/donghochoi/Documents/Work/Exploration_Study/Dissertation/Video_Analysis/TH/vcode_export/"
    print("PARSING VCODE CODING DATA")
    userID_list = {7,8}
    for userID in userID_list:
        input_file_name = data_directory+ "user" + str(userID) + ".txt"
        with open(input_file_name) as f:
            for _ in range(4):
                next(f)
            for line in f:
                print(line)
                currentline = line.split(",")
                print("time:{0}, duration: {1}, label: {2}".format(currentline[0],currentline[1],currentline[2]))

                sql = "INSERT INTO user_TH_vCode_results (userID,observed_time,duration,label) VALUES (" + str(userID) + "," + str(currentline[0]) + "," + str(currentline[1]) + ",'" + str(currentline[2]) + "');"
                print(sql)
                cursor.execute(sql)

    server.stop()