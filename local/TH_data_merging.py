#!/usr/bin/python
# Author: Dongho Choi
'''
This script
(1) import coding data from BORIS, which is located in the table named 'user_TH_Boris_results'
(2) calculate the time spent on each information patch
(3) import coding data from vCode, which is located in the table named 'user_TH_vCode_results'
(4) since there are some changes/differences in vCode data format (e.g., time unit is millisecond, and labels for the location and information patch were updated after coding of user7 and user8's data), these changes should be considered in this process

'''

import os.path
import datetime
import math
import time
import itertools
import pandas as pd
from sshtunnel import SSHTunnelForwarder # for SSH connection
import pymysql.cursors # MySQL handling API
import sys
import csv
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

    # import Boris coding data
    df_boris_data = pd.read_sql('SELECT * FROM user_TH_Boris_results', con=connection)
    print("Boris data imported.")

    # import vCode coding data
    df_vcode_data = pd.read_sql('SELECT * FROM user_TH_vCode_results', con=connection)
    print("vCode data imported.")

    # 

    server.stop()