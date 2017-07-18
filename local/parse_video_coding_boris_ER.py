#!/usr/bin/python
# Author: Dongho Choi

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

    # Read a video code data file
    data_directory = "/Users/donghochoi/Documents/Work/Exploration_Study/Dissertation/Video_Analysis/ER/boris_export/1stRound/"
    print("PARSING BORIS CODING DATA")
    userID_list = {2,7,10,11,14,21,22,27,28,37}
    for userID in userID_list:
        input_file_name = data_directory+ "user" + str(userID) + ".txt.tsv"
        with open(input_file_name) as f:
            tsvreader = csv.reader(f, delimiter="\t")
            for _ in range(16):
                next(tsvreader)
            for line in tsvreader:
                print(line)
                #currentline = line.split("\t")
                #print("time:{0}, behavior: {1}, label: {2}, status: {3}".format(currentline[0],currentline[5],currentline[6],currentline[8]))
                print("time:{0}, behavior: {1}, label: {2}, status: {3}".format(line[0],line[5],line[6],line[8]))

                sql = "INSERT INTO user_ER_Boris_results_1_round (userID,observed_time,behavior,label,status) VALUES (" + \
                      str(userID) + "," + str(line[0]) + ",'" + str(line[5]) + "','" + str(line[6]) + "','" + str(line[8]) +"');"
                print(sql)
                cursor.execute(sql)
        f.close()

    server.stop()