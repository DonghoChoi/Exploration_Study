#!/usr/bin/python
# Author: Dongho Choi
'''
This script
(1) read multiple tables from data base
(2) join the tables into one for analysis

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
import matplotlib.pyplot as plt
import numpy as np
from sklearn import datasets, linear_model
import statsmodels.api as sm
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
    print("MySQL connection established")

    # Average Coverage and UsefulCoverage when Coverage is greater than zero coding data
    df_coverage_with_zero_field = pd.read_sql('SELECT userID,count(Coverage) as count_session_with_zero, avg(Coverage) as avg_cov_with_zero,avg(UsefulCoverage) as avg_usecov_with_zero FROM user_field_session_coverage WHERE Coverage >= 0 group by userID', con=connection)
    print("Coverage_with_zero data imported.")

    # Average Coverage and UsefulCoverage when Coverage is greater than zero coding data
    df_coverage_gt_zero_field = pd.read_sql('SELECT userID,count(Coverage) as count_session_gt_zero, avg(Coverage) as avg_cov_gt_zero,avg(UsefulCoverage) as avg_usecov_gt_zero FROM user_field_session_coverage WHERE Coverage > 0 group by userID', con=connection)
    print("Coverage_gt_zero data imported.")

    # Average Use_ratio when Coverage being greater than zero
    df_useratio_field = pd.read_sql('SELECT userID,avg(Use_ratio) as avg_useratio FROM user_field_session_coverage WHERE Coverage > 0 group by userID', con=connection)
    print("Useratio data imported.")

    # Online diversity in field session
    df_online_diversity_field = pd.read_sql('SELECT userID, online_diversity as online_diversity_field, online_loyalty as online_loyalty_field FROM user_online_diversity', con=connection)
    print("Online diversity data imported.")

    # Online performance in lab task 2
    df_lab_performance = pd.read_sql('SELECT userID,Coverage as Cov_task2,UniqueCoverage as UniCov_task2,UsefulCoverage as UseCov_task2, UniqueUsefulCoverage as UniUseCov_task2 FROM individual_data', con=connection)
    print("Individual data imported.")

    # Geo-exploration: S_k measure
    df_geo_s_k = pd.read_sql('SELECT userID,gyration_all,gyration_k,s_k FROM mobility_data', con=connection)
    print("s_k measure imported")

    # Geo-exploration diversity in field session
    df_location_diversity = pd.read_sql('SELECT userID,location_diversity,location_loyalty FROM user_location_diversity', con=connection)
    print("Location diversity imported")

    #server.stop()

    # Joining multiple dataframes
    df_join = pd.merge(df_coverage_with_zero_field, df_coverage_gt_zero_field, on='userID', how='inner')
    df_join = pd.merge(df_join, df_useratio_field, on='userID', how='inner')
    df_join = pd.merge(df_join, df_online_diversity_field, on='userID', how='inner')
    df_join = pd.merge(df_join, df_lab_performance, on='userID', how='inner')
    df_join = pd.merge(df_join, df_geo_s_k, on='userID', how='inner')
    df_join = pd.merge(df_join, df_location_diversity, on='userID', how='inner')
    print(df_join)
    print(df_join.corr(method='pearson'))


