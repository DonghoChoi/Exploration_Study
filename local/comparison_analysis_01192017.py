#!/usr/bin/python
# Author: Dongho Choi

import os.path
import datetime
import sys
import itertools
import pandas as pd
import numpy as np
import scipy.stats as stats
from sshtunnel import SSHTunnelForwarder # for SSH connection
import pymysql.cursors # MySQL handling API
import random
import sys
sys.path.append("./configs/")
sys.path.append("/Users/donghochoi/Documents/Work/Exploration_Study/Dissertation/Code/local/configs/")
import server_config # (1) info2_server (2) exploration_db
import matplotlib.pyplot as plt
from pylab import plot, show, savefig, xlim, figure, hold, ylim, legend, boxplot, setp, axes

def setBoxColors(bp):
    setp(bp['boxes'][0], color='blue')
    setp(bp['caps'][0], color='blue')
    setp(bp['caps'][1], color='blue')
    setp(bp['whiskers'][0], color='blue')
    setp(bp['whiskers'][1], color='blue')
    setp(bp['fliers'][0], color='blue')
    setp(bp['fliers'][1], color='blue')
    setp(bp['medians'][0], color='blue')

    setp(bp['boxes'][1], color='red')
    setp(bp['caps'][2], color='red')
    setp(bp['caps'][3], color='red')
    setp(bp['whiskers'][2], color='red')
    setp(bp['whiskers'][3], color='red')
    setp(bp['fliers'][2], color='red')
 #   setp(bp['fliers'][3], color='yellow')
    setp(bp['medians'][1], color='red')

    setp(bp['boxes'][2], color='green')
    setp(bp['caps'][4], color='green')
    setp(bp['caps'][5], color='green')
    setp(bp['whiskers'][4], color='green')
    setp(bp['whiskers'][5], color='green')
  #  setp(bp['fliers'][4], color='green')
  #  setp(bp['fliers'][5], color='green')
    setp(bp['medians'][2], color='green')

def f_pairset(x):
    return set(x['user_a'],x['user_b'])

def get_statistics(df):
    baseline_coverage_all_pairs = df['Coverage'].mean()
    print(baseline_coverage_all_pairs)
    df['Coverage'].plot.hist()

    baseline_uniquecoverage_all_pairs = df['UniqueCoverage'].mean()
    print(baseline_uniquecoverage_all_pairs)
    df['UniqueCoverage'].plot.hist()

    baseline_usefulcoverage_all_pairs = df['UsefulCoverage'].mean()
    print(baseline_usefulcoverage_all_pairs)
    df['UsefulCoverage'].plot.hist()

    baseline_uniqueusefulcoverage_all_pairs = df['UniqueUsefulCoverage'].mean()
    print(baseline_uniqueusefulcoverage_all_pairs)
    df['UniqueUsefulCoverage'].plot.hist()

def get_random_results_histogram(df):
    coverage_all_pairs = df['Cov_mean'].mean()
    print(coverage_all_pairs)
    df['Cov_mean'].plot.hist()

    uniquecoverage_all_pairs = df['UniCov_mean'].mean()
    print(uniquecoverage_all_pairs)
    df['UniCov_mean'].plot.hist()

    usefulcoverage_all_pairs = df['UseCov_mean'].mean()
    print(usefulcoverage_all_pairs)
    df['UseCov_mean'].plot.hist()

    uniqueusefulcoverage_all_pairs = df['UniUseCov_mean'].mean()
    print(uniqueusefulcoverage_all_pairs)
    df['UniUseCov_mean'].plot.hist()

if __name__ == "__main__":

    # READ DATA FROM SERVER
    # read_Data_from_Server()
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

    # Get individual data
    df_individual_data = pd.read_sql('SELECT * FROM individual_data', con=connection)
    print("Individual data READ")

    # Get dyad data
    df_dyad_data = pd.read_sql('SELECT * FROM dyad_data', con=connection)
    print("Dyad data READ")

    df_dyad_data["user_set"] = ""
    for i in range(0, len(df_dyad_data)):
        user_a = df_dyad_data.iloc[i]['user_a']
        user_b = df_dyad_data.iloc[i]['user_b']
        user_set = set([user_a, user_b])
        df_dyad_data.set_value(i, 'user_set', user_set)

    # Get mobility data
    df_mobility_data = pd.read_sql('SELECT * FROM mobility_data WHERE userID!=23', con=connection)
    print("Mobility data READ")

    server.stop()

    df_random_results = pd.DataFrame(columns=('trial_No','Cov_mean','UniCov_mean','UseCov_mean','UniUseCov_mean'))
    for i in range(0,100):
        list_participants = df_mobility_data['userID'].tolist()
        list_pairs = []

        while len(list_participants) > 1:
            # Using the randomly created indices, respective elements are popped out
            r1 = random.randrange(0, len(list_participants))
            elem1 = list_participants.pop(r1)

            r2 = random.randrange(0, len(list_participants))
            elem2 = list_participants.pop(r2)

            # now the selecetd elements are paired in a dictionary
            list_pairs.append((elem1, elem2))

        df_random_dyad = pd.DataFrame()
        for j in range(0,len(list_pairs)):
            user_a = list_pairs[j][0]
            user_b = list_pairs[j][1]
            df_temp_a = df_dyad_data[(df_dyad_data['user_a']==user_a) & (df_dyad_data['user_b']==user_b)]
            if len(df_temp_a)==0:
                df_temp_b = df_dyad_data[(df_dyad_data['user_a']==user_b) & (df_dyad_data['user_b']==user_a)]
                df_random_dyad = df_random_dyad.append(df_temp_b)
            else:
                df_random_dyad = df_random_dyad.append(df_temp_a)
        Cov_mean = df_random_dyad['Coverage'].mean()
        UniCov_mean = df_random_dyad['UniqueCoverage'].mean()
        UseCov_mean = df_random_dyad['UsefulCoverage'].mean()
        UniUseCov_mean = df_random_dyad['UniqueUsefulCoverage'].mean()
        df_temp = pd.DataFrame([[i,Cov_mean,UniCov_mean,UseCov_mean,UniUseCov_mean]], columns=('trial_No','Cov_mean','UniCov_mean','UseCov_mean','UniUseCov_mean'))
        df_random_results = df_random_results.append(df_temp)

    df_random_results_1000 = pd.DataFrame(columns=('trial_No','Cov_mean','UniCov_mean','UseCov_mean','UniUseCov_mean'))
    for i in range(0,1000):
        list_participants = df_mobility_data['userID'].tolist()
        list_pairs = []

        while len(list_participants) > 0:
            # Using the randomly created indices, respective elements are popped out
            r1 = random.randrange(0, len(list_participants))
            elem1 = list_participants.pop(r1)

            r2 = random.randrange(0, len(list_participants))
            elem2 = list_participants.pop(r2)

            # now the selecetd elements are paired in a dictionary
            list_pairs.append((elem1, elem2))

        df_random_dyad = pd.DataFrame()
        for j in range(0,len(list_pairs)):
            user_a = list_pairs[j][0]
            user_b = list_pairs[j][1]
            df_temp_a = df_dyad_data[(df_dyad_data['user_a']==user_a) & (df_dyad_data['user_b']==user_b)]
            if len(df_temp_a)==0:
                df_temp_b = df_dyad_data[(df_dyad_data['user_a']==user_b) & (df_dyad_data['user_b']==user_a)]
                df_random_dyad = df_random_dyad.append(df_temp_b)
            else:
                df_random_dyad = df_random_dyad.append(df_temp_a)
        Cov_mean = df_random_dyad['Coverage'].mean()
        UniCov_mean = df_random_dyad['UniqueCoverage'].mean()
        UseCov_mean = df_random_dyad['UsefulCoverage'].mean()
        UniUseCov_mean = df_random_dyad['UniqueUsefulCoverage'].mean()
        df_temp = pd.DataFrame([[i,Cov_mean,UniCov_mean,UseCov_mean,UniUseCov_mean]], columns=('trial_No','Cov_mean','UniCov_mean','UseCov_mean','UniUseCov_mean'))
        df_random_results_1000 = df_random_results_1000.append(df_temp)

    # Get statistics
    get_random_results_histogram(df_random_results)

    df_mobility_sorted = df_mobility_data.sort_values(by='s_k')

    # Scenario 1. Similar pairs
    list_scenario_1 = df_mobility_sorted['userID'].tolist()
    list_pairs_scenario_1 = []

    while len(list_scenario_1) > 0:
        elem1 = list_scenario_1.pop(0)
        elem2 = list_scenario_1.pop(0)
        list_pairs_scenario_1.append((elem1, elem2))

    df_dyad_scenario_1 = pd.DataFrame()
    for i in range(0,len(list_pairs_scenario_1)):
        user_a = list_pairs_scenario_1[i][0]
        user_b = list_pairs_scenario_1[i][1]
        df_temp_a = df_dyad_data[(df_dyad_data['user_a']==user_a) & (df_dyad_data['user_b']==user_b)]
        if len(df_temp_a)==0:
            df_temp_b = df_dyad_data[(df_dyad_data['user_a']==user_b) & (df_dyad_data['user_b']==user_a)]
            df_dyad_scenario_1 = df_dyad_scenario_1.append(df_temp_b)
        else:
            df_dyad_scenario_1 = df_dyad_scenario_1.append(df_temp_a)

    # Scenario 2. Opposite pairs
    list_scenario_2 = df_mobility_sorted['userID'].tolist()
    list_pairs_scenario_2 = []

    while len(list_scenario_2) > 0:
        elem1 = list_scenario_2.pop(0)
        elem2 = list_scenario_2.pop(len(list_scenario_2)//2)
        list_pairs_scenario_2.append((elem1, elem2))

    df_dyad_scenario_2 = pd.DataFrame()
    for i in range(0,len(list_pairs_scenario_2)):
        user_a = list_pairs_scenario_2[i][0]
        user_b = list_pairs_scenario_2[i][1]
        df_temp_a = df_dyad_data[(df_dyad_data['user_a']==user_a) & (df_dyad_data['user_b']==user_b)]
        if len(df_temp_a)==0:
            df_temp_b = df_dyad_data[(df_dyad_data['user_a']==user_b) & (df_dyad_data['user_b']==user_a)]
            df_dyad_scenario_2 = df_dyad_scenario_2.append(df_temp_b)
        else:
            df_dyad_scenario_2 = df_dyad_scenario_2.append(df_temp_a)


    print("##### END #####")