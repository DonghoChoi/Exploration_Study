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

'''
This file
(1) reads user's location diversity and loyalty data
(2) matches participants based on the diversity and loyalty considering two scenarios - scenario 1 (similar individuals) and scenario 2 (different people)
(3) basesline data is from scenario 0 - random pairs - with 10,000 trials

'''

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

    # Get location diversity data
    df_location_diversity_data = pd.read_sql('SELECT userID,total_locations,location_diversity,location_loyalty FROM user_location_diversity', con=connection)
    print("Location diversity data READ")

    # Get mobility data
    df_mobility_data = pd.read_sql('SELECT * FROM mobility_data WHERE userID!=23', con=connection)
    print("Mobility data READ")

    server.stop()

    # Scenario 0. random pairs, 1000 trials, to get the average values for each measure
    df_random_results_1000 = pd.DataFrame(columns=('trial_No','Cov_mean','UniCov_mean','UseCov_mean','UniUseCov_mean'))
    for i in range(0,1000):
        list_participants = df_location_diversity_data['userID'].tolist()
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

    # visualize the results of random trials
    #get_random_results_histogram(df_random_results_1000)


    fig, axes = plt.subplots(nrows=2, ncols=2)
    ax0, ax1, ax2, ax3 = axes.flatten()
    n_bins = 20

    ax0.hist(df_random_results_1000['Cov_mean'], n_bins , normed=1, histtype='bar')
    ax0.set_title("Mean of Coverage")

    ax1.hist(df_random_results_1000['UniCov_mean'], n_bins, normed=1, histtype='bar')
    ax1.set_title("Mean of Unique Coverage")

    ax2.hist(df_random_results_1000['UseCov_mean'], n_bins, normed=1, histtype='bar')
    ax2.set_title("Mean of Useful Coverage")

    ax3.hist(df_random_results_1000['UniUseCov_mean'], n_bins, normed=1, histtype='bar')
    ax3.set_title("Mean of Unique Useful Coverage")

    fig.tight_layout()
    #plt.show()
    fig.savefig('random_trials_distribution.png')

    df_location_sorted_by_loyalty = df_location_diversity_data.sort_values(by='location_loyalty')
    print("User list sorted by location loyalty")
    print(df_location_sorted_by_loyalty)


    # Scenario 1. Similar pairs based on location loyalty
    list_scenario_1 = df_location_sorted_by_loyalty['userID'].tolist()
    list_pairs_scenario_1 = []

    while len(list_scenario_1) > 0:
        elem1 = list_scenario_1.pop(0)
        elem2 = list_scenario_1.pop(0)
        list_pairs_scenario_1.append((elem1, elem2))
    print("Scenario 1 pairs list:")
    print(list_pairs_scenario_1)

    df_dyad_scenario_1 = pd.DataFrame()
    for i in range(0, len(list_pairs_scenario_1)):
        user_a = list_pairs_scenario_1[i][0]
        user_b = list_pairs_scenario_1[i][1]
        df_temp_a = df_dyad_data[(df_dyad_data['user_a'] == user_a) & (df_dyad_data['user_b'] == user_b)]
        if len(df_temp_a) == 0:
            df_temp_b = df_dyad_data[(df_dyad_data['user_a'] == user_b) & (df_dyad_data['user_b'] == user_a)]
            df_dyad_scenario_1 = df_dyad_scenario_1.append(df_temp_b)
        else:
            df_dyad_scenario_1 = df_dyad_scenario_1.append(df_temp_a)
    scenario_1_Coverage_mean = df_dyad_scenario_1['Coverage'].mean()
    scenario_1_UniqueCoverage_mean = df_dyad_scenario_1['UniqueCoverage'].mean()
    scenario_1_UsefulCoverage_mean = df_dyad_scenario_1['UsefulCoverage'].mean()
    scenario_1_UniqueUsefulCoverage_mean = df_dyad_scenario_1['UniqueUsefulCoverage'].mean()
    print("Scenario 1 - loyalty: Mean of Coverage - {0}, Unique Coverage - {1}, Useful Coverage - {2}, Unique Useful Coverage - {3}".\
          format(scenario_1_Coverage_mean,scenario_1_UniqueCoverage_mean,scenario_1_UsefulCoverage_mean,scenario_1_UniqueUsefulCoverage_mean))
    print("Coverage is top {0}".format(len(df_random_results_1000.loc[df_random_results_1000['Cov_mean']>scenario_1_Coverage_mean])))
    print("UniqueCoverage is top {0}".format(len(df_random_results_1000.loc[df_random_results_1000['UniCov_mean']>scenario_1_UniqueCoverage_mean])))
    print("UsefulCoverage is top {0}".format(len(df_random_results_1000.loc[df_random_results_1000['UseCov_mean']>scenario_1_UsefulCoverage_mean])))
    print("UniqueUsefulCoverage is top {0}".format(len(df_random_results_1000.loc[df_random_results_1000['UniUseCov_mean']>scenario_1_UniqueUsefulCoverage_mean])))


    # Scenario 2.
    list_scenario_2 = df_location_sorted_by_loyalty['userID'].tolist()
    list_pairs_scenario_2 = []

    while len(list_scenario_2) > 0:
        elem1 = list_scenario_2.pop(0)
        elem2 = list_scenario_2.pop(len(list_scenario_2)//2)
        list_pairs_scenario_2.append((elem1, elem2))
    print("Scenario 2 pairs list:")
    print(list_pairs_scenario_2)

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
    scenario_2_Coverage_mean = df_dyad_scenario_2['Coverage'].mean()
    scenario_2_UniqueCoverage_mean = df_dyad_scenario_2['UniqueCoverage'].mean()
    scenario_2_UsefulCoverage_mean = df_dyad_scenario_2['UsefulCoverage'].mean()
    scenario_2_UniqueUsefulCoverage_mean = df_dyad_scenario_2['UniqueUsefulCoverage'].mean()
    print("Scenario 2 - loyalty: Mean of Coverage - {0}, Unique Coverage - {1}, Useful Coverage - {2}, Unique Useful Coverage - {3}".\
          format(scenario_2_Coverage_mean,scenario_2_UniqueCoverage_mean,scenario_2_UsefulCoverage_mean,scenario_2_UniqueUsefulCoverage_mean))
    print("Coverage is top {0}".format(len(df_random_results_1000.loc[df_random_results_1000['Cov_mean']>scenario_2_Coverage_mean])))
    print("UniqueCoverage is top {0}".format(len(df_random_results_1000.loc[df_random_results_1000['UniCov_mean']>scenario_2_UniqueCoverage_mean])))
    print("UsefulCoverage is top {0}".format(len(df_random_results_1000.loc[df_random_results_1000['UseCov_mean']>scenario_2_UsefulCoverage_mean])))
    print("UniqueUsefulCoverage is top {0}".format(len(df_random_results_1000.loc[df_random_results_1000['UniUseCov_mean']>scenario_2_UniqueUsefulCoverage_mean])))

    df_location_sorted_by_diversity = df_location_diversity_data.sort_values(by='location_diversity')
    print("User list sorted by location diversity")
    print(df_location_sorted_by_diversity)

    # Scenario 3. Similar pairs based on location diversity
    list_scenario_3 = df_location_sorted_by_diversity['userID'].tolist()
    list_pairs_scenario_3 = []

    while len(list_scenario_3) > 0:
        elem1 = list_scenario_3.pop(0)
        elem2 = list_scenario_3.pop(0)
        list_pairs_scenario_3.append((elem1, elem2))
    print("Scenario 3 pairs list:")
    print(list_pairs_scenario_3)

    df_dyad_scenario_3 = pd.DataFrame()
    for i in range(0, len(list_pairs_scenario_3)):
        user_a = list_pairs_scenario_3[i][0]
        user_b = list_pairs_scenario_3[i][1]
        df_temp_a = df_dyad_data[(df_dyad_data['user_a'] == user_a) & (df_dyad_data['user_b'] == user_b)]
        if len(df_temp_a) == 0:
            df_temp_b = df_dyad_data[(df_dyad_data['user_a'] == user_b) & (df_dyad_data['user_b'] == user_a)]
            df_dyad_scenario_3 = df_dyad_scenario_3.append(df_temp_b)
        else:
            df_dyad_scenario_3 = df_dyad_scenario_3.append(df_temp_a)
    scenario_3_Coverage_mean = df_dyad_scenario_3['Coverage'].mean()
    scenario_3_UniqueCoverage_mean = df_dyad_scenario_3['UniqueCoverage'].mean()
    scenario_3_UsefulCoverage_mean = df_dyad_scenario_3['UsefulCoverage'].mean()
    scenario_3_UniqueUsefulCoverage_mean = df_dyad_scenario_3['UniqueUsefulCoverage'].mean()
    print("Scenario 3 - diversity: Mean of Coverage - {0}, Unique Coverage - {1}, Useful Coverage - {2}, Unique Useful Coverage - {3}".\
          format(scenario_3_Coverage_mean,scenario_3_UniqueCoverage_mean,scenario_3_UsefulCoverage_mean,scenario_3_UniqueUsefulCoverage_mean))
    print("Coverage is top {0}".format(len(df_random_results_1000.loc[df_random_results_1000['Cov_mean']>scenario_3_Coverage_mean])))
    print("UniqueCoverage is top {0}".format(len(df_random_results_1000.loc[df_random_results_1000['UniCov_mean']>scenario_3_UniqueCoverage_mean])))
    print("UsefulCoverage is top {0}".format(len(df_random_results_1000.loc[df_random_results_1000['UseCov_mean']>scenario_3_UsefulCoverage_mean])))
    print("UniqueUsefulCoverage is top {0}".format(len(df_random_results_1000.loc[df_random_results_1000['UniUseCov_mean']>scenario_3_UniqueUsefulCoverage_mean])))


    # Scenario 4. Different pairs based on location diversity
    list_scenario_4 = df_location_sorted_by_diversity['userID'].tolist()
    list_pairs_scenario_4 = []

    while len(list_scenario_4) > 0:
        elem1 = list_scenario_4.pop(0)
        elem2 = list_scenario_4.pop(len(list_scenario_4)//2)
        list_pairs_scenario_4.append((elem1, elem2))
    print("Scenario 4 pairs list:")
    print(list_pairs_scenario_4)

    df_dyad_scenario_4 = pd.DataFrame()
    for i in range(0,len(list_pairs_scenario_4)):
        user_a = list_pairs_scenario_4[i][0]
        user_b = list_pairs_scenario_4[i][1]
        df_temp_a = df_dyad_data[(df_dyad_data['user_a']==user_a) & (df_dyad_data['user_b']==user_b)]
        if len(df_temp_a)==0:
            df_temp_b = df_dyad_data[(df_dyad_data['user_a']==user_b) & (df_dyad_data['user_b']==user_a)]
            df_dyad_scenario_4 = df_dyad_scenario_4.append(df_temp_b)
        else:
            df_dyad_scenario_4 = df_dyad_scenario_4.append(df_temp_a)
    scenario_4_Coverage_mean = df_dyad_scenario_4['Coverage'].mean()
    scenario_4_UniqueCoverage_mean = df_dyad_scenario_4['UniqueCoverage'].mean()
    scenario_4_UsefulCoverage_mean = df_dyad_scenario_4['UsefulCoverage'].mean()
    scenario_4_UniqueUsefulCoverage_mean = df_dyad_scenario_4['UniqueUsefulCoverage'].mean()
    print("Scenario 4 - diversity: Mean of Coverage - {0}, Unique Coverage - {1}, Useful Coverage - {2}, Unique Useful Coverage - {3}".\
          format(scenario_4_Coverage_mean,scenario_4_UniqueCoverage_mean,scenario_4_UsefulCoverage_mean,scenario_4_UniqueUsefulCoverage_mean))
    print("Coverage is top {0}".format(len(df_random_results_1000.loc[df_random_results_1000['Cov_mean']>scenario_4_Coverage_mean])))
    print("UniqueCoverage is top {0}".format(len(df_random_results_1000.loc[df_random_results_1000['UniCov_mean']>scenario_4_UniqueCoverage_mean])))
    print("UsefulCoverage is top {0}".format(len(df_random_results_1000.loc[df_random_results_1000['UseCov_mean']>scenario_4_UsefulCoverage_mean])))
    print("UniqueUsefulCoverage is top {0}".format(len(df_random_results_1000.loc[df_random_results_1000['UniUseCov_mean']>scenario_4_UniqueUsefulCoverage_mean])))

    # Scenario 5. Similar pairs based on s_k ratio
    df_mobility_sorted = df_mobility_data.sort_values(by='s_k')
    list_scenario_5 = df_mobility_sorted['userID'].tolist()
    list_pairs_scenario_5 = []

    while len(list_scenario_5) > 0:
        elem1 = list_scenario_5.pop(0)
        elem2 = list_scenario_5.pop(0)
        list_pairs_scenario_5.append((elem1, elem2))

    df_dyad_scenario_5 = pd.DataFrame()
    for i in range(0,len(list_pairs_scenario_5)):
        user_a = list_pairs_scenario_5[i][0]
        user_b = list_pairs_scenario_5[i][1]
        df_temp_a = df_dyad_data[(df_dyad_data['user_a']==user_a) & (df_dyad_data['user_b']==user_b)]
        if len(df_temp_a)==0:
            df_temp_b = df_dyad_data[(df_dyad_data['user_a']==user_b) & (df_dyad_data['user_b']==user_a)]
            df_dyad_scenario_5 = df_dyad_scenario_5.append(df_temp_b)
        else:
            df_dyad_scenario_5 = df_dyad_scenario_5.append(df_temp_a)
    scenario_5_Coverage_mean = df_dyad_scenario_5['Coverage'].mean()
    scenario_5_UniqueCoverage_mean = df_dyad_scenario_5['UniqueCoverage'].mean()
    scenario_5_UsefulCoverage_mean = df_dyad_scenario_5['UsefulCoverage'].mean()
    scenario_5_UniqueUsefulCoverage_mean = df_dyad_scenario_5['UniqueUsefulCoverage'].mean()
    print("Scenario 5 - s_k: Mean of Coverage - {0}, Unique Coverage - {1}, Useful Coverage - {2}, Unique Useful Coverage - {3}". \
        format(scenario_5_Coverage_mean, scenario_5_UniqueCoverage_mean, scenario_5_UsefulCoverage_mean,scenario_5_UniqueUsefulCoverage_mean))
    print("Coverage is top {0}".format(len(df_random_results_1000.loc[df_random_results_1000['Cov_mean'] > scenario_5_Coverage_mean])))
    print("UniqueCoverage is top {0}".format(len(df_random_results_1000.loc[df_random_results_1000['UniCov_mean'] > scenario_5_UniqueCoverage_mean])))
    print("UsefulCoverage is top {0}".format(len(df_random_results_1000.loc[df_random_results_1000['UseCov_mean'] > scenario_5_UsefulCoverage_mean])))
    print("UniqueUsefulCoverage is top {0}".format(len(df_random_results_1000.loc[df_random_results_1000['UniUseCov_mean'] > scenario_5_UniqueUsefulCoverage_mean])))
    # Scenario 6. Opposite pairs based on s_k ratio
    list_scenario_6 = df_mobility_sorted['userID'].tolist()
    list_pairs_scenario_6 = []

    while len(list_scenario_6) > 0:
        elem1 = list_scenario_6.pop(0)
        elem2 = list_scenario_6.pop(len(list_scenario_6)//2)
        list_pairs_scenario_6.append((elem1, elem2))

    df_dyad_scenario_6 = pd.DataFrame()
    for i in range(0,len(list_pairs_scenario_6)):
        user_a = list_pairs_scenario_6[i][0]
        user_b = list_pairs_scenario_6[i][1]
        df_temp_a = df_dyad_data[(df_dyad_data['user_a']==user_a) & (df_dyad_data['user_b']==user_b)]
        if len(df_temp_a)==0:
            df_temp_b = df_dyad_data[(df_dyad_data['user_a']==user_b) & (df_dyad_data['user_b']==user_a)]
            df_dyad_scenario_6 = df_dyad_scenario_6.append(df_temp_b)
        else:
            df_dyad_scenario_6 = df_dyad_scenario_6.append(df_temp_a)
    scenario_6_Coverage_mean = df_dyad_scenario_6['Coverage'].mean()
    scenario_6_UniqueCoverage_mean = df_dyad_scenario_6['UniqueCoverage'].mean()
    scenario_6_UsefulCoverage_mean = df_dyad_scenario_6['UsefulCoverage'].mean()
    scenario_6_UniqueUsefulCoverage_mean = df_dyad_scenario_6['UniqueUsefulCoverage'].mean()
    print("Scenario 6 - s_k: Mean of Coverage - {0}, Unique Coverage - {1}, Useful Coverage - {2}, Unique Useful Coverage - {3}". \
        format(scenario_6_Coverage_mean, scenario_6_UniqueCoverage_mean, scenario_6_UsefulCoverage_mean,scenario_6_UniqueUsefulCoverage_mean))
    print("Coverage is top {0}".format(len(df_random_results_1000.loc[df_random_results_1000['Cov_mean'] > scenario_6_Coverage_mean])))
    print("UniqueCoverage is top {0}".format(len(df_random_results_1000.loc[df_random_results_1000['UniCov_mean'] > scenario_6_UniqueCoverage_mean])))
    print("UsefulCoverage is top {0}".format(len(df_random_results_1000.loc[df_random_results_1000['UseCov_mean'] > scenario_6_UsefulCoverage_mean])))
    print("UniqueUsefulCoverage is top {0}".format(len(df_random_results_1000.loc[df_random_results_1000['UniUseCov_mean'] > scenario_6_UniqueUsefulCoverage_mean])))

    print("END")