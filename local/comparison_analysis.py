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
from geopy.distance import vincenty
import sys
sys.path.append("./configs/")
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
    df_mobility_data = pd.read_sql('SELECT * FROM mobility_data', con=connection)
    print("Mobility data READ")

    # Get statistics
    get_statistics(df_dyad_data)

    df_mobility_data_explorer = df_mobility_data.sort_values(by='s_k').loc[df_mobility_data['s_k']<1]
    df_mobility_data_returner = df_mobility_data.sort_values(by='s_k').loc[df_mobility_data['s_k']>=1]

    list_explorer = df_mobility_data_explorer['userID']
    list_returner = df_mobility_data_returner['userID']

    list_explorer_pairset = [list(x) for x in itertools.combinations(list_explorer,2)]
    list_returner_pairset = [list(x) for x in itertools.combinations(list_returner,2)]
    list_explorer_returner_pairset = list(itertools.product(list_explorer,list_returner))

    # Explorer-Explorer pair data
    df_dyad_explorer_explorer = pd.DataFrame()
    for i in range(0,len(list_explorer_pairset)):
        user_a = list_explorer_pairset[i][0]
        user_b = list_explorer_pairset[i][1]
        df_temp_a = df_dyad_data[(df_dyad_data['user_a']==user_a) & (df_dyad_data['user_b']==user_b)]
        if len(df_temp_a)==0:
            df_temp_b = df_dyad_data[(df_dyad_data['user_a']==user_b) & (df_dyad_data['user_b']==user_a)]
            df_dyad_explorer_explorer = df_dyad_explorer_explorer.append(df_temp_b)
        else:
            df_dyad_explorer_explorer = df_dyad_explorer_explorer.append(df_temp_a)
    get_statistics(df_dyad_explorer_explorer)

    # Returner-Returner pair data
    df_dyad_returner_returner = pd.DataFrame()
    for i in range(0,len(list_returner_pairset)):
        user_a = list_returner_pairset[i][0]
        user_b = list_returner_pairset[i][1]
        df_temp_a = df_dyad_data[(df_dyad_data['user_a']==user_a) & (df_dyad_data['user_b']==user_b)]
        if len(df_temp_a)==0:
            df_temp_b = df_dyad_data[(df_dyad_data['user_a']==user_b) & (df_dyad_data['user_b']==user_a)]
            df_dyad_returner_returner = df_dyad_returner_returner.append(df_temp_b)
        else:
            df_dyad_returner_returner = df_dyad_returner_returner.append(df_temp_a)
    get_statistics(df_dyad_returner_returner)

    # Explorer-Returner pair data
    df_dyad_explorer_returner = pd.DataFrame()
    for i in range(0,len(list_explorer_returner_pairset)):
        user_a = list_explorer_returner_pairset[i][0]
        user_b = list_explorer_returner_pairset[i][1]
        df_temp_a = df_dyad_data[(df_dyad_data['user_a']==user_a) & (df_dyad_data['user_b']==user_b)]
        if len(df_temp_a)==0:
            df_temp_b = df_dyad_data[(df_dyad_data['user_a']==user_b) & (df_dyad_data['user_b']==user_a)]
            df_dyad_explorer_returner = df_dyad_explorer_returner.append(df_temp_b)
        else:
            df_dyad_explorer_returner = df_dyad_explorer_returner.append(df_temp_a)
    get_statistics(df_dyad_explorer_returner)

    # Statistics

    # Box plot
    fig = figure()
    ax = axes()
    hold(True)

    Coverage = [df_dyad_explorer_explorer['Coverage'], df_dyad_returner_returner['Coverage'],
                df_dyad_explorer_returner['Coverage']]
    UniqueCoverage = [df_dyad_explorer_explorer['UniqueCoverage'], df_dyad_returner_returner['UniqueCoverage'],
                      df_dyad_explorer_returner['UniqueCoverage']]
    UsefulCoverage = [df_dyad_explorer_explorer['UsefulCoverage'], df_dyad_returner_returner['UsefulCoverage'],
                      df_dyad_explorer_returner['UsefulCoverage']]
    UniqueUsefulCoverage = [df_dyad_explorer_explorer['UniqueUsefulCoverage'],
                            df_dyad_returner_returner['UniqueUsefulCoverage'],
                            df_dyad_explorer_returner['UniqueUsefulCoverage']]

    # first boxplot pair
    bp = boxplot(Coverage, positions=[1, 2, 3], widths=0.2)
    setBoxColors(bp)

    # second boxplot pair
    bp = boxplot(UniqueCoverage, positions=[5, 6, 7], widths=0.2)
    setBoxColors(bp)

    # third boxplot pair
    bp = boxplot(UsefulCoverage, positions=[9, 10, 11], widths=0.2)
    setBoxColors(bp)

    # fourth boxplot pair
    bp = boxplot(UniqueUsefulCoverage, positions=[13, 14, 15], widths=0.2)
    setBoxColors(bp)

    # set axes limits and labels
    xlim(0, 16)
    ylim(0, 45)
    ax.set_xticklabels(['Coverage', 'UniqueCoverage', 'UsefulCoverage', 'UniqueUsefulCoverage'])
    ax.set_xticks([2, 6, 10, 14])

    # draw temporary red and blue lines and use them to create a legend
    hB, = plot([1, 1], 'b-')
    hR, = plot([1, 1], 'r-')
    hG, = plot([1, 1], 'g-')
    legend((hB, hR, hG), ('Explorer-Explorer', 'Returner-Returner', 'Explorer-Returner'))
    hB.set_visible(False)
    hR.set_visible(False)
    hG.set_visible(False)

    #savefig('boxcompare.png')
    show()

    print("##### END #####")