#!/usr/bin/python
# Author: Dongho Choi

from sshtunnel import SSHTunnelForwarder # for SSH connection
import pymysql.cursors # MySQL handling API
import pandas as pd
import sys
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

    sql = "DROP VIEW IF EXISTS aggregated_data"
    cursor.execute(sql)

    sql = "CREATE VIEW aggregated_data AS (\
            SELECT id.userID,id.Coverage,id.UniqueCoverage,id.UsefulCoverage,id.UniqueUsefulCoverage,loc.total_locations,loc.location_diversity,loc.location_loyalty,\
                online.total_domains,online.online_diversity,online.online_loyalty,pers.Extraversion,pers.Agreeableness,pers.Conscientiousness,pers.Neuroticism,pers.Openness,\
                user_Miller.M_score,user_Miller.B_score,user_Miller.Miller_score,will.Bonding_scale,will.Bridging_scale,recruits.gender,recruits.year,\
                demo.income_work,demo.income_parents,demo.income_others,\
                demo.expense_leisure,demo.search_years,demo.search_expertise,\
                post.Q2 AS topic_familiarity, \
                sc.cr_score,sc.cc_score,sc.mt_score,sc.cp_score,sc.mp_score, \
                tp.timeA, tp.timeB, \
                rp.riskA, rp.riskB \
            FROM individual_data id \
            INNER JOIN user_location_diversity loc ON id.userID=loc.userID \
            INNER JOIN user_online_diversity online ON id.userID=online.userID \
            INNER JOIN user_personality pers ON id.userID=pers.userID \
            INNER JOIN user_Miller ON id.userID=user_Miller.userID \
            INNER JOIN user_Williams will ON id.userID=will.userID \
            INNER JOIN recruits ON id.userID=recruits.userID \
            INNER JOIN post_questions post ON id.userID=post.userID \
            INNER JOIN questionnaire_demographic demo ON id.userID=demo.userID \
            INNER JOIN spatial_capability sc ON id.userID=sc.userID \
            INNER JOIN user_time_preference tp ON id.userID=tp.userID \
            INNER JOIN user_risk_preference rp ON id.userID=rp.userID)"

    cursor.execute(sql)

    df_aggregated_data = pd.read_sql('SELECT * FROM aggregated_data', con=connection)
    df_aggregated_data.to_csv(path_or_buf="/Users/donghochoi/Documents/Work/Exploration_Study/Dissertation/Data/aggregate_data.csv")
    server.stop()

    print("END")