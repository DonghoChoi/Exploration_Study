#!/usr/bin/python
# Author: Dongho Choi

#import time

import config
import json
import sqlite3 as lite
import time
import pandas as pd
from collections import defaultdict
#import utils
import glob
from time import localtime, strftime
import string
import os.path
import csv
import sys


device_imei = {} # a set: device_imei[device] = IMEI
device_id_list = [] # list of device id
row_id_list = [] # list of row id
datetime_list = [] # list of datetime
device_imei_list = []
num_device = 0

def preprocessing(con):
    # extract imei list from the database
    # get_imei_list(main_con)
    cur = con.cursor()
    cur.execute("PRAGMA temp_store = 2")

    data_imei = cur.execute("SELECT device, value FROM data WHERE probe LIKE '%Hardware%'")

    for x in data_imei:
        # print("device: " + x[0])  # x[0]: device column from the query results
        j = json.loads(x[1])  # x[1]: content in value column from the query results
        imei_num = j['deviceId']
        # print("deviceId: " + j['deviceId'])
        device_imei[x[0]] = imei_num  # create an array of (1) device (number) and (2) imei number

        # print (len(device_imei)) # length of the set 'device_imei'

    # add imei column to the database: function - update_imei_into_data(main_con)
    cur = con.cursor()
    cur.execute("alter table data add column imei text")
    i = 0

    cur = con.cursor()
    for device in device_imei:
        try:
            imei_value = device_imei[device]
        except KeyError:
            imei_value = device

        print ("device: %s, imei: %s, i: %d" % (device, imei_value, i))
        try:
            cur.execute("""update data set imei="%s" where device="%s";""" % (imei_value, device))
        except lite.OperationalError:
            print (imei_value)
        i += 1

    cur.execute("PRAGMA temp_store = 2")
    cur.execute("VACUUM data;")
    con.commit()

    # remove duplicate rows: function - remove_duplicate(main_con)
    cur = con.cursor()
    cur.execute("PRAGMA temp_store = 2")
    cur = con.cursor()
    cur.execute("""delete from data
                        where id not in
                        (select id
                         from data
                         group by probe,timestamp,imei);""")

    cur.execute("PRAGMA temp_store = 2")
    cur.execute("VACUUM data;")
    con.commit()

    # keep rows that contain location data: function - keep_location_probe(main_con)
    cur = con.cursor()
    cur.execute("PRAGMA temp_store = 2")
    cur = con.cursor()
    cur.execute("""DELETE FROM data WHERE probe NOT LIKE '%Location%'""")
    cur.execute("PRAGMA temp_store = 2")
    cur.execute("VACUUM data;")
    con.commit()


def get_imei_list(con):

    cur1 = con.cursor()
    cur1.execute("PRAGMA temp_store = 2")

    data_imei = cur1.execute("SELECT device, value FROM data WHERE probe LIKE '%Hardware%'")

    for x in data_imei:
        #print("device: " + x[0])  # x[0]: device column from the query results
        j = json.loads(x[1])  # x[1]: content in value column from the query results
        imei_num = j['deviceId']
        #print("deviceId: " + j['deviceId'])
        device_imei[x[0]] = imei_num  # create an array of (1) device (number) and (2) imei number

    num_device = len(device_imei)
    #print (len(device_imei)) # length of the set 'device_imei'

def update_imei_into_data(con):
    cur1 = con.cursor()
    cur1.execute("alter table data add column imei text")
    i = 0

    cur1 = con.cursor()
    for device in device_imei:
        try:
            imei_value = device_imei[device]
        except KeyError:
            imei_value = device

        print ("device: %s, imei: %s, i: %d" % (device, imei_value, i))
        try:
            cur1.execute("""update data set imei="%s" where device="%s";""" % (imei_value, device))
        except lite.OperationalError:
            print (imei_value)
        i += 1

    cur1.execute("PRAGMA temp_store = 2")
    cur1.execute("VACUUM data;")
    con.commit()

def remove_duplicate(con):
    cur1 = con.cursor()
    cur1.execute("PRAGMA temp_store = 2")
    cur1 = con.cursor()
    cur1.execute("""delete from data
                    where id not in
                    (select id
                     from data
                     group by probe,timestamp,imei);""")

    cur1.execute("PRAGMA temp_store = 2")
    cur1.execute("VACUUM data;")
    con.commit()

def keep_location_probe(con):
    cur1 = con.cursor()
    cur1.execute("PRAGMA temp_store = 2")
    cur1 = con.cursor()
    cur1.execute("""DELETE FROM data WHERE probe NOT LIKE '%Location%'""")
    cur1.execute("PRAGMA temp_store = 2")
    cur1.execute("VACUUM data;")
    con.commit()

def change_time(con):
    cur = con.cursor()
    cur.execute("PRAGMA temp_store = 2")
    cur = con.cursor()
    cur.execute("""alter table data add column date_time DATETIME;""")
    db_timestamp = cur.execute("""SELECT timestamp FROM data;""")
    i = 0
    for x in db_timestamp:
        print("index: %d - %s" % (i, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(x[0]))))
        datetime_list.append(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(x[0])))
        i+=1
    #cur.executemany("""INSERT INTO data(date_time) VALUES(?);""", db_timestamp)
    cur.executemany("""UPDATE data SET date_time VALUES(?);""", db_timestamp)
    con.commit()


if __name__ == "__main__":
    #print(directory_of_all_data)
    all_data_db = config.directory_of_all_data + "/all_data.db"
    #print(all_data_db)

    # Check if the all_data database file exists
    if (os.path.isfile(all_data_db) == False):
        print("All_data.db does not exist. Please run the funf scripts first.")
        sys.exit()

    else:
        # connecting to the database file
        main_con = lite.connect(all_data_db)

        # extract imei list from the database
        get_imei_list(main_con)

        # preprocessing: before timestamp conversion
        #preprocessing(main_con)

        # get the number of device
        #cur = main_con.cursor()
        #cur.execute("""alter table data add column date_time DATETIME;""")

        while len(device_imei)>0:
            device_imei_list.append(device_imei.pop())
        #num_device = len(device_imei) # number of device (participant)

        for i in range(num_device):
            current_imei = device_imei_list[i]
            print(current_imei)
        #    df = pd.read_sql_query("""SELECT imei,timestamp,value FROM data WHERE imei="%s";""", main_con, current_imei)

        # create separate tables by imei
        #print(df)



