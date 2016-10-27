#!/usr/bin/python
# Author: Dongho Choi

#import time

from config import directory_of_all_data
import json

from collections import defaultdict
#import utils
import glob
from time import localtime, strftime
import sqlite3 as lite
import string
import time
import os.path
import csv
import sys


device_imei = {} # device_imei[device] = IMEI
device_id_list = [] # list of device id
row_id_list = [] # list of row id

def get_imei_list(con):

    cur1 = con.cursor()
    cur1.execute("PRAGMA temp_store = 2")

    data_imei = cur1.execute("SELECT device, value FROM data WHERE probe LIKE '%Hardware%'")

    for x in data_imei:
        print("device: " + x[0])  # x[0]: device column from the query results
        j = json.loads(x[1])  # x[1]: content in value column from the query results
        imei_num = j['deviceId']
        print("deviceId: " + j['deviceId'])
        device_imei[x[0]] = imei_num  # create an array of (1) device (number) and (2) imei number

def get_device_row_id_list(con):
    cur1 = con.cursor()
    cur1.execute("PRAGMA temp_store = 2")
    cur1 = main_con.cursor()
    data = cur1.execute("select rowid,device from data")

    for x in data:
        device_id_list.append(x[1])
        row_id_list.append(x[0])

def update_imei_into_data(con):
    cur1 = con.cursor()
    cur1.execute("alter table data add column imei text")
    i = 0

    cur1 = con.cursor()
    for device in device_id_list:
        try:
            column_field = device_imei[device]
        except KeyError:
            column_field = device

        print (column_field, row_id_list[i])
        try:
            cur1.execute("""update data set imei="%s" where rowid="%d";""" % (column_field, row_id_list[i]))
        except lite.OperationalError:
            print (column_field)
        i += 1

    cur1.execute("PRAGMA temp_store = 2")
    cur1.execute("VACUUM data;")
    con.commit()

if __name__ == "__main__":
    print(directory_of_all_data)
    all_data_db = directory_of_all_data + "/all_data.db"
    print(all_data_db)

    # Check if the all_data database file exists
    if (os.path.isfile(all_data_db) == False):
        print("All_data.db does not exist. Please run the funf scripts first.")
        sys.exit()

    else:
        # connecting to the database file
        main_con = lite.connect(all_data_db)

        get_imei_list(main_con)
        get_device_row_id_list(main_con)
        update_imei_into_data(main_con)

