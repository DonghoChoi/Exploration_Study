#!/usr/bin/python
# Author: Dongho Choi

#!/usr/bin/python
# Author: Dongho Choi

#import time

#import config
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
sys.path.append("./configs/")
import directory_config # (1) base_directory (2) raw_data_directory


device_imei = {} # a set: device_imei[device] = IMEI
device_imei_list = []
num_device = 0

def get_imei_list(con):

    cur1 = con.cursor()
    cur1.execute("PRAGMA temp_store = 2")

    data_imei = cur1.execute("SELECT device, value FROM data WHERE probe LIKE '%Hardware%'")

    for x in data_imei:
        j = json.loads(x[1])  # x[1]: content in value column from the query results
        imei_num = j['deviceId']
        device_imei[x[0]] = imei_num  # create an array of (1) device (number) and (2) imei number
        #print("device number:%s, imei number:%s" % (x[0],imei_num))
    num_device = len(device_imei)
    #print (len(device_imei)) # length of the set 'device_imei'


if __name__ == "__main__":

    all_data_db = directory_config.exploration_base_directory + "1123to1124/data/all_data.db"
    print(all_data_db)

    # Check if the all_data database file exists
    if (os.path.isfile(all_data_db) == False):
        print("All_data.db does not exist. Please run the funf scripts first.")
        sys.exit()

    else:
        # connecting to the database file
        main_con = lite.connect(all_data_db)

        # extract imei list from the database
        get_imei_list(main_con) # will get the set 'device_imei'
        print ("number of imei: %d" % len(device_imei)) # length of the set 'device_imei'
        #print(device_imei)
        device_imei_list = list(device_imei.values())
        for i in range(0,len(device_imei_list)):
            current_imei = device_imei_list[i]
            print(current_imei)

