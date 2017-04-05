#!/usr/bin/python
# Author: Dongho Choi
'''
This script
(1)

'''

import os.path
import datetime
import math
import time
import pandas as pd
from sshtunnel import SSHTunnelForwarder # for SSH connection
import pymysql.cursors # MySQL handling API
import sys
import csv
#sys.path.append("./configs/")
sys.path.append("/Users/donghochoi/Documents/Work/Exploration_Study/Dissertation/Code/local/configs/")
import server_config # (1) info2_server (2) exploration_db
from xml.dom.minidom import parseString
import dateutil.parser
import pytz
from tzwhere import tzwhere
from timezonefinder import TimezoneFinder
import glob, os

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
                                 db=server_config.exploration_db['database'],
                                 charset='utf8')
    connection.autocommit(True)
    cursor = connection.cursor()
    print("MySQL connection established")

    # Get the participants list from the table of 'final_participants'
    df_participants = pd.read_sql('SELECT * FROM final_participants', con=connection)
    print("Participants Table READ")
    userID_list = df_participants['userID'].tolist()

    # Base director for google kml files
    input_directory_base = "/Users/donghochoi/Documents/Work/Exploration_Study/Dissertation/UserStudy_Nov/google_kml/"

    #for i in range(0, 1):
    for i in range(0, len(userID_list)):  # i - current userID
        userID = userID_list[i]
        #userID=0
        input_directory_user = input_directory_base + str(userID) +"/"
        os.chdir(input_directory_user)
        print("################## userID: {0} ###################".format(userID))
        # KML files in the user directory
        for file in glob.glob("*.kml"):
            print(file)
            input_file = open(str(input_directory_user + file))
            data = input_file.read()
            input_file.close()

            # Parse the string into a DOM
            dom = parseString(data)

            for d in dom.getElementsByTagName('Placemark'):
                # Name of the Place
                name_element = d.getElementsByTagName('name')
                if (name_element[0].hasChildNodes()):
                    name = name_element[0].firstChild.data
                else:
                    name = "NO_NAME"
                # Coordinates
                gx_track = d.getElementsByTagName('gx:Track')[0]
                gx_coord_element = gx_track.getElementsByTagName('gx:coord')
                if gx_coord_element.length > 0:
                    gx_coord = gx_track.getElementsByTagName('gx:coord')[0]
                    coords = gx_coord.firstChild.data.split(' ')
                    longitude = float(coords[0])
                    latitude = float(coords[1])
                else:
                    break
                #print("longitude:{0}, latitude:{1}".format(longitude, latitude))
                # Get timezone of the coordinates
                # t = datetime.tzinfo("America/New_York")
                tf = TimezoneFinder()
                tz = tzwhere.tzwhere(shapely=True)
                timezone_str = tf.timezone_at(lng=longitude, lat=latitude)
                # timezone_str = tzwhere.tzNameAt(37.3880961, -5.9823299)
                #print("timezone of current location: {0}".format(timezone_str))
                t = pytz.timezone(timezone_str)
                # begin and end time of the visiting
                timespan = d.getElementsByTagName('TimeSpan')[0]
                begin = timespan.getElementsByTagName('begin')[0].firstChild.data
                begin_datetime = dateutil.parser.parse(begin).astimezone(t)
                end = timespan.getElementsByTagName('end')[0].firstChild.data
                end_datetime = dateutil.parser.parse(end).astimezone(t)
                begin_date = begin_datetime.date()
                begin_time = begin_datetime.time()
                end_date = end_datetime.date()
                end_time = end_datetime.time()
                #print("Begin date: {0}, Begin time: {1}, End date: {2}, End time: {3}".format(begin_date, begin_time,end_date, end_time))
                # insert sql row using this data
                sql = "INSERT INTO user_kml_data (userID,place_name,begin_datetime,end_datetime,begin_date,begin_time,end_date,end_time,longitude,latitude,timezone) VALUES ({0},'{1}','{2}','{3}','{4}','{5}','{6}','{7}',{8},{9},'{10}')". \
                    format(str(userID),str(connection.escape_string(name)),str(begin_datetime.strftime('%Y-%m-%d %H:%M:%S')),str(end_datetime.strftime('%Y-%m-%d %H:%M:%S')),str(begin_date),str(begin_time),\
                           str(end_date),str(end_time),str(longitude),str(latitude),str(timezone_str))
                print(sql)
                cursor.execute(sql)

    server.stop()

