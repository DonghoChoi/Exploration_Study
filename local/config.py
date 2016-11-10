# -*- coding: utf-8 -*-
"""
Created on Tue Apr 26 02:06:33 2016

@author: donghochoi
"""

import datetime

#directory_of_all_data = '/Users/donghochoi/Documents/Work/Sensors_Study/Dropbox/Apps/Funf In A Box/daily_process/data'
#directory_of_all_data='/Users/donghochoi/Documents/Work/Sensors_Study/Dropbox/Apps/Funf In A Box/data_upto_110616/data' #

base_directory = '/Users/donghochoi/Documents/Work/Sensors_Study/Dropbox/Apps/Funf In A Box/'
raw_data_directory = base_directory + "RU SensorStudy/data/raw"
current_all_data = base_directory + "data_upto_110916/data"

#date_today = datetime.datetime.now()
#print("mmddyy:%s" %(date_today.strftime('%m%d%y')))


info2_server = {
    'user': 'dc817',
    'password':'Chlehdgh0717',
    'host':'128.6.192.99'
}

exploration_db = {
  'user': 'dc817',
  'password': 'Chlehdgh0717',
  'host': '128.6.192.99',
  'database': 'ExplorationStudy',
  'raise_on_warnings': True
}