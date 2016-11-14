# Author: Dongho Choi

import os
import sys, datetime
import time
sys.path.append("./configs/")
import server_config # (1) info2_server (2) exploration_db
import directory_config # (1) base_directory (2) raw_data_directory

print(server_config.info2_server['user'])
print(directory_config.base_directory)
print(directory_config.raw_data_directory)

today = datetime.date.today()
print(today)
new_directory = directory_config.base_directory + 'data_upto_'+ str(today.month) + str(today.day) + str(today.year)
print(new_directory)
m_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(m_now)
#print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(m_now)))

# create a new directory unless the same directory already exists
#if(os.path.exists(new_directory)):
 #   os.makedirs(new_directory)