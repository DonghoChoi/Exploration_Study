# Author: Dongho Choi
# Copy the raw files in dropbox into another directory regarding date

import datetime, time
import os
import shutil


import sys, datetime
sys.path.append("./configs/")
import server_config # (1) info2_server (2) exploration_db
import directory_config # (1) base_directory (2) raw_data_directory


#src_directory = '/Users/donghochoi/Documents/Work/Sensors_Study/Dropbox/Apps/Funf In A Box/RU SensorStudy/data/raw'
src_directory = directory_config.exploration_raw_data_directory
dst_directory = '/Users/donghochoi/Documents/Work/Sensors_Study/Dropbox/Apps/Funf In A Box/RU ExplorationStudy/1115to1116/data/raw'

'''
# For directory 'data_upto_110516'
end_day = datetime.datetime(2016,11,6,0,0).timestamp()
start_day = datetime.datetime(2016,11,1,0,0).timestamp()
# For directory 'data_upto_110716'
start_day = datetime.datetime(2016,11,7,0,0).timestamp()
end_day = datetime.datetime(2016,11,8,0,0).timestamp()
# For directory 'data_upto_110816'
start_day = datetime.datetime(2016,11,8,0,0).timestamp()
end_day = datetime.datetime(2016,11,9,0,0).timestamp()
# For directory 'data_upto_111016'
start_day = datetime.datetime(2016,11,10,0,0).timestamp()
end_day = datetime.datetime(2016,11,11,0,0).timestamp()
# For directory '1102to1106'
start_day = datetime.datetime(2016,11,2,0,0).timestamp()
end_day = datetime.datetime(2016,11,7,0,0).timestamp()
# For directory '1107to1109'
start_day = datetime.datetime(2016,11,7,0,0).timestamp()
end_day = datetime.datetime(2016,11,10,0,0).timestamp()
# For directory '1110to1111'
start_day = datetime.datetime(2016,11,10,0,0).timestamp()
end_day = datetime.datetime(2016,11,12,0,0).timestamp()
'''
# For directory '1112to1113'
start_day = datetime.datetime(2016,11,15,0,0).timestamp()
end_day = datetime.datetime(2016,11,17,0,0).timestamp()

print("start day: %s, end day: %s" % (start_day,end_day))

files = os.listdir(src_directory)

i=0
j=0
for f in files:
    src_file = os.path.join(src_directory,f)
    file_ctime = os.path.getctime(src_file)
    print(file_ctime)
    if ((file_ctime >= start_day) & (file_ctime < end_day)):
        print("created: %s - one to be copied." % (time.strftime('%m-%d-%Y %H:%M:%S',time.localtime(file_ctime))))
        dst_file = os.path.join(dst_directory,f)
        shutil.copy(src_file,dst_file)
        i+=1
    print("%d files are copied" % (i))
    j+=1

print("%d files in raw directory" % (j))
print("Copy completed.")