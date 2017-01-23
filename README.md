# Exploration Study
Repository for Exploration Study Project

1. Mobile Phone Data
2. Fitbit Data (not analyzed yet)
  1. Collect activity data from fitbit server
  2. Store retrieved data into DB
3. Search Task Data 


- gps_data_analysis.py: read the gps data to populate the location list as well as s_k ratio or gyration
- gps_to_location_list.py: read the gps data and figure the location list of each participant, putting them into the mysql server with variables of 'userID','locationID','latitude','longitude','visit_times','spent_time','routine'.
