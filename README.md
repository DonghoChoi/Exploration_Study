# Exploration Study
Repository for Exploration Study Project

1. Mobile Phone Data
2. Fitbit Data (not analyzed yet)
  1. Collect activity data from fitbit server
  2. Store retrieved data into DB
3. Search Task Data 


[Location Data Process]
- gps_data_analysis.py: read the gps data to populate the location list as well as s_k ratio or gyration [table: mobility_data].
- gps_to_location_list.py: read the gps data and figure the location list of each participant, putting them into the mysql server with variables of 'userID', 'locationID', 'latitude', 'longitude', 'visit_times', 'spent_time', 'routine' [table:user_location_list].
- calculate_location_diversity.py: Read user_location_list table, calculate (1) number of locations, (2) location diversity, and (3) location loyalty, and save it into the [table:user_location_diversity]

[Search Data Process]
- searchlog_analysis_dyad.py: read search log data from lab session, calculate measures for all possible pairs. [table: dyad_data]
- search_log_to_page_list.py: read search log data from field session, extract web pages they visited, calculating distinct pages, visit_times, spent_time, routine.. [table:user_pages_visit_list]
- calculage_browse_diversity.py: Read user_pages_visit_list table, calculate (1) number of distinct domains, (2) browse diversity, and (3) browse loyalty, and save it into the [table:user_browse_history]

[Data Analysis]
- comparison_analysis_using_location_features.py: reads 

[Survey Data]
- calculate_personality.py: read user_personality_responses table, which was already imported from the survey responses, and calculate the five category scores to save them into user_personality table.
- calculate_Miller.py: read user_Miller_responses table, to calculate and save the Miller score into the table, user_Miller.
- calculate_Williams.py: reads user_Williams_respones table, calculates and saves the Williams score (Bonding scale & Bridging scale)
