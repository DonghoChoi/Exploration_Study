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
- search_log_analysis_dyad.py: read search log data from lab session, calculate measures for all possible pairs. [table: dyad_data]
- search_log_to_page_list.py: read search log data from field session, extract web pages they visited, calculating distinct pages, visit_times, spent_time, routine.. [table:user_pages_visit_list]
- calculage_browse_diversity.py: Read user_pages_visit_list table, calculate (1) number of distinct domains, (2) browse diversity, and (3) browse loyalty, and save it into the [table:user_browse_history]
- lab_task1_to_running_time.py: read search log data from lab session and calculate the time spent each activity.
- extract_query_from_field_search.py: out of four-week field search data, this code mines search sessions and extracts distinct queries, coverage, useful coverage, etc. [table:pages_field_session]
- mine_sessions_from_field_search.py: [input table:pages_field_session] mine subsessions in the pages list and merge them into sessions with sessionID [output table: user_field_search_session] and the number of issued queries and distinct queries [outpu table: user_field_queries] 
- calculate_coverage_field_session.py: [input table:user_field_search_session] calculate coverage, useful coverage, and utilization ratio for each search session.


[General Data Analysis]
- comparison_analysis_using_location_features.py: reads
- join_tables.py: create an aggregated view that contains independent variables.

[Treasure Hunt Data Process]
- parse_video_coding_vcode.py: read the coding data from the **vCode** program in the text format and store them into the mysql db in the server [output table: user_TH_vCode_results]
- parse_video_coding_boris.py: read the coding data from the **Boris** program, which is in tsv format, and store them into the mysql db in the server [output table: user_TH_Boris_results].
- TH_data_merging.py: read both vCode and Boris coding data and merge them into a table [output table: user_TH_merged_results]

[Survey Data]
- calculate_personality.py: read user_personality_responses table, which was already imported from the survey responses, and calculate the five category scores to save them into user_personality table.
- calculate_Miller.py: read user_Miller_responses table, to calculate and save the Miller score into the table, user_Miller.
- calculate_Williams.py: reads user_Williams_respones table, calculates and saves the Williams score (Bonding scale & Bridging scale)
- calculate_time_preference.py: reads responses regarding time preference, calculates the measures of timeA and timeB, explaining the participant's time preference.
- calculate_risk_preference.py: get riskA and riskB.