# import the needed packages

import sqlite3
import pandas as pd
import csv
from row64tools import ramdb

# #######################################
#   STEP 1: CONNECT TO THE DATABASE     #
#########################################

conn = sqlite3.connect(r"C:\Users\mikha\Downloads\DBexampleROW64.db")
cursor = conn.cursor()

# ################################
#   STEP 2: DATA TRANSFORMATION  #
##################################

 
#Grab the data for from LAPANALYSIS table and merge it with other look up tables. Execute the query
lap_analysis_query = """
SELECT 
    LAPANALYSIS.LapID,
    ANALYSISFORMULATION.Name AS FormulationName,
    ANALYSISCHANNEL.Name AS ChannelName,
    ANALYSISLOCATION.Name AS LocationName,
    ANALYSISCONDITION.Name AS ConditionName,
    MIN(LAPANALYSIS.Idx) AS ValueMin,
    MAX(LAPANALYSIS.Idx) AS ValueMax,
    AVG(LAPANALYSIS.Idx) AS ValueAvg
FROM LAPANALYSIS
JOIN ANALYSISFORMULATION ON LAPANALYSIS.FormulationID = ANALYSISFORMULATION.ID
JOIN ANALYSISCHANNEL ON LAPANALYSIS.ChannelID = ANALYSISCHANNEL.ID
JOIN ANALYSISLOCATION ON LAPANALYSIS.LocationID = ANALYSISLOCATION.ID
JOIN ANALYSISCONDITION ON LAPANALYSIS.ConditionID = ANALYSISCONDITION.ID
WHERE
  ANALYSISCHANNEL.Name IN ('presLDL_F', 'tempLDL_F')
  AND  ANALYSISLOCATION.Name = 'LAP'
  AND ANALYSISCONDITION.Name = 'none'

GROUP BY LAPANALYSIS.LapID, ANALYSISFORMULATION.Name, ANALYSISCHANNEL.Name
"""

lap_analysis_df = pd.read_sql_query(lap_analysis_query, conn)

#Grab additional information from other lookup tables needed for the reports and dashboards

final_query = """
SELECT 
    SEASON.Name AS SeasonName,
    EVENT.Name AS EventName,
    SESSION.Name AS SessionName,
    SESSION.Name || '_' || RUN.Number AS SessionRunNumber,
    LAP.LapNumber,
    RUN.Number AS RunNumber,
    RIDER.Alias AS RiderAlias,
    LAP.ID AS LapID
FROM LAP
JOIN RUN ON LAP.RunID = RUN.ID
JOIN SESSION ON RUN.SessionID = SESSION.ID
JOIN EVENT ON SESSION.EventID = EVENT.ID
JOIN SEASON ON EVENT.SeasonID = SEASON.ID
JOIN RIDER ON RUN.RiderID = RIDER.ID
"""
meta_df = pd.read_sql_query(final_query, conn)


# Merge the two dataframes to get one flat table with all sessions, riders, laps, etc.
final_df = meta_df.merge(lap_analysis_df, on="LapID", how="inner")

# Pivot the data to set each channel type as a separate column
final_pivot = final_df.pivot_table(
    index=[
        "SeasonName", "EventName", "SessionName", "SessionRunNumber",
        "LapNumber", "RunNumber", "RiderAlias"
    ],
    columns="ChannelName",
    values=["ValueMin", "ValueMax", "ValueAvg"]
    
).reset_index()

# Rename the columns
final_pivot.columns = [
    f"{col[1]}_{col[0].replace('Value', '').lower()}" if col[1] else col[0]
    for col in final_pivot.columns
]

###############################################################################
# THIS STEP IS OPTIONAL AND IS DONE SO WE ARE ABLE TO PLOT LOCATIONS ON A MAP # 
###############################################################################

# create a table with track names and geo coordinates
coords_map = {
    'SEPANG': (2.7608, 101.7381),        # Sepang International Circuit, Malaysia
    'DOHA': (25.2843, 51.4410),          # Losail Intl Circuit near Doha, Qatar
    'SPIELBERG': (47.2190, 14.7640),     # Red Bull Ring, Spielberg, Austria
    'MISANO': (43.9620, 12.6846),        # Misano World Circuit, Italy
    'ARGENTINA': (-27.5063, -64.9309),   # Termas de Río Hondo Circuit, Argentina
    'AUSTIN': (30.1346, -97.6359),       # Circuit of the Americas, Texas, USA
    'JEREZ': (36.7081, -6.0341),         # Circuito de Jerez, Spain
    'LEMANS': (47.9565, 0.2249),         # Le Mans, France
    'MUGELLO': (43.9970, 11.3710),       # Mugello Circuit, Italy
    'BARCELONA': (41.5700, 2.2610),      # Circuit de Barcelona-Catalunya, Spain
    'ASSEN': (53.0036, 6.5150),          # TT Circuit Assen, Netherlands
    'SACHSENRING': (50.7180, 12.6950),   # Sachsenring, Germany
    'BRNO': (49.2020, 16.5650),          # Brno Circuit, Czech Republic
    'SILVERSTONE': (52.0733, -1.0140),   # Silverstone Circuit, UK
    'ARAGON': (41.0670, -0.2160),        # MotorLand Aragón, Spain
    'BURIRAM': (14.9576, 103.0849),      # Chang International Circuit, Thailand
    'MOTEGI': (36.5323, 140.2267),       # Twin Ring Motegi, Japan
    'PISLAND': (-38.4910, 145.2370),     # Phillip Island Circuit, Australia
    'VALENCIA': (39.4910, -0.6340),      # Circuit Ricardo Tormo, Spain
    'PORTIMAO': (37.2270, -8.6260),      # Algarve Intl Circuit, Portugal
    'MANDALIKA': (-8.8830, 116.3080),    # Mandalika International Street Circuit, Indonesia
    'TERMAS': (-27.5063, -64.9309),      # Termas de Río Hondo (same as ARGENTINA)
    'BUDDH': (28.3487, 77.5331)          # Buddh International Circuit, India
}

# merge it with the master table by location name in the EventName column
final_pivot["location"] = final_pivot["EventName"].str.split().str[-1]
final_pivot[["latitude", "longitude"]] = final_pivot["location"].map(coords_map).apply(pd.Series)

# ###############################
#       STEP 3: CLEAN DATA      #
#################################


# -------------------------------#
#   OPTION 1: Create a csv file  #
# -------------------------------#
final_pivot.to_csv(r"C:\Users\mikha\final_pivot_output.csv", index=False)

# ------------------------------------#
#   OPTION 1: Upload to ROW64 server  #
# ------------------------------------#

# Create a file 
ramdb.save_from_df(lap_analysis_df, r"C:\Users\mikha\LapAnalysisKTM.ramdb")

localfile = r"C:\Users\mikha\LapAnalysisKTM.ramdb"

remote_path = "/var/www/ramdb/live/RAMDB.MikhailData/MikhailData/LapAnalysis.ramdb"

# Ubuntu server credentials
hostname = "192.168.1.20"   # Replace with your Ubuntu server's local IP
port = 22
username = "row64"   # Ubuntu login username
password = "temp7"   # Or use SSH key auth

# Transmit the data
try:
    # Connect via SSH
    transport = paramiko.Transport((hostname, port))
    transport.connect(username=username, password=password)

    # Start SFTP session
    sftp = paramiko.SFTPClient.from_transport(transport)

    # Upload file
    sftp.put(localfile, remote_path)
    print(f"✅ File uploaded to {remote_path} on {hostname}")

    # Close connection
    sftp.close()
    transport.close()

except Exception as e:
    print(f"❌ Failed to upload: {e}")