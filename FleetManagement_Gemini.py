import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# --- FILE PATHS ---
WAYPOINTS_PATH = r"C:\Users\mikha\OneDrive\Documents\Demos\Fleet Management\route_waypoints_new.csv"
OUTPUT_PATH = r"C:\Users\mikha\OneDrive\Documents\Demos\Fleet Management\expanded_fleet_data_5trucks.csv"

# --- DENSITY CONFIGURATION ---
INTERVAL_SECONDS = 45  # Increased interval slightly to keep row count near 20k
STEP_SIZE = 8          # Taking every 8th point

# --- TRUCK METADATA ---
INPUT_FLEET_DATA = [
    {"truck_id": "TRK-001", "route_id": "RTE0001", "origin_city": "Atlanta", "origin_latitude": 33.749, "origin_longitude": -84.388, "destination_city": "Chicago", "destination_latitude": 41.8781, "destination_longitude": -87.6298, "truck_make": "International", "truck_model": "RH Series", "truck_year": 2021, "odometer_miles": 153303, "fuel_capacity_gallons": 212, "driver_name": "Matthew Williams", "load_type": "Food & Beverage", "load_weight_lbs": 20292, "load_value_usd": 68531, "is_temperature_controlled": False, "is_hazmat": False},
    {"truck_id": "TRK-002", "route_id": "RTE0004", "origin_city": "Dallas", "origin_latitude": 32.7767, "origin_longitude": -96.797, "destination_city": "New York", "destination_latitude": 40.7128, "destination_longitude": -74.006, "truck_make": "Freightliner", "truck_model": "M2 106", "truck_year": 2023, "odometer_miles": 262022, "fuel_capacity_gallons": 212, "driver_name": "Andrew Smith", "load_type": "Medical Equipment", "load_weight_lbs": 30033, "load_value_usd": 63513, "is_temperature_controlled": True, "is_hazmat": False},
    {"truck_id": "TRK-003", "route_id": "RTE0008", "origin_city": "Las Vegas", "origin_latitude": 36.1699, "origin_longitude": -115.1398, "destination_city": "Philadelphia", "destination_latitude": 39.9526, "destination_longitude": -75.1652, "truck_make": "Mack", "truck_model": "Granite", "truck_year": 2020, "odometer_miles": 351827, "fuel_capacity_gallons": 248, "driver_name": "Paul Lopez", "load_type": "Building Materials", "load_weight_lbs": 30096, "load_value_usd": 38052, "is_temperature_controlled": False, "is_hazmat": False},
    {"truck_id": "TRK-004", "route_id": "RTE0012", "origin_city": "New York", "origin_latitude": 40.7128, "origin_longitude": -74.006, "destination_city": "Miami", "destination_latitude": 25.7617, "destination_longitude": -80.1918, "truck_make": "Peterbilt", "truck_model": "389", "truck_year": 2022, "odometer_miles": 189041, "fuel_capacity_gallons": 205, "driver_name": "Jennifer Rodriguez", "load_type": "Chemicals", "load_weight_lbs": 27254, "load_value_usd": 62018, "is_temperature_controlled": False, "is_hazmat": True},
    {"truck_id": "TRK-005", "route_id": "RTE0002", "origin_city": "Chicago", "origin_latitude": 41.8781, "origin_longitude": -87.6298, "destination_city": "Los Angeles", "destination_latitude": 34.0522, "destination_longitude": -118.2437, "truck_make": "Volvo", "truck_model": "VNL 860", "truck_year": 2018, "odometer_miles": 291035, "fuel_capacity_gallons": 282, "driver_name": "Donald Johnson", "load_type": "Pharmaceuticals", "load_weight_lbs": 12135, "load_value_usd": 384692, "is_temperature_controlled": True, "is_hazmat": False}
]

# --- INCIDENT CONFIG ---
incident_defs = {
    "TRK-001": {"pct": 0.10, "dur_min": 60, "type": "Engine Overheat", "sev": "High"},
    "TRK-002": {"pct": 0.08, "dur_min": 45, "type": "Brake Issue", "sev": "High"},
    "TRK-003": {"pct": 0.12, "dur_min": 90, "type": "Tire Failure", "sev": "Medium"},
    "TRK-004": {"pct": 0.05, "dur_min": 30, "type": "Electrical Fault", "sev": "Medium"},
    "TRK-005": {"pct": 0.15, "dur_min": 60, "type": "Sensor Error", "sev": "Low"}
}

def haversine(lat1, lon1, lat2, lon2):
    R = 3958.8 
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi, dlambda = np.radians(lat2 - lat1), np.radians(lon2 - lon1)
    a = np.sin(dphi / 2)**2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2)**2
    return 2 * R * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

def generate_simulation():
    print("Reading waypoints...")
    try:
        waypoints_df = pd.read_csv(WAYPOINTS_PATH)
    except FileNotFoundError:
        print(f"Error: Could not find waypoints file at {WAYPOINTS_PATH}")
        return

    all_rows = []
    start_time_base = datetime(2026, 2, 2, 10, 0, 0)

    for truck_meta in INPUT_FLEET_DATA:
        truck_id = truck_meta['truck_id']
        route_id = truck_meta['route_id']
        
        # Safety filter: make sure the route actually exists in the CSV
        route_pts = waypoints_df[waypoints_df['route_id'] == route_id].sort_values('waypoint_index').reset_index(drop=True)
        
        if route_pts.empty:
            print(f"Warning: Route {route_id} for {truck_id} not found in waypoints. Skipping.")
            continue

        num_points = len(route_pts)
        lats, lons = route_pts['latitude'].values, route_pts['longitude'].values
        dists = [0] + [haversine(lats[j], lons[j], lats[j+1], lons[j+1]) for j in range(num_points - 1)]
        total_dist = sum(dists)
        
        inc_setup = incident_defs.get(truck_id)
        target_inc_idx = int(num_points * inc_setup['pct'])
        
        curr_time = start_time_base
        dist_traveled = 0.0
        
        print(f"Generating data for {truck_id} ({route_id})...")
        for idx in range(0, num_points, STEP_SIZE):
            seg_dist = sum(dists[max(0, idx-STEP_SIZE+1) : idx+1])
            dist_traveled += seg_dist
            progress = (dist_traveled / total_dist) * 100 if total_dist > 0 else 100
            
            row = truck_meta.copy()
            row.update({
                'date': curr_time.strftime('%Y-%m-%d'),
                'current_time': curr_time,
                'current_latitude': lats[idx],
                'current_longitude': lons[idx],
                'waypoint_index': idx,
                'total_waypoints': num_points,
                'route_progress_percent': round(progress, 2),
                'speed_mph': round(65.0 + np.random.normal(0, 2), 1),
                'engine_rpm': int(1850 + np.random.normal(0, 20)),
                'fuel_level_percent': round(max(0, 98.0 - (progress * 0.7)), 1),
                'engine_temp_f': round(185 + np.random.normal(0, 1), 1),
                'tire_pressure_psi': round(110 + np.random.normal(0, 0.5), 1),
                'battery_voltage': round(13.8 + np.random.normal(0, 0.05), 2),
                'distance_traveled_miles': round(dist_traveled, 2),
                'distance_remaining_miles': round(max(0, total_dist - dist_traveled), 2),
                'status': "In Transit",
                'has_incident': False, 'incident_type': None, 'incident_severity': None, 'at_incident_location': False
            })
            all_rows.append(row)
            curr_time += timedelta(seconds=INTERVAL_SECONDS)
            
            # Check for Incident
            if idx <= target_inc_idx < idx + STEP_SIZE:
                steps = (inc_setup['dur_min'] * 60) // INTERVAL_SECONDS
                for _ in range(steps):
                    stationary_row = row.copy()
                    stationary_row.update({
                        'current_time': curr_time,
                        'speed_mph': 0.0,
                        'engine_rpm': 650,
                        'status': f"Stationary ({inc_setup['type']})",
                        'has_incident': True,
                        'incident_type': inc_setup['type'],
                        'incident_severity': inc_setup['sev'],
                        'at_incident_location': True
                    })
                    all_rows.append(stationary_row)
                    curr_time += timedelta(seconds=INTERVAL_SECONDS)

    if not all_rows:
        print("Error: No data rows generated. Check route IDs.")
        return

    # --- FINAL SORTING ---
    print("Finalizing sequential sort...")
    final_df = pd.DataFrame(all_rows)
    
    # Sorting interleaved by time so all trucks move together
    final_df = final_df.sort_values(by=['current_time', 'truck_id']).reset_index(drop=True)
    
    # Format time for CSV
    final_df['current_time'] = final_df['current_time'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Define exact column order as requested
    cols_order = [
        'truck_id', 'route_id', 'origin_city', 'origin_latitude', 'origin_longitude', 
        'destination_city', 'destination_latitude', 'destination_longitude', 
        'truck_make', 'truck_model', 'truck_year', 'odometer_miles', 'fuel_capacity_gallons', 
        'driver_name', 'load_type', 'load_weight_lbs', 'load_value_usd', 'is_temperature_controlled', 
        'is_hazmat', 'date', 'current_time', 'current_latitude', 'current_longitude', 
        'waypoint_index', 'total_waypoints', 'route_progress_percent', 'speed_mph', 
        'engine_rpm', 'fuel_level_percent', 'engine_temp_f', 'tire_pressure_psi', 
        'battery_voltage', 'distance_traveled_miles', 'distance_remaining_miles', 
        'status', 'has_incident', 'incident_type', 'incident_severity', 'at_incident_location'
    ]
    
    # Final column filtering
    final_df = final_df[cols_order]
    
    final_df.to_csv(OUTPUT_PATH, index=False)
    print(f"Success! Final record count: {len(final_df)}")

if __name__ == "__main__":
    generate_simulation()