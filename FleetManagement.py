"""
Fleet Simulation Generator — 40 Trucks, 10 Routes
====================================================
Expands the 5-truck simulation to 40 trucks (4 per route).
Each truck generates time-series rows as it moves along waypoints,
creating the streaming animation effect in Row64.

Output columns (exact order):
  - Original columns (truck_id through at_incident_location)
  - _ImagePath, _File, Filetype
  - Maintenance columns (last_service_date through maintenance_priority)
  - HOS columns (shift_start_time through hos_7day_remaining)
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

np.random.seed(42)
random.seed(42)

# --- FILE PATHS ---
WAYPOINTS_PATH = r"C:\Users\mikha\OneDrive\Documents\Demos\Fleet Management\route_waypoints_new.csv"
OUTPUT_PATH = r"C:\Users\mikha\OneDrive\Documents\Demos\Fleet Management\expanded_fleet_data_40trucks.csv"

# --- DENSITY CONFIGURATION ---
INTERVAL_SECONDS = 45   # Time between data points
STEP_SIZE = 8           # Take every 8th waypoint

# --- REFERENCE DATA ---
TRUCK_MAKES = {
    'Freightliner': ['Cascadia', 'M2 106', 'Columbia'],
    'Peterbilt': ['579', '389', '567'],
    'Kenworth': ['T680', 'W900', 'T880'],
    'Volvo': ['VNL 760', 'VNL 860', 'VHD'],
    'Mack': ['Anthem', 'Pinnacle', 'Granite'],
    'International': ['LT Series', 'HX Series', 'RH Series']
}

LOAD_TYPES = [
    {'type': 'Refrigerated Goods', 'temp': True, 'hazmat': False, 'weight': (15000, 35000), 'value': (50000, 200000)},
    {'type': 'Electronics', 'temp': False, 'hazmat': False, 'weight': (8000, 25000), 'value': (100000, 500000)},
    {'type': 'Pharmaceuticals', 'temp': True, 'hazmat': False, 'weight': (5000, 15000), 'value': (200000, 1000000)},
    {'type': 'General Freight', 'temp': False, 'hazmat': False, 'weight': (20000, 45000), 'value': (20000, 80000)},
    {'type': 'Automotive Parts', 'temp': False, 'hazmat': False, 'weight': (15000, 40000), 'value': (50000, 150000)},
    {'type': 'Chemicals', 'temp': False, 'hazmat': True, 'weight': (25000, 44000), 'value': (30000, 100000)},
    {'type': 'Food & Beverage', 'temp': True, 'hazmat': False, 'weight': (18000, 42000), 'value': (25000, 75000)},
    {'type': 'Building Materials', 'temp': False, 'hazmat': False, 'weight': (30000, 45000), 'value': (15000, 50000)},
    {'type': 'Consumer Goods', 'temp': False, 'hazmat': False, 'weight': (12000, 35000), 'value': (40000, 120000)},
    {'type': 'Medical Equipment', 'temp': True, 'hazmat': False, 'weight': (6000, 20000), 'value': (150000, 800000)}
]

FIRST_NAMES = ['James', 'John', 'Robert', 'Michael', 'William', 'David', 'Richard', 'Joseph', 'Thomas', 'Charles',
               'Mary', 'Patricia', 'Jennifer', 'Linda', 'Barbara', 'Susan', 'Jessica', 'Sarah', 'Karen', 'Nancy',
               'Daniel', 'Matthew', 'Anthony', 'Mark', 'Donald', 'Steven', 'Paul', 'Andrew', 'Joshua', 'Kenneth',
               'Lisa', 'Betty', 'Margaret', 'Sandra', 'Ashley', 'Dorothy', 'Kimberly', 'Emily', 'Donna', 'Michelle']

LAST_NAMES = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez',
              'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin',
              'Lee', 'Perez', 'Thompson', 'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson']

INCIDENT_TYPES = [
    {'type': 'Engine Overheat', 'severity': 'High', 'dur_min': 60},
    {'type': 'Brake Issue', 'severity': 'High', 'dur_min': 45},
    {'type': 'Tire Failure', 'severity': 'Medium', 'dur_min': 90},
    {'type': 'Electrical Fault', 'severity': 'Medium', 'dur_min': 30},
    {'type': 'Sensor Error', 'severity': 'Low', 'dur_min': 20},
    {'type': 'Traffic Delay', 'severity': 'Low', 'dur_min': 40},
    {'type': 'Weather Delay', 'severity': 'Medium', 'dur_min': 60},
    {'type': 'Road Closure', 'severity': 'Medium', 'dur_min': 50},
    {'type': 'Mechanical Issue', 'severity': 'High', 'dur_min': 75},
    {'type': 'Inspection Checkpoint', 'severity': 'Low', 'dur_min': 25},
]

INCIDENT_FILES = [
    '/Incident_Truck001.mp4',
    '/Incident_Truck002.mp4',
    '/Incident_Truck003.mp4',
    '/Incident_Truck004.mp4',
    '/Incident_Truck005.mp4',
]

# ================================================================
# HELPER FUNCTIONS
# ================================================================

def haversine(lat1, lon1, lat2, lon2):
    R = 3958.8  # miles
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi, dlambda = np.radians(lat2 - lat1), np.radians(lon2 - lon1)
    a = np.sin(dphi / 2)**2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2)**2
    return 2 * R * np.arctan2(np.sqrt(a), np.sqrt(1 - a))


def generate_truck_profile(truck_index):
    """Generate a consistent truck + driver profile"""
    rng = random.Random(truck_index * 7919)  # Prime seed for uniqueness

    make = rng.choice(list(TRUCK_MAKES.keys()))
    model = rng.choice(TRUCK_MAKES[make])
    year = rng.randint(2018, 2024)
    odometer = rng.randint(50000, 450000)
    fuel_cap = rng.randint(200, 300)

    first = rng.choice(FIRST_NAMES)
    last = rng.choice(LAST_NAMES)

    load = rng.choice(LOAD_TYPES)
    weight = rng.randint(load['weight'][0], load['weight'][1])
    value = rng.randint(load['value'][0], load['value'][1])

    return {
        'truck_id': f"TRK-{truck_index:03d}",
        'truck_make': make,
        'truck_model': model,
        'truck_year': year,
        'odometer_miles': odometer,
        'fuel_capacity_gallons': fuel_cap,
        'driver_name': f"{first} {last}",
        'load_type': load['type'],
        'load_weight_lbs': weight,
        'load_value_usd': value,
        'is_temperature_controlled': load['temp'],
        'is_hazmat': load['hazmat']
    }


def generate_incident_config(truck_index):
    """Generate incident location and type for a truck"""
    rng = random.Random(truck_index * 3571)

    # ~30% of trucks get an incident (about 12 of 40)
    if rng.random() > 0.30:
        return None

    incident = rng.choice(INCIDENT_TYPES)
    incident_pct = rng.uniform(0.05, 0.85)  # Where along route the incident occurs

    return {
        'pct': incident_pct,
        'dur_min': incident['dur_min'],
        'type': incident['type'],
        'severity': incident['severity']
    }


def generate_maintenance_baseline(profile):
    """Generate per-truck baseline maintenance values (starting conditions).
    These get degraded per-row based on route progress."""
    rng = random.Random(hash(profile['truck_id']) % 100000)
    odometer = profile['odometer_miles']
    truck_year = profile['truck_year']

    service_interval = rng.randint(15000, 25000)
    miles_since = rng.randint(500, int(service_interval * 1.5))
    last_service_mileage = odometer - miles_since

    days_since = max(1, int(miles_since / rng.randint(400, 600)))
    last_service_date = (datetime.now() - timedelta(days=days_since)).strftime('%Y-%m-%d')

    age_factor = max(0.6, 1.0 - (2025 - truck_year) * 0.03)

    # Starting component health (will degrade with progress)
    oil_start = round(max(20, min(100, rng.uniform(50, 100))), 1)
    brake_start = round(max(20, min(100, rng.uniform(50, 100) * age_factor)), 1)
    tire_start = round(max(20, min(100, rng.uniform(50, 100) * age_factor)), 1)
    trans_start = round(max(50, min(100, rng.uniform(70, 100) * age_factor)), 1)

    # Next service due in 200-800 miles at start (in 100s)
    next_service_due_start = rng.randint(2, 8) * 100

    return {
        'last_service_date': last_service_date,
        'last_service_mileage': last_service_mileage,
        'service_interval': service_interval,
        'miles_since_start': miles_since,
        'next_service_due_start': next_service_due_start,
        'oil_start': oil_start,
        'brake_start': brake_start,
        'tire_start': tire_start,
        'trans_start': trans_start,
        'age_factor': age_factor
    }


def compute_maintenance_row(baseline, progress_pct, total_route_miles):
    """Compute maintenance values for a single row based on route progress.
    Components degrade as the truck drives further. Priority escalates.
    Repair cost increases with severity."""

    # Miles driven on this trip so far
    trip_miles = total_route_miles * (progress_pct / 100.0)

    # Miles since service increases with trip
    miles_since = baseline['miles_since_start'] + int(trip_miles)

    # Next service due counts down by actual miles driven
    next_due = round(max(0, baseline['next_service_due_start'] - trip_miles), 1)

    # Component degradation: lose health as progress increases
    # Each component degrades at a slightly different rate
    # At 0% progress = starting health, at 100% progress = starting health minus degradation
    degrade = progress_pct / 100.0  # 0.0 to 1.0

    oil = round(max(5, baseline['oil_start'] - degrade * 35 + np.random.normal(0, 1)), 1)
    brake = round(max(5, baseline['brake_start'] - degrade * 25 + np.random.normal(0, 0.8)), 1)
    tire = round(max(8, baseline['tire_start'] - degrade * 20 + np.random.normal(0, 0.5)), 1)
    trans = round(max(30, baseline['trans_start'] - degrade * 15 + np.random.normal(0, 0.5)), 1)

    # Composite health score
    svc_factor = max(0.3, 1.0 - (miles_since / baseline['service_interval']) * 0.5)
    health = round(oil * 0.25 + brake * 0.25 + tire * 0.25 + trans * 0.15 + (svc_factor * 100) * 0.10, 1)

    # Priority escalates with progress
    if next_due == 0 or health < 40:
        priority = 'Critical'
    elif next_due <= 100 or health < 55:
        priority = 'Overdue'
    elif next_due <= 300 or health < 70:
        priority = 'Due Soon'
    else:
        priority = 'OK'

    # Repair cost scales with severity
    if priority == 'Critical':
        cost = int(5000 + (100 - health) * 100 + np.random.normal(0, 200))
    elif priority == 'Overdue':
        cost = int(2000 + (80 - health) * 50 + np.random.normal(0, 150))
    elif priority == 'Due Soon':
        cost = int(800 + (70 - health) * 30 + np.random.normal(0, 100))
    else:
        cost = int(200 + np.random.normal(0, 50))
    cost = max(100, cost)

    return {
        'last_service_date': baseline['last_service_date'],
        'last_service_mileage': baseline['last_service_mileage'],
        'miles_since_service': miles_since,
        'next_service_due_miles': next_due,
        'oil_life_pct': oil,
        'brake_pad_pct': brake,
        'tire_tread_pct': tire,
        'transmission_health_pct': trans,
        'engine_health_score': health,
        'estimated_repair_cost': cost,
        'maintenance_priority': priority
    }


def generate_hos_data(profile, current_time_dt, has_incident=False, status='In Transit'):
    """Generate HOS data that evolves with time"""
    rng = random.Random(hash(profile['truck_id']) % 100000)

    shift_hour = rng.randint(4, 9)
    shift_minute = rng.choice([0, 15, 30, 45])
    shift_start = f"{shift_hour:02d}:{shift_minute:02d}"

    current_hour = current_time_dt.hour + current_time_dt.minute / 60.0
    hours_elapsed = max(0, min(14, current_hour - shift_hour))

    drive_ratio = rng.uniform(0.65, 0.85)
    hours_driven = round(min(11, hours_elapsed * drive_ratio), 2)
    hours_on_duty = round(min(14, hours_elapsed), 2)

    if has_incident or status != 'In Transit':
        hours_driven = round(max(0, hours_driven - rng.uniform(0.5, 2)), 2)

    remaining_drive = round(max(0, 11 - hours_driven), 2)
    remaining_duty = round(max(0, 14 - hours_on_duty), 2)

    rest_hour = max(0, shift_hour - 10)
    last_rest = f"{rest_hour:02d}:00"

    next_break = round((8 - hours_driven) * 60) if hours_driven < 8 else 0

    consec_days = rng.randint(1, 7)
    avg_daily = rng.uniform(7, 10)
    driven_7day = round(min(60, (consec_days - 1) * avg_daily + hours_driven), 2)
    remaining_7day = round(max(0, 60 - driven_7day), 2)

    if hours_driven > 11 or hours_on_duty > 14 or driven_7day > 60:
        hos_status = 'Violation'
    elif remaining_drive < 2 or remaining_duty < 2 or remaining_7day < 5:
        hos_status = 'Warning'
    else:
        hos_status = 'Compliant'

    return {
        'shift_start_time': shift_start,
        'hours_driven_today': hours_driven,
        'hours_on_duty_today': hours_on_duty,
        'hours_remaining_drive': remaining_drive,
        'hours_remaining_duty': remaining_duty,
        'last_rest_start': last_rest,
        'next_mandatory_break_min': next_break,
        'hos_status': hos_status,
        'consecutive_days_driving': consec_days,
        'hours_driven_7day': driven_7day,
        'hos_7day_remaining': remaining_7day
    }


# ================================================================
# MAIN SIMULATION
# ================================================================

def generate_simulation():
    print("=" * 60)
    print("FLEET SIMULATION — 40 Trucks, 10 Routes")
    print("=" * 60)

    print(f"\nReading waypoints from: {WAYPOINTS_PATH}")
    try:
        waypoints_df = pd.read_csv(WAYPOINTS_PATH)
    except FileNotFoundError:
        print(f"Error: Could not find {WAYPOINTS_PATH}")
        return

    routes = sorted(waypoints_df['route_id'].unique())
    print(f"Found {len(routes)} routes: {routes}")

    if len(routes) < 10:
        print(f"Warning: Only {len(routes)} routes found. Trucks will be distributed across available routes.")

    # Assign 4 trucks per route
    all_rows = []
    truck_index = 1
    start_time_base = datetime(2026, 2, 2, 6, 0, 0)

    for route_idx, route_id in enumerate(routes):
        route_pts = waypoints_df[waypoints_df['route_id'] == route_id].sort_values('waypoint_index').reset_index(drop=True)

        if route_pts.empty or len(route_pts) < 20:
            print(f"  Skipping {route_id} — insufficient waypoints ({len(route_pts)})")
            continue

        num_points = len(route_pts)
        lats = route_pts['latitude'].values
        lons = route_pts['longitude'].values
        origin_city = route_pts.iloc[0]['origin_city']
        dest_city = route_pts.iloc[0]['destination_city']
        origin_lat = route_pts.iloc[0].get('origin_latitude', lats[0])
        origin_lon = route_pts.iloc[0].get('origin_longitude', lons[0])
        dest_lat = route_pts.iloc[0].get('destination_latitude', lats[-1])
        dest_lon = route_pts.iloc[0].get('destination_longitude', lons[-1])

        # Pre-compute segment distances
        dists = [0] + [haversine(lats[j], lons[j], lats[j+1], lons[j+1]) for j in range(num_points - 1)]
        total_dist = sum(dists)

        print(f"\n  Route {route_id}: {origin_city} -> {dest_city} ({num_points} waypoints, {total_dist:.0f} mi)")

        for t in range(TRUCKS_PER_ROUTE := 4):
            profile = generate_truck_profile(truck_index)
            incident_cfg = generate_incident_config(truck_index)
            maint_baseline = generate_maintenance_baseline(profile)

            # --- START POSITION LOGIC ---
            # Goal: 12 trucks at/near origin, 28 spread along routes
            #
            # Per route (4 trucks):
            #   t=0: At origin (0%)
            #   t=1: Spread out (15-25%)
            #   t=2: Mid-route (40-55%)
            #   t=3: Far along (70-85%)
            #
            # For first 2 routes, t=1 also starts near origin (3-6%)
            # giving us 10 + 2 = 12 trucks near origin
            
            rng_offset = random.Random(truck_index * 31)
            
            if t == 0:
                # Always at origin
                progress_pct = 0.0
            elif t == 1 and route_idx < 2:
                # First 2 routes: second truck also near origin
                progress_pct = rng_offset.uniform(0.03, 0.06)
            elif t == 1:
                # Other routes: second truck ~20% in
                progress_pct = rng_offset.uniform(0.15, 0.25)
            elif t == 2:
                progress_pct = rng_offset.uniform(0.40, 0.55)
            else:  # t == 3
                progress_pct = rng_offset.uniform(0.70, 0.85)
            
            # Start waypoint index based on progress
            start_offset = int(num_points * progress_pct)
            # Align to STEP_SIZE grid
            start_offset = (start_offset // STEP_SIZE) * STEP_SIZE
            
            # All trucks share the same time axis
            curr_time = start_time_base
            
            # Pre-compute distance traveled up to start_offset
            dist_traveled = sum(dists[0:start_offset + 1])

            target_inc_idx = int(num_points * incident_cfg['pct']) if incident_cfg else -1
            # If incident is behind this truck's start, move it ahead
            if incident_cfg and target_inc_idx < start_offset:
                target_inc_idx = start_offset + int((num_points - start_offset) * incident_cfg['pct'])
            file_assigned = False

            print(f"    {profile['truck_id']}: {profile['driver_name']}, start={progress_pct*100:.0f}%, "
                  f"incident={'Yes (' + incident_cfg['type'] + ')' if incident_cfg else 'None'}, "
                  f"next_svc={maint_baseline['next_service_due_start']}mi")

            for idx in range(start_offset, num_points, STEP_SIZE):
                seg_dist = sum(dists[max(0, idx - STEP_SIZE + 1): idx + 1])
                dist_traveled += seg_dist
                progress = (dist_traveled / total_dist) * 100 if total_dist > 0 else 100

                is_incident_here = (incident_cfg and
                                    idx <= target_inc_idx < idx + STEP_SIZE)

                # Telemetry
                speed = round(65.0 + np.random.normal(0, 2), 1) if not is_incident_here else 0.0
                rpm = int(1850 + np.random.normal(0, 20)) if speed > 0 else 650
                fuel = round(max(5, 98.0 - (progress * 0.7) + np.random.normal(0, 1)), 1)
                eng_temp = round(185 + np.random.normal(0, 1), 1)
                tire_psi = round(110 + np.random.normal(0, 0.5), 1)
                battery = round(13.8 + np.random.normal(0, 0.05), 2)
                status = "In Transit"

                # File columns (empty unless at incident)
                image_path = ''
                file_path = ''
                file_type = ''

                has_incident = False
                inc_type = ''
                inc_sev = ''
                at_incident = False

                # HOS data (evolves with time)
                hos_data = generate_hos_data(profile, curr_time, has_incident=False, status=status)

                # Maintenance data (evolves with progress)
                maint_data = compute_maintenance_row(maint_baseline, progress, total_dist)

                row = {
                    # --- ORIGINAL COLUMNS ---
                    'truck_id': profile['truck_id'],
                    'route_id': route_id,
                    'origin_city': origin_city,
                    'origin_latitude': origin_lat,
                    'origin_longitude': origin_lon,
                    'destination_city': dest_city,
                    'destination_latitude': dest_lat,
                    'destination_longitude': dest_lon,
                    'truck_make': profile['truck_make'],
                    'truck_model': profile['truck_model'],
                    'truck_year': profile['truck_year'],
                    'odometer_miles': profile['odometer_miles'],
                    'fuel_capacity_gallons': profile['fuel_capacity_gallons'],
                    'driver_name': profile['driver_name'],
                    'load_type': profile['load_type'],
                    'load_weight_lbs': profile['load_weight_lbs'],
                    'load_value_usd': profile['load_value_usd'],
                    'is_temperature_controlled': profile['is_temperature_controlled'],
                    'is_hazmat': profile['is_hazmat'],
                    'date': curr_time.strftime('%Y-%m-%d'),
                    'current_time': curr_time,
                    'current_latitude': lats[idx],
                    'current_longitude': lons[idx],
                    'waypoint_index': idx,
                    'total_waypoints': num_points,
                    'route_progress_percent': round(progress, 2),
                    'speed_mph': speed,
                    'engine_rpm': rpm,
                    'fuel_level_percent': fuel,
                    'engine_temp_f': eng_temp,
                    'tire_pressure_psi': tire_psi,
                    'battery_voltage': battery,
                    'distance_traveled_miles': round(dist_traveled, 2),
                    'distance_remaining_miles': round(max(0, total_dist - dist_traveled), 2),
                    'status': status,
                    'has_incident': has_incident,
                    'incident_type': inc_type,
                    'incident_severity': inc_sev,
                    'at_incident_location': at_incident,
                    '_ImagePath': image_path,
                    '_File': file_path,
                    'Filetype': file_type,
                    # --- MAINTENANCE ---
                    **maint_data,
                    # --- HOS ---
                    **hos_data
                }
                all_rows.append(row)
                curr_time += timedelta(seconds=INTERVAL_SECONDS)

                # Generate incident stationary rows
                if is_incident_here:
                    steps = (incident_cfg['dur_min'] * 60) // INTERVAL_SECONDS

                    # Assign file to first incident row
                    file_idx = (truck_index - 1) % len(INCIDENT_FILES)

                    for s in range(steps):
                        hos_inc = generate_hos_data(profile, curr_time, has_incident=True,
                                                     status=f"Stationary ({incident_cfg['type']})")
                        inc_row = row.copy()
                        inc_row.update({
                            'current_time': curr_time,
                            'speed_mph': 0.0,
                            'engine_rpm': 650,
                            'engine_temp_f': round(210 + np.random.normal(0, 5), 1) if 'Overheat' in incident_cfg['type'] else eng_temp,
                            'status': f"Stationary ({incident_cfg['type']})",
                            'has_incident': True,
                            'incident_type': incident_cfg['type'],
                            'incident_severity': incident_cfg['severity'],
                            'at_incident_location': True,
                            '_ImagePath': '',
                            '_File': INCIDENT_FILES[file_idx] if s == 0 else '',
                            'Filetype': 'mp4' if s == 0 else '',
                        })
                        inc_row.update(hos_inc)
                        all_rows.append(inc_row)
                        curr_time += timedelta(seconds=INTERVAL_SECONDS)

            truck_index += 1

    if not all_rows:
        print("\nError: No rows generated. Check route IDs in waypoints file.")
        return

    # --- FINALIZE ---
    print("\n\nFinalizing...")
    final_df = pd.DataFrame(all_rows)

    # Sort interleaved by time so all trucks move together
    final_df = final_df.sort_values(by=['current_time', 'truck_id']).reset_index(drop=True)

    # Format time
    final_df['current_time'] = final_df['current_time'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Save
    final_df.to_csv(OUTPUT_PATH, index=False)

    print(f"\n{'=' * 60}")
    print(f"SUMMARY")
    print(f"{'=' * 60}")
    print(f"Total trucks: {truck_index - 1}")
    print(f"Total rows: {len(final_df)}")
    print(f"Total columns: {len(final_df.columns)}")
    print(f"Output: {OUTPUT_PATH}")

    print(f"\nRows per truck:")
    print(final_df.groupby('truck_id').size().to_string())

    print(f"\nIncident trucks:")
    inc_trucks = final_df[final_df['has_incident'] == True]['truck_id'].unique()
    print(f"  {len(inc_trucks)} trucks with incidents: {list(inc_trucks)}")

    print(f"\nMaintenance priority at START of routes:")
    first_rows = final_df.sort_values('current_time').drop_duplicates(subset='truck_id', keep='first')
    print(first_rows['maintenance_priority'].value_counts().to_string())

    print(f"\nMaintenance priority at END of routes:")
    latest = final_df.sort_values('current_time').drop_duplicates(subset='truck_id', keep='last')
    print(latest['maintenance_priority'].value_counts().to_string())

    print(f"\nNext service due miles range:")
    print(f"  Min: {final_df['next_service_due_miles'].min()}")
    print(f"  Max: {final_df['next_service_due_miles'].max()}")

    print(f"\nHOS status (at latest time per truck):")
    print(latest['hos_status'].value_counts().to_string())

    print(f"\nColumn order verification:")
    for i, col in enumerate(final_df.columns):
        print(f"  {i:2d}. {col}")


if __name__ == "__main__":
    generate_simulation()
