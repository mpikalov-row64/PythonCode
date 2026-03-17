import pandas as pd
import numpy as np
import requests
import json
import time
from typing import List, Tuple, Dict
import math
from datetime import datetime, timedelta
import random

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

# City coordinates (major cities in the route data)
CITY_COORDS = {
    'Atlanta': (33.7490, -84.3880),
    'Chicago': (41.8781, -87.6298),
    'Miami': (25.7617, -80.1918),
    'Los Angeles': (34.0522, -118.2437),
    'Portland': (45.5152, -122.6784),
    'Las Vegas': (36.1699, -115.1398),
    'Charlotte': (35.2271, -80.8431),
    'New York': (40.7128, -74.0060),
    'Denver': (39.7392, -104.9903),
    'Dallas': (32.7767, -96.7970),
    'Indianapolis': (39.7684, -86.1581),
    'Philadelphia': (39.9526, -75.1652)
}

# Truck fleet data
TRUCK_MAKES = ['Freightliner', 'Peterbilt', 'Kenworth', 'Volvo', 'Mack', 'International']
TRUCK_MODELS = {
    'Freightliner': ['Cascadia', 'M2 106', 'Columbia'],
    'Peterbilt': ['579', '389', '567'],
    'Kenworth': ['T680', 'W900', 'T880'],
    'Volvo': ['VNL 760', 'VNL 860', 'VHD'],
    'Mack': ['Anthem', 'Pinnacle', 'Granite'],
    'International': ['LT Series', 'HX Series', 'RH Series']
}

# Load types and characteristics
LOAD_TYPES = [
    {'type': 'Refrigerated Goods', 'temp_sensitive': True, 'hazmat': False, 'weight_range': (15000, 35000), 'value_range': (50000, 200000)},
    {'type': 'Electronics', 'temp_sensitive': False, 'hazmat': False, 'weight_range': (8000, 25000), 'value_range': (100000, 500000)},
    {'type': 'Pharmaceuticals', 'temp_sensitive': True, 'hazmat': False, 'weight_range': (5000, 15000), 'value_range': (200000, 1000000)},
    {'type': 'General Freight', 'temp_sensitive': False, 'hazmat': False, 'weight_range': (20000, 45000), 'value_range': (20000, 80000)},
    {'type': 'Automotive Parts', 'temp_sensitive': False, 'hazmat': False, 'weight_range': (15000, 40000), 'value_range': (50000, 150000)},
    {'type': 'Chemicals', 'temp_sensitive': False, 'hazmat': True, 'weight_range': (25000, 44000), 'value_range': (30000, 100000)},
    {'type': 'Food & Beverage', 'temp_sensitive': True, 'hazmat': False, 'weight_range': (18000, 42000), 'value_range': (25000, 75000)},
    {'type': 'Building Materials', 'temp_sensitive': False, 'hazmat': False, 'weight_range': (30000, 45000), 'value_range': (15000, 50000)},
    {'type': 'Consumer Goods', 'temp_sensitive': False, 'hazmat': False, 'weight_range': (12000, 35000), 'value_range': (40000, 120000)},
    {'type': 'Medical Equipment', 'temp_sensitive': True, 'hazmat': False, 'weight_range': (6000, 20000), 'value_range': (150000, 800000)}
]

# Driver names (first and last)
FIRST_NAMES = ['James', 'John', 'Robert', 'Michael', 'William', 'David', 'Richard', 'Joseph', 'Thomas', 'Charles',
               'Mary', 'Patricia', 'Jennifer', 'Linda', 'Barbara', 'Susan', 'Jessica', 'Sarah', 'Karen', 'Nancy',
               'Daniel', 'Matthew', 'Anthony', 'Mark', 'Donald', 'Steven', 'Paul', 'Andrew', 'Joshua', 'Kenneth',
               'Lisa', 'Betty', 'Margaret', 'Sandra', 'Ashley', 'Dorothy', 'Kimberly', 'Emily', 'Donna', 'Michelle']

LAST_NAMES = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez',
              'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin',
              'Lee', 'Perez', 'Thompson', 'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson']

# Incident types and severities
INCIDENT_TYPES = [
    {'type': 'Traffic Delay', 'severity': 'Low', 'delay_minutes': (15, 60)},
    {'type': 'Weather Delay', 'severity': 'Medium', 'delay_minutes': (30, 180)},
    {'type': 'Mechanical Issue', 'severity': 'Medium', 'delay_minutes': (45, 240)},
    {'type': 'Tire Failure', 'severity': 'Medium', 'delay_minutes': (60, 180)},
    {'type': 'Minor Accident', 'severity': 'High', 'delay_minutes': (120, 360)},
    {'type': 'Road Closure', 'severity': 'Medium', 'delay_minutes': (45, 150)},
    {'type': 'Fuel Stop', 'severity': 'Low', 'delay_minutes': (20, 45)},
    {'type': 'Rest Break', 'severity': 'Low', 'delay_minutes': (30, 60)},
    {'type': 'Inspection Checkpoint', 'severity': 'Low', 'delay_minutes': (15, 45)},
    {'type': 'Loading Issue', 'severity': 'Medium', 'delay_minutes': (30, 120)}
]

def generate_truck_id(index):
    """Generate truck ID like TRK-001, TRK-002, etc."""
    return f"TRK-{index:03d}"

def generate_driver():
    """Generate driver profile"""
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    years_exp = random.randint(2, 25)
    age = random.randint(25, 65)
    
    # Performance metrics
    safety_score = round(random.uniform(75, 99.5), 1)
    on_time_delivery_rate = round(random.uniform(85, 99), 1)
    
    # License class (most truck drivers have Class A)
    license_class = random.choices(['Class A CDL', 'Class B CDL'], weights=[0.85, 0.15])[0]
    
    return {
        'driver_name': f"{first} {last}",
        'driver_age': age,
        'years_experience': years_exp,
        'license_class': license_class,
        'safety_score': safety_score,
        'on_time_delivery_rate': on_time_delivery_rate
    }

def generate_truck():
    """Generate truck specifications"""
    make = random.choice(TRUCK_MAKES)
    model = random.choice(TRUCK_MODELS[make])
    year = random.randint(2018, 2024)
    odometer = random.randint(50000, 450000)
    
    # Fuel capacity in gallons
    fuel_capacity = random.randint(200, 300)
    
    return {
        'truck_make': make,
        'truck_model': model,
        'truck_year': year,
        'odometer_miles': odometer,
        'fuel_capacity_gallons': fuel_capacity
    }

def generate_load():
    """Generate load details"""
    load_info = random.choice(LOAD_TYPES)
    
    weight = random.randint(load_info['weight_range'][0], load_info['weight_range'][1])
    value = random.randint(load_info['value_range'][0], load_info['value_range'][1])
    
    return {
        'load_type': load_info['type'],
        'load_weight_lbs': weight,
        'load_value_usd': value,
        'is_temperature_controlled': load_info['temp_sensitive'],
        'is_hazmat': load_info['hazmat']
    }

def generate_telemetry(route_progress, has_incident=False, incident_type=None):
    """Generate realistic telemetry data"""
    
    # Base values
    base_speed = random.uniform(55, 70)
    base_fuel = random.uniform(30, 95)
    base_temp = random.uniform(65, 75)
    
    # Adjust based on incident
    if has_incident:
        if incident_type in ['Traffic Delay', 'Road Closure']:
            base_speed = random.uniform(0, 25)
        elif incident_type == 'Mechanical Issue':
            base_speed = 0
            base_temp = random.uniform(180, 220)  # Engine overheating
        elif incident_type in ['Fuel Stop', 'Rest Break']:
            base_speed = 0
    
    # Add some realistic variance
    speed = max(0, base_speed + random.uniform(-5, 5))
    fuel_level = max(5, min(100, base_fuel + random.uniform(-2, 2)))
    engine_temp = max(160, min(230, base_temp + random.uniform(-5, 5)))
    
    # RPM correlates with speed
    rpm = speed * random.uniform(25, 35) if speed > 0 else random.uniform(600, 800)
    
    # Tire pressure (normal range 100-120 PSI for trucks)
    tire_pressures = [random.randint(95, 120) for _ in range(6)]  # 6 tires
    
    # Battery voltage
    battery_voltage = round(random.uniform(12.4, 14.2), 1)
    
    # DEF (Diesel Exhaust Fluid) level
    def_level = round(random.uniform(20, 100), 1)
    
    return {
        'current_speed_mph': round(speed, 1),
        'fuel_level_percent': round(fuel_level, 1),
        'engine_temp_fahrenheit': round(engine_temp, 1),
        'engine_rpm': round(rpm, 0),
        'tire_pressure_avg_psi': round(sum(tire_pressures) / len(tire_pressures), 1),
        'battery_voltage': battery_voltage,
        'def_level_percent': def_level
    }
    """Calculate distance between two coordinates in km"""
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    
    R = 6371  # Earth's radius in km
    
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    
    return R * c

def interpolate_points(coord1: Tuple[float, float], coord2: Tuple[float, float], 
                       num_points: int) -> List[Tuple[float, float]]:
    """Linearly interpolate points between two coordinates"""
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    
    points = []
    for i in range(num_points):
        t = i / (num_points - 1) if num_points > 1 else 0
        lat = lat1 + (lat2 - lat1) * t
        lon = lon1 + (lon2 - lon1) * t
        points.append((lat, lon))
    
    return points

def get_route_osrm(start_coords: Tuple[float, float], end_coords: Tuple[float, float]) -> List[Tuple[float, float]]:
    """
    Get route coordinates using OSRM (Open Source Routing Machine)
    Free alternative to commercial routing services
    """
    # OSRM uses lon,lat format
    url = f"http://router.project-osrm.org/route/v1/driving/{start_coords[1]},{start_coords[0]};{end_coords[1]},{end_coords[0]}"
    params = {
        'overview': 'full',
        'geometries': 'geojson',
        'steps': 'true'
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data['code'] == 'Ok' and len(data['routes']) > 0:
                # Extract coordinates from the route geometry
                coordinates = data['routes'][0]['geometry']['coordinates']
                # Convert from [lon, lat] to (lat, lon)
                return [(coord[1], coord[0]) for coord in coordinates]
    except Exception as e:
        print(f"OSRM API error: {e}")
    
    return None
def haversine_distance(coord1: Tuple[float, float], coord2: Tuple[float, float]) -> float:
    """Calculate distance between two coordinates in km"""
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    
    R = 6371  # Earth's radius in km
    
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    
    return R * c

def densify_route(coordinates: List[Tuple[float, float]], target_spacing_km: float = 1.0) -> List[Dict]:
    """
    Add intermediate points to ensure no segment is longer than target_spacing_km
    Returns list of dicts with lat, lon, and segment_index
    """
    densified = []
    segment_index = 0
    
    for i in range(len(coordinates) - 1):
        start = coordinates[i]
        end = coordinates[i + 1]
        
        # Add the start point
        densified.append({
            'latitude': start[0],
            'longitude': start[1],
            'segment_index': segment_index
        })
        
        # Calculate distance
        distance = haversine_distance(start, end)
        
        # Determine how many intermediate points we need
        if distance > target_spacing_km:
            num_intermediate = int(distance / target_spacing_km)
            
            # Add intermediate points
            for j in range(1, num_intermediate + 1):
                t = j / (num_intermediate + 1)
                lat = start[0] + (end[0] - start[0]) * t
                lon = start[1] + (end[1] - start[1]) * t
                densified.append({
                    'latitude': lat,
                    'longitude': lon,
                    'segment_index': segment_index
                })
        
        segment_index += 1
    
    # Add the final point
    densified.append({
        'latitude': coordinates[-1][0],
        'longitude': coordinates[-1][1],
        'segment_index': segment_index
    })
    
    return densified

def generate_route_waypoints(route_id: str, origin_city: str, destination_city: str,
                            use_api: bool = True, spacing_km: float = 1.0) -> List[Dict]:
    """
    Generate fine-grained waypoints for a route
    """
    print(f"Processing {route_id}: {origin_city} -> {destination_city}")
    
    if origin_city not in CITY_COORDS or destination_city not in CITY_COORDS:
        print(f"  Warning: City coordinates not found, skipping")
        return []
    
    start_coords = CITY_COORDS[origin_city]
    end_coords = CITY_COORDS[destination_city]
    
    waypoints = []
    
    if use_api:
        # Try to get actual road route
        print(f"  Fetching route from OSRM...")
        route_coords = get_route_osrm(start_coords, end_coords)
        
        if route_coords:
            print(f"  Got {len(route_coords)} points from OSRM")
            # Densify the route
            waypoints = densify_route(route_coords, spacing_km)
        else:
            print(f"  API failed, using linear interpolation")
            use_api = False
    
    if not use_api:
        # Fallback to simple interpolation
        distance = haversine_distance(start_coords, end_coords)
        num_points = max(int(distance / spacing_km), 10)
        
        interpolated = interpolate_points(start_coords, end_coords, num_points)
        waypoints = [{
            'latitude': coord[0],
            'longitude': coord[1],
            'segment_index': i
        } for i, coord in enumerate(interpolated)]
    
    # Add route metadata to each waypoint INCLUDING city GPS coordinates
    for i, wp in enumerate(waypoints):
        wp['route_id'] = route_id
        wp['origin_city'] = origin_city
        wp['origin_latitude'] = start_coords[0]
        wp['origin_longitude'] = start_coords[1]
        wp['destination_city'] = destination_city
        wp['destination_latitude'] = end_coords[0]
        wp['destination_longitude'] = end_coords[1]
        wp['waypoint_index'] = i
        wp['total_waypoints'] = len(waypoints)
    
    print(f"  Generated {len(waypoints)} waypoints")
    return waypoints

def main():
    # Read the routes data from the actual file
    input_file = r"C:\Users\mikha\OneDrive\Documents\Demos\Fleet Management\routes.csv"
    
    try:
        routes = pd.read_csv(input_file)
        print(f"Successfully loaded routes from: {input_file}")
        print(f"Found {len(routes)} routes\n")
    except FileNotFoundError:
        print(f"Error: Could not find file at {input_file}")
        print("Using sample data instead...\n")
        routes = pd.DataFrame({
            'route_id': ['RTE0001', 'RTE0002', 'RTE0003', 'RTE0004', 'RTE0005', 'RTE0006', 
                         'RTE0007', 'RTE0008', 'RTE0009', 'RTE0010', 'RTE0011', 'RTE0012',
                         'RTE0013', 'RTE0014', 'RTE0015'],
            'origin_city': ['Atlanta', 'Chicago', 'Los Angeles', 'Dallas', 'Miami', 'Denver',
                           'Portland', 'Las Vegas', 'Charlotte', 'Indianapolis', 'Philadelphia',
                           'New York', 'Atlanta', 'Chicago', 'Denver'],
            'origin_state': ['GA', 'IL', 'CA', 'TX', 'FL', 'CO', 'OR', 'NV', 'NC', 'IN', 'PA',
                            'NY', 'GA', 'IL', 'CO'],
            'destination_city': ['Chicago', 'Los Angeles', 'Portland', 'New York', 'Atlanta', 'Las Vegas',
                                'Dallas', 'Philadelphia', 'Miami', 'Denver', 'Charlotte',
                                'Miami', 'Denver', 'Portland', 'Indianapolis'],
            'destination_state': ['IL', 'CA', 'OR', 'NY', 'GA', 'NV', 'TX', 'PA', 'FL', 'CO', 'NC',
                                 'FL', 'CO', 'OR', 'IN']
        })
    
    print("=" * 60)
    print("ROUTE WAYPOINT GENERATOR")
    print("=" * 60)
    print(f"\nProcessing {len(routes)} routes...")
    print(f"Target spacing: 1.0 km between waypoints\n")
    
    all_waypoints = []
    
    # Process each route
    for idx, row in routes.iterrows():
        waypoints = generate_route_waypoints(
            row['route_id'],
            row['origin_city'],
            row['destination_city'],
            use_api=True,  # Set to False to use only interpolation
            spacing_km=1.0  # Adjust this for more/fewer waypoints
        )
        all_waypoints.extend(waypoints)
        
        # Be nice to the free API
        if idx < len(routes) - 1:
            time.sleep(0.5)
    
    # Convert to DataFrame
    waypoints_df = pd.DataFrame(all_waypoints)
    
    # Reorder columns for better readability
    column_order = [
        'route_id',
        'origin_city', 'origin_latitude', 'origin_longitude',
        'destination_city', 'destination_latitude', 'destination_longitude',
        'waypoint_index', 'total_waypoints',
        'latitude', 'longitude', 'segment_index'
    ]
    waypoints_df = waypoints_df[column_order]
    
    # Save locally (will be provided to user for Windows machine)
    output_file = r"C:\Users\mikha\OneDrive\Documents\Demos\Fleet Management\route_waypoints_new.csv"
    waypoints_df.to_csv(output_file, index=False)
    
    print(f"\n{'=' * 60}")
    print(f"SUMMARY")
    print(f"{'=' * 60}")
    print(f"Total routes processed: {len(routes)}")
    print(f"Total waypoints generated: {len(waypoints_df)}")
    print(f"Average waypoints per route: {len(waypoints_df) / len(routes):.1f}")
    print(f"\nOutput saved to: {output_file}")
    
    # Show sample of the data
    print(f"\nSample waypoints:")
    print(waypoints_df.head(10))
    
    # Show statistics per route
    print(f"\nWaypoints per route:")
    print(waypoints_df.groupby('route_id').size().to_string())

if __name__ == "__main__":
    main()
