import pandas as pd
import requests
import json
import time
from typing import List, Tuple, Dict
import math

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
                         'RTE0007', 'RTE0008', 'RTE0009', 'RTE0010', 'RTE0011'],
            'origin_city': ['Atlanta', 'Atlanta', 'Chicago', 'Chicago', 'Chicago', 'Chicago',
                           'Dallas', 'Dallas', 'Dallas', 'New York', 'New York'],
            'origin_state': ['GA', 'GA', 'IL', 'IL', 'IL', 'IL', 'TX', 'TX', 'TX', 'NY', 'NY'],
            'destination_city': ['Chicago', 'Miami', 'Los Angeles', 'Portland', 'Las Vegas', 'Charlotte',
                                'New York', 'Denver', 'Indianapolis', 'Philadelphia', 'Charlotte'],
            'destination_state': ['IL', 'FL', 'CA', 'OR', 'NV', 'NC', 'NY', 'CO', 'IN', 'PA', 'NC']
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
    output_file = r"C:\Users\mikha\OneDrive\Documents\Demos\Fleet Management\route_waypoints.csv"
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