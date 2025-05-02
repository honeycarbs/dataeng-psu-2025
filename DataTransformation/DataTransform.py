import pandas as pd

def calculate_speed(df):
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
    
    df = df.sort_values(['EVENT_NO_TRIP', 'TIMESTAMP'])
    
    df['dMETERS'] = df.groupby('EVENT_NO_TRIP')['METERS'].diff()
    df['dTIMESTAMP'] = df.groupby('EVENT_NO_TRIP')['TIMESTAMP'].diff().dt.total_seconds()

    def calculate_speed(row):
        if pd.isna(row['dMETERS']) or pd.isna(row['dTIMESTAMP']):
            return 0.0
        if row['dTIMESTAMP'] <= 0:
            return 0.0
        return row['dMETERS'] / row['dTIMESTAMP']
    
    df['SPEED'] = df.apply(calculate_speed, axis=1)
    
    return df.drop(columns=['dMETERS', 'dTIMESTAMP'])

def analyze_vehicle_speeds(df, vehicle_id, date):
    """
    Analyzes speed patterns for a specific vehicle on a specific date.
    
    Parameters:
        df (pd.DataFrame): DataFrame containing vehicle data
        vehicle_id (int/str): Vehicle ID to analyze (e.g., 4223)
        date (str): Date in 'YYYY-MM-DD' format (e.g., '2023-02-15')
        
    Returns:
        dict: Dictionary containing speed analysis results
    """
    # Filter for the specific vehicle and date
    date_filter = pd.to_datetime(date).normalize()
    vehicle_df = df[
        (df['VEHICLE_ID'] == vehicle_id) &
        (df['TIMESTAMP'].dt.normalize() == date_filter)
    ].copy()
    
    if vehicle_df.empty:
        return {"error": f"No data found for vehicle {vehicle_id} on {date}"}
    
    # Calculate speeds (using our previous function)
    vehicle_df = calculate_speed(vehicle_df)
    
    # Find maximum speed occurrence
    max_speed = vehicle_df['SPEED'].max()
    max_speed_record = vehicle_df[vehicle_df['SPEED'] == max_speed].iloc[0]
    
    # Calculate median speed
    median_speed = vehicle_df['SPEED'].median()
    
    return {
        "vehicle_id": vehicle_id,
        "date": date,
        "max_speed": max_speed,
        "max_speed_location": (max_speed_record['GPS_LATITUDE'], max_speed_record['GPS_LONGITUDE']),
        "max_speed_time": max_speed_record['TIMESTAMP'].strftime('%H:%M:%S'),
        "median_speed": median_speed,
        "trip_count": vehicle_df['EVENT_NO_TRIP'].nunique(),
        "record_count": len(vehicle_df)
    }

def create_timestamp(df):
    
    def combine_datetime(row):
        fmt = '%d%b%Y:%H:%M:%S'
        base_date = pd.to_datetime(row['OPD_DATE'], format=fmt)
        time_delta = pd.to_timedelta(row['ACT_TIME'], unit='s')
        return base_date + time_delta
    
    df['TIMESTAMP'] = df.apply(combine_datetime, axis=1)
    cols_to_drop = ['OPD_DATE', 'ACT_TIME']
    df = df.drop(columns=cols_to_drop)

    return df


if __name__ == "__main__":
    filepath = 'DataTransformation/bc_veh4223_230215.csv'
    # fileath = 'DataTransformation/bc_trip259172515_230215.csv'

    cols_to_drop = ['EVENT_NO_STOP', 'GPS_SATELLITES', 'GPS_HDOP']

    """
    df = pd.read_csv(filepath)
    df = df.drop(columns=cols_to_drop)
    print(df)
    """

    """
    Reading the same thing again, but we need to determine what 
    fileds are we reading first
    """
    headers = pd.read_csv(filepath, nrows=0).columns.tolist()
    cols_to_use = [col for col in headers if col not in cols_to_drop]
    df = pd.read_csv(filepath, usecols=cols_to_use)

    df = create_timestamp(df)
    print(df)

    df = calculate_speed(df)
    print(df)

    results = analyze_vehicle_speeds(df, 4223, '2023-02-15')

    print("\nSpeed Analysis Results:")
    print(f"Vehicle: {results['vehicle_id']}")
    print(f"Date: {results['date']}")
    print(f"Maximum speed: {results['max_speed']:.2f} m/s")
    print(f"Location of max speed: {results['max_speed_location']}")
    print(f"Time of max speed: {results['max_speed_time']}")
    print(f"Median speed: {results['median_speed']:.2f} m/s")
    print(f"Number of trips: {results['trip_count']}")
    print(f"Number of breadcrumbs: {results['record_count']}")

    

