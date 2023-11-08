"""
Parse Hilltop XML data for Sensor locations, Sensor measuremnets and then create a feature class in a geodatabase
"""
# ---------------------------------------------------------------------------------------------------------------------
__author__ = "William Hamilton"
__python__ = ""
__created__ = "5/11/23"
__copyright__ = "Copyright © 2023~"
__license__ = ""
__ToDo__ = """
- nothing to add just yet
"""
import requests
import xml.etree.ElementTree as ET
import pandas as pd
import arcpy
import os

# URL to fetch data
URL_BASE = "https://hilltop.gw.govt.nz/"
SITE_URL = "data.hts?Service=Hilltop&Request=SiteList&Location=LatLong"
SITE_SENSORS_URL = "data.hts?Service=Hilltop&Request=MeasurementList&Site="

GEODATABASE_PATH = r"C:\Users\williamh\Documents\ArcGIS\Projects\Ass2\Ass2.gdb"
PROJECT_PATH = r"C:\Users\williamh\Documents\ArcGIS\Projects\Ass2\Ass2.aprx"
FEATURE_CLASS_NAME = 'Sensor_Locations'
TABLE_NAME = 'Sensor_Locations_Table'
SPATIAL_REFERENCE = arcpy.SpatialReference(2193)
# SPATIAL_REFERENCE_LATLONG = arcpy.SpatialReference(4326)
SPATIAL_REFERENCE_LATLONG = 'GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]];-400 -400 1000000000;-100000 10000;-100000 10000;8.98315284119521E-09;0.001;0.001;IsHighPrecision'

SENSOR_FILE = 'sensors.csv'
LOCATION_FILE = 'locations.csv'

# DEFINITION QUERIES
RAINFALL_QUERY = "Measurement_Name = 'Rainfall'"
DERIVED_FLOW_QUERY = "Measurement_Name = 'Derived Flow'"

def fetch_xml_data_from_url(url):
    response = requests.get(url)

    if response.status_code == 200:
        try:
            root = ET.fromstring(response.text)
            return root
        except ET.ParseError:
            print("Failed to parse XML data.")
    else:
        print("Failed to fetch data from the URL.")

    return None


def parse_site_locations(root):
    location_list = []

    for site in root.findall('.//Site'):
        name = site.get('Name')
        latitude_element = site.find('Latitude')
        longitude_element = site.find('Longitude')

        if latitude_element is not None and longitude_element is not None:
            latitude = float(latitude_element.text)
            longitude = float(longitude_element.text)
            location_info = {'Name': name, 'Latitude': latitude, 'Longitude': longitude}
            location_list.append(location_info)

    location_df = pd.DataFrame(location_list)
    return location_df


def get_measurement_names_for_sites(location_df):
    measurement_names = []

    for site_name in location_df['Name']:
        measure_url = f"{URL_BASE}{SITE_SENSORS_URL}{site_name}"
        response_root = fetch_xml_data_from_url(measure_url)

        if response_root is not None:
            measurement_element = response_root.find('.//DataSource/Measurement[@Name]')
            # measurement_element = response_root.find('.//Measurement[@Name]')
            if measurement_element is not None:
                measurement_name = measurement_element.get('Name')
                measurement_names.append(measurement_name)
                print(f"Measurement Name: {site_name}: {measurement_name}")
            else:
                measurement_names.append(None)  # Measurement Name not found
        else:
            measurement_names.append(None)  # Handle failed requests

    location_df['Measurement_Name'] = measurement_names

    return location_df


def get_measurement_name_for_single_site(site_name):
    measure_url = f"{URL_BASE}{SITE_SENSORS_URL}{site_name}"
    response_root = fetch_xml_data_from_url(measure_url)

    if response_root is not None:
        # Find the first Measurement element within the DataSource element
        measurement_element = response_root.find('.//DataSource/Measurement[@Name]')
        if measurement_element is not None:
            measurement_name = measurement_element.get('Name')
            print(f"Measurement Name found for site {site_name}: {measurement_name}")
            return measurement_name
        else:
            print(f"Measurement Name not found for site {site_name}")

    return None


def dataframe_to_feature_class(dataframe, geodatabase_path, feature_class_name, spatial_reference,
                              overwrite_existing=True):
    # Ensure the workspace is set to the geodatabase path
    arcpy.env.workspace = geodatabase_path

    # Check if the feature class already exists
    feature_class_path = os.path.join(geodatabase_path, feature_class_name)
    if arcpy.Exists(feature_class_path):
        if overwrite_existing:
            # Overwrite the existing feature class
            arcpy.management.Delete(feature_class_path)
            print(f"Feature class '{feature_class_name}' already exists in '{geodatabase_path}' and will be overwritten.")
        else:
            print(f"Feature class '{feature_class_name}' already exists in '{geodatabase_path}' and will not be overwritten.")
            return

    # Create a feature class with the specified spatial reference
    arcpy.management.CreateFeatureclass(geodatabase_path, feature_class_name, "POINT",
                                       spatial_reference=spatial_reference)

    # Get a list of existing field names
    existing_fields = [field.name for field in arcpy.ListFields(feature_class_path)]

    # Add fields to the feature class
    field_names = dataframe.columns.tolist()
    for field_name in field_names:
        if field_name not in existing_fields:
            if field_name == "Name":
                arcpy.management.AddField(feature_class_path, field_name, "TEXT")
            elif field_name == "Measurement_Name":
                arcpy.management.AddField(feature_class_path, field_name, "TEXT")
            else:
                arcpy.management.AddField(feature_class_path, field_name, "DOUBLE")
        else:
            print(f"Field '{field_name}' already exists in the feature class.")

    # Insert data from the DataFrame into the feature class
    with arcpy.da.InsertCursor(feature_class_path, field_names) as cursor:
        for row in dataframe.itertuples(index=False):
            cursor.insertRow(row)

    print(f"Feature class '{feature_class_name}' created successfully in '{geodatabase_path}'.")


def set_coordinate_system(geodatabase_path, feature_class_name, spatial_reference):
    # Set the coordinate system for the feature class
    arcpy.management.DefineProjection(os.path.join(geodatabase_path, feature_class_name), spatial_reference)
    print(f"Coordinate system set for '{feature_class_name}' in '{geodatabase_path}'.")



def create_table_from_dataframe(dataframe, geodatabase_path, table_name, overwrite_existing=True):
    print(f"Creating table '{table_name}' in '{geodatabase_path}'.")
    table_path = os.path.join(geodatabase_path, table_name)

    # Check if the table already exists
    if arcpy.Exists(table_path):
        if overwrite_existing:
            # Overwrite the existing table
            arcpy.management.Delete(table_path)
            print(f"Table '{table_name}' already exists in '{geodatabase_path}' and will be overwritten.")
        else:
            print(f"Table '{table_name}' already exists in '{geodatabase_path}' and will not be overwritten.")
            return

    # Create an empty table
    arcpy.management.CreateTable(geodatabase_path, table_name)
    print(f"Table '{table_name}' created successfully in '{geodatabase_path}'.")

    # Create fields based on DataFrame column names and data types
    field_info = [(col, 'TEXT') if dtype == 'object' else (col, 'DOUBLE') for col, dtype in zip(dataframe.columns, dataframe.dtypes)]

    for field_name, field_type in field_info:
        arcpy.management.AddField(table_name, field_name, field_type)

    # Create an insert cursor and populate the table with data from the DataFrame
    insert_cursor = arcpy.da.InsertCursor(table_name, [field_name for field_name, _ in field_info])

    for _, row in dataframe.iterrows():
        values = [row[field_name] for field_name, _ in field_info]
        insert_cursor.insertRow(values)

    # Clean up cursor
    del insert_cursor

    print(f"Data from DataFrame added to table '{table_name}' in '{geodatabase_path}'.")


def create_feature_class_from_table(table_path, feature_class_name, spatial_reference, overwrite_existing=True):
    print(f"Creating feature class '{feature_class_name}'.")

    # Check if the feature class already exists
    if arcpy.Exists(feature_class_name):
        if overwrite_existing:
            arcpy.management.Delete(feature_class_name)
            print(f"Feature class '{feature_class_name}' already exists and will be overwritten.")
        else:
            print(f"Feature class '{feature_class_name}' already exists and will not be overwritten.")
            return

    # Check the path to the table
    print(f"Checking the path to the table '{table_path}'.")
    if not arcpy.Exists(table_path):
        print(f"Table '{table_path}' does not exist. Ensure the correct path to the table is provided.")
        return

    # Use XYTableToPoint to create a feature class with point geometries
    print(f"Creating feature class '{feature_class_name}'. with XYTable thingy")
    arcpy.management.XYTableToPoint(
        in_table=table_path,
        out_feature_class=feature_class_name,
        x_field="Longitude",
        y_field="Latitude",
        z_field=None,
        coordinate_system=spatial_reference
        # coordinate_system = arcpy.SpatialReference(4326)
        # coordinate_system = SPATIAL_REFERENCE_LATLONG

    )

    # Check if the feature class was created successfully
    if arcpy.Exists(feature_class_name):
        print(f"Feature class '{feature_class_name}' created successfully.")
    else:
        print(f"Failed to create feature class '{feature_class_name}'.")

    # Define the spatial reference for the feature class
    print('Define Projection thingy ')
    if arcpy.Exists(feature_class_name):
        arcpy.management.DefineProjection(feature_class_name, spatial_reference)
        print(f"Spatial reference defined for '{feature_class_name}'.")
    else:
        print(f"Failed to define spatial reference for '{feature_class_name}'.")


import arcpy
import os

def add_feature_class_to_map(project_path, feature_class_name, feature_layer_name, overwrite_existing=True):
    try:
        # Open the ArcGIS project
        aprx = arcpy.mp.ArcGISProject(project_path)

        # Get the first map in the project
        map_obj = aprx.listMaps()[0]

        # Check if a layer with the same name already exists in the map
        existing_layers = map_obj.listLayers(feature_layer_name)

        if existing_layers and overwrite_existing:
            # Remove the existing layer
            map_obj.removeLayer(existing_layers[0])

        # Add the feature class to the map
        feature_class = os.path.join(arcpy.env.workspace, feature_class_name)
        feature_layer = map_obj.addDataFromPath(feature_class)
        feature_layer.name = feature_layer_name

        # Save the project
        aprx.save()

        print(f"Feature class '{feature_class_name}' added to the map as '{feature_layer_name}' in '{project_path}'.")

        # Close the project
        aprx = None
    except Exception as e:
        print(f"Failed to add the feature class to the map. Error: {str(e)}")




def main():
    arcpy.env.workspace = GEODATABASE_PATH
    if os.path.exists(SENSOR_FILE):
        print(f"The file {SENSOR_FILE} exists.")
        location_df = pd.read_csv(SENSOR_FILE)
    else:
        print(f"The file {SENSOR_FILE} does not exist.")
        root = fetch_xml_data_from_url(URL_BASE + SITE_URL)
        location_df = parse_site_locations(root)
        print(location_df.head())
        if os.path.exists(LOCATION_FILE):
            print(f"The file {LOCATION_FILE} exists.")
        #     delete the file
            os.remove(LOCATION_FILE)
        location_df.to_csv(LOCATION_FILE, index=False)
        location_df = get_measurement_names_for_sites(location_df)
        print(location_df.head())
        location_df.to_csv(SENSOR_FILE, index=False)

    print(location_df.head())
    column_headers = list(location_df.columns.values)
    print("The Column Header :", column_headers)
    # dataframe_to_feature_class(location_df, GEODATABASE_PATH, FEATURE_CLASS_NAME, SPATIAL_REFERENCE)
    # set_coordinate_system(GEODATABASE_PATH, FEATURE_CLASS_NAME, SPATIAL_REFERENCE)
    # dataframe_to_table(location_df, GEODATABASE_PATH, FEATURE_CLASS_NAME)
    create_table_from_dataframe(location_df, GEODATABASE_PATH, TABLE_NAME)
    table_path = os.path.join(GEODATABASE_PATH, TABLE_NAME)
    create_feature_class_from_table(TABLE_NAME, FEATURE_CLASS_NAME, SPATIAL_REFERENCE_LATLONG)
    # add the created featureclass to the map
    add_feature_class_to_map(PROJECT_PATH, FEATURE_CLASS_NAME, FEATURE_CLASS_NAME)





if __name__ == "__main__":
    print('\nStarting Processing')
    main()
    print('\nTerminating')


    '''
    # create points from table
    flight_points = arcpy.management.XYTableToPoint(
        in_table=flight_table,
        # out_feature_class="C:\Admin - Local\Work\Drone\Flight_20230702\FlightProject_20230702\Default.gdb\flight_table_XYTableToPoint",
        out_feature_class="memory\\flight_table_XYTableToPoint",
        x_field="SensorLongitude",
        y_field="SensorLatitude",
        z_field=None,
        coordinate_system='GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]];-400 -400 1000000000;-100000 10000;-100000 10000;8.98315284119521E-09;0.001;0.001;IsHighPrecision'

    )
    '''
