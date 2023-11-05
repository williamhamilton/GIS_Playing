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
SPATIAL_REFERENCE = arcpy.SpatialReference(2193)


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

    location_df['Measurement Name'] = measurement_names

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
    if arcpy.Exists(os.path.join(geodatabase_path, feature_class_name)):
        if overwrite_existing:
            # Overwrite the existing feature class
            arcpy.management.Delete(os.path.join(geodatabase_path, feature_class_name))
            print(
                f"Feature class '{feature_class_name}' already exists in '{geodatabase_path}' and will be overwritten.")

    # Create a feature class with the specified spatial reference
    arcpy.management.CreateFeatureclass(geodatabase_path, feature_class_name, "POINT",
                                        spatial_reference=spatial_reference)

    # Add fields to the feature class (e.g., Latitude and Longitude fields)
    field_names = dataframe.columns.tolist()
    for field_name in field_names:
        arcpy.management.AddField(os.path.join(geodatabase_path, feature_class_name), field_name, "DOUBLE")

    # Insert data from the DataFrame into the feature class
    with arcpy.da.InsertCursor(os.path.join(geodatabase_path, feature_class_name), field_names) as cursor:
        for row in dataframe.itertuples(index=False):
            cursor.insertRow(row)

    print(f"Feature class '{feature_class_name}' created successfully in '{geodatabase_path}'.")


def set_coordinate_system(geodatabase_path, feature_class_name, spatial_reference):
    # Set the coordinate system for the feature class
    arcpy.management.DefineProjection(os.path.join(geodatabase_path, feature_class_name), spatial_reference)
    print(f"Coordinate system set for '{feature_class_name}' in '{geodatabase_path}'.")


def main():
    root = fetch_xml_data_from_url(URL_BASE + SITE_URL)
    location_df = parse_site_locations(root)
    print(location_df.head())
    location_df.to_csv('locations.csv', index=False)
    location_df= get_measurement_names_for_sites(location_df)
    print(location_df.head())
    location_df.to_csv('sensors.csv', index=False)
    dataframe_to_feature_class(location_df, GEODATABASE_PATH, FEATURE_CLASS_NAME, SPATIAL_REFERENCE)
    set_coordinate_system(GEODATABASE_PATH, FEATURE_CLASS_NAME, SPATIAL_REFERENCE)


    # if response.status_code == 200:
    #     root = parse_xml_data(response)
    #
    #     create_feature_class(geodatabase_path, feature_class_name)
    #     insert_data_into_feature_class(root, feature_class_name)
    #
    #     print(f"Feature class '{feature_class_name}' created successfully in '{geodatabase_path}'.")


if __name__ == "__main__":
    print('\nStarting Processing')
    main()
    print('\nTerminating')
