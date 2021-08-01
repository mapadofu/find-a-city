import sys, os
import argparse
import pandas

import dataclasses
import geopandas
import shapely.geometry as sg

import db

# epsg:2163 is reasonable for the continental us

def grab_table(month='AUG'):
    ''' Grab the data as a GeoDataFrame '''
    records = grab_records(month)
    points = [sg.Point(x.longitude, x.latitude) for x in records]
    table = geopandas.GeoDataFrame(records, geometry=points, crs='epsg:4326') 
    return table

def grab_records(month='AUG'):
    ''' Pull the summer temperature data, 
        @retval: List[SummerTemperature] 
    '''
    conn = db.sql_connection()
    cur = conn.cursor()
    #excluded = """('FL', 'GA', 'SC', 'AL', 'MS', 'TX', 'OK', 'AZ', 'CA', 'ND', 'SD', 'NE', 'MO', 'IA', 'KS', 'LA', 'AK', 'AR' )"""
    excluded = """('AK', 'HI')"""
    res = list(cur.execute(f"""SELECT latitude, longitude, elevation, state, name, {tmax_tbl}.{month}, {tmax_tbl}.id FROM {stations_tbl} JOIN {tmax_tbl} ON {tmax_tbl}.id=stations.id WHERE state NOT IN {excluded} AND latitude >25 AND latitude < 51 AND longitude > -140 AND longitude < -60""").fetchall())      
    return [SummerTemperature(*item) for item in res]
#_columns = 'latitude longitude elevation state name aug_temperature station'.split()


# Table names
stations_tbl = 'stations'
tmax_tbl = 'tmax'  

@dataclasses.dataclass
class SummerTemperature:
    latitude: float
    longitude: float
    elevation: float
    state: str
    name: str
    temperature:  float
    station: str

    @property
    def __geo_interface__(self):
        props = ['state', 'name', 'temperature', 'station']
        properties = {k:getattr(self, k) for k in props}
        properties['color'] = 'red'
        type_ = 'Feature'
        geometry = {'type':'Point', 'coordinates':(self.latitude, self.longitude, self.elevation)}
        return {'properties':properties, 'type':type_, 'geometry': geometry}




