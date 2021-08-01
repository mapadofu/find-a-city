import sys
import requests

import db
from ncei import tmax_tbl 
from setup_stations import _populate_db


# URL for obtaining the station data
tmax_file_url = "https://www1.ncdc.noaa.gov/pub/data/normals/1981-2010/products/temperature/mly-tmax-normal.txt"

def _extract_temp(item):
    i = int(item[:-1])/10.0
    return i

month = dict([(i+1, v) for (i,v) in enumerate("JAN,FEB,MAR,APR,MAY,JUN,JUL,AUG,SEP,OCT,NOV,DEC".split(','))])


def parse_line(line):
    # lines are fixed-format
    parts = line.split()
    #parts[0] == the station ID

    bad = [x for x in parts[1:] if x[-1] not in 'CSRPQ']
    if bad:
        raise ValueError("Undefined value in max-temperature data {0}".format(line))

    items = [(i+1, _extract_temp(item)) for (i, item) in enumerate(parts[1:])]
    items = [(month[k], v) for (k,v) in items]

    record = dict(items)
    record['id'] = parts[0]
    
    return record

def populate_db(records):
    with db.ez_connection() as c:
        _populate_db(c, tmax_tbl, records)

def download_stations():
    ''' @retval generates the individual data rows, as text '''
    res = requests.get(tmax_file_url) 
    return (x for x in res.text.split('\n') if x)
    

def install_data():
    lines = download_stations()
    records = (parse_line(line) for line in lines)
    populate_db(records)

def check_data():
    ''' Returns True if it looks like the stations data have been installed '''
    tbl = db.ez_connection()[tmax_tbl]
    return len(tbl) == 7501

if __name__ == "__main__":
    install_data()
    if not check_data():
        print("Got {0} records, expected {1}".format(len(db.ez_connection()[tmax_tbl]), 7501))
        exit(1)

