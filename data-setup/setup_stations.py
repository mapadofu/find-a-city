import sys
import requests
import db
from ncei import stations_tbl



# URL for obtaining the station data
stations_file_url = "https://www1.ncdc.noaa.gov/pub/data/normals/1981-2010/station-inventories/allstations.txt"


def parse_line(line):
    # lines are fixed-format
    record = {
        'id':line[:11],
        'latitude':float(line[12:20].strip()),   # deg
        'longitude':float(line[21:30].strip()),
        'elevation':float(line[31:37].strip()),    # meters
        'state':line[38:40],
        'name':line[41:71].strip(),
        # there are some other fields that I don't understand/care about
	#'gsn':line[72:75].strip(), # TBR: what is this?
	#'hcn':line[76:79].strip(), # TBR: what is this?
	#'wmoid':line[80:85].strip(), # TBR: what is this?	
	#'method':line[86:99].strip(),
    }
    return record

def populate_db(records):
    with db.ez_connection() as c:
        _populate_db(c, stations_tbl, records)


def _populate_db(db, table, records):
    if table not in db:
        tbl = db.create_table(table, primary_id='id', primary_type=db.types.string(11))
        tbl_op = tbl.insert
    else:
        tbl = db[table]
        def tbl_op(r):
            tbl.update(r, r['id'])
    
    for r in records:
        tbl_op(r)


def parse_file(fname):
    ''' Read the station records from a file '''
    with open(fname, 'r') as fin:
        for line in fin:
            yield parse_line(line)

def download_stations():
    ''' @retval generates the individual data rows, as text '''
    res = requests.get(stations_file_url) 
    return (x for x in res.text.split('\n') if x)
    

def install_data():
    lines = download_stations()
    records = (parse_line(line) for line in lines)
    populate_db(records)

def check_data():
    ''' Returns True if it looks like the stations data have been installed '''
    tbl = db.ez_connection()[stations_tbl]
    return len(tbl) == 9887

if __name__ == "__main__":
    install_data()
    if not check_data():
        print("Got {0} records, expected {1}".format(len(ez_connection()[stations_tbl]), 9887))
        exit(1)

