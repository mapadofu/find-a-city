import dataset
import urllib.parse
import os

# URL for obtaining the station data
_source="https://www1.ncdc.noaa.gov/pub/data/normals/1981-2010/station-inventories/allstations.txt"


db_url = '/'.join(['sqlite:/', '', os.path.expanduser('~'), '.local', 'ncei.db'])

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
    }
    return record

def parse_file(fname):
    ''' Read the station records from a file '''
    with open(fname, 'r') as fin:
        for line in fin:
            yield parse_line(line)


def populate_db(records):
    with dataset.connect(db_url) as db:
        if 'stations' not in db:
            tbl = db.create_table('stations', primary_id='id', primary_type=db.types.string(11))
        else:
            tbl = db['stations']
        for r in records:
            tbl.insert(r)

def db():
    return dataset.connect(db_url)


