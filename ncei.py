import dataset
import sys, os
import argparse



def Connection():
    ''' Connection to the local NCEI database '''
    return dataset.connect(db_url)

db_url = '/'.join(['sqlite:/', '', os.path.expanduser('~'), '.local', 'ncei.db'])

stations_tbl = 'stations'
tmax_tbl = 'tmax'  # 




