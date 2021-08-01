'''
    Supports both `dataset` ("ez") access and sqlite3 access to the database file. 
    
'''
import dataset
import sqlite3
import xdg

def sql_connection():
    ''' Returns: as sqlite3 connection object to the database 
        (an also be used to create the DB file)
    '''
    return sqlite3.connect(_db_file)

def ez_connection():
    ''' Return a dataset (easy-to-use) connection to the NCEI data database '''
    return dataset.connect('sqlite:///'+_db_file)

def ensure_db():
    ''' Make sure the database exists '''
    c = sql_connection()
    c.close()
    return True

_db_file = str(xdg.xdg_data_home())+'/findacity.db'
