# I write and test methods one at a time.
# This file contains unit tests of individual methods.

from src.CSVDataTable import CSVDataTable
import logging
import os
import json

# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.DEBUG)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = os.path.abspath("../Data/Baseball")


def t_load():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)

    print("Created table = " + str(csv_tbl))


def t_find_by_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    fields = ['playerID', 'teamID', 'yearID', 'AB', 'H', 'HR', 'RBI']
    tmp = {'teamID': 'BOS', 'yearID': '1960'}

    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)

    res = csv_tbl.find_by_template(template=tmp, field_list=fields)

    print("Query result = \n", json.dumps(res, indent=2))


def t_find_by_primary_key():
    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    fields = ['playerID', 'teamID', 'yearID', 'AB', 'H', 'HR', 'RBI']
    key_vals = {'willite01': 'BOS', '1960': '1'}

    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)

    res = csv_tbl.find_by_primary_key(key_vals)

    print("Query result = \n", json.dumps(res, indent=2))


def test_matches():
    r = {
        "playerID": "webstra01",
        "teamID": "BOS",
        "yearID": "1960",
        "AB": "3",
        "H": "0",
        "HR": "0",
        "RBI": "1"
    }
    tmp = {"playerID": "webstra01"}

    test = CSVDataTable.matches_template(r, tmp)
    print("Matches = ", test)


# t_load()
#
test_matches()
