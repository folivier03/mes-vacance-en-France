"""Config."""
from bottle import sys
import os

# EDIT
## SNCF API
TOKEN_AUTH = '0ce55a95-071c-479a-adef-7f7770aecd3f'

## DB
HOST = 'localhost'
PORT = 3306
USER = 'root'
PWD = '1708'
DB = 'vacation'
SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://{}:{}@{}/{}?charset=utf8mb4'.format(
    USER, PWD, HOST, DB)

## PATH
DIRNAME = os.path.dirname(sys.argv[0])+os.sep
CSV_PATH = DIRNAME+'static'+os.sep+'csv'+os.sep
IMG_PATH = DIRNAME+'img'+os.sep
DAL_PATH = DIRNAME+'DAL'+os.sep

