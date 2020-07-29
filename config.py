"""Config."""
from bottle import sys
import os

## EDIT
# SNCF API
TOKEN_AUTH = '920ef78f-4884-4327-b2fa-0015c3377b57'

## EDIT
# DB
HOST = 'localhost'
PORT = 3306
USER = 'root'
PWD = '1708'
DB = 'vacance'
SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://{}:{}@{}/{}?charset=utf8mb4'.format(
    USER, PWD, HOST, DB)

# PATH
DIRNAME = os.path.dirname(sys.argv[0])+os.sep
CSV_PATH = DIRNAME+'static'+os.sep+'csv'+os.sep
IMG_PATH = DIRNAME+'img'+os.sep
DAL_PATH = DIRNAME+'DAL'+os.sep

