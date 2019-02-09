import cx_Oracle
import pandas as pd
import json
from mapfuncs import mapfuncsfuncs as mf
import time


def getobiee():
    # date var used for saving file names
    date1 = time.strftime('%Y_%m_%d_')
    with open('obiee/obieeqs.json') as json_data:
        queries = json.load(json_data)


    ebs = {}
    for q in queries['queries']:
        print(q['name'])
        con = cx_Oracle.connect(user=q['username'], password=q['password'], dsn=q['dsn'])
        print(con.version)
        cursor = con.cursor()

        fd = open(q['query'], 'r')
        q_sql = fd.read()
        fd.close()

        cursor.execute(q_sql)