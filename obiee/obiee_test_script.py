import cx_Oracle
import pandas as pd
import json
from mapfuncs import mapfuncsfuncs as mf
import time

with open('obieeqs_test.json') as json_data:
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
    column_names = [i[0] for i in cursor.description]
    q_output = pd.DataFrame(cursor.fetchall())
    q_output.columns = column_names
    cursor.close()
    con.close()
    # exporting entire obiee pull
    # dirpath = 'dataflow/0fullpulls/'
    q_output.to_csv('test_obieefull_' + q['destname'], index=False)
    # q_output.to_csv(dirpath + 'hist/' + date1 + 'obieefull_' + q['destname'], index=False)
    # taking only needed columns
    q_output = q_output.loc[:, q['finalcols']]
    ebs[q['name']] = q_output
    obiee = q_output

dtypeCount = [obiee.iloc[:, i].apply(type).value_counts() for i in range(obiee.shape[1])]
print(dtypeCount)
