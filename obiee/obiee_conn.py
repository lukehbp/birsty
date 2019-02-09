import cx_Oracle
import pandas as pd
import json
import time
from mapfuncs import mapfuncsfuncs as mf


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
        column_names = [i[0] for i in cursor.description]
        q_output = pd.DataFrame(cursor.fetchall())
        q_output.columns = column_names
        cursor.close()
        con.close()
        # exporting entire obiee pull
        dirpath = 'dataflow/0fullpulls/'
        q_output.to_csv(dirpath + 'obieefull_' + q['destname'], index=False)
        # q_output.to_csv(dirpath + 'hist/' + date1 + 'obieefull_' + q['destname'], index=False)
        # taking only needed columns
        q_output = q_output.loc[:, q['finalcols']]
        ebs[q['name']] = q_output
    # ****************************************************************************************************

    # special modifications to POH
    # wanted to use the obiee-birst query
    with open('obiee/poh_renames.json') as json_data:
        cols = json.load(json_data)
    ebs['poh'] = pd.merge(ebs['poh'], ebs['reforderopp'], left_on='ORDER_NUMBER', right_on='ORDER_NUMBER', how='left')
    ebs['poh'] = ebs['poh'].rename(columns=cols)
    ebs['poh'] = ebs['poh'].filter(cols.values())
    ebs['poh']['FISCAL_YYYYMM'] = ebs['poh']['FISCAL_YYYYMM'].apply(mf.fp)
    # ****************************************************************************************************

    #modify obiee order num
    ebs['reforderopp']['OPPORTUNITY_ID'] = ebs['reforderopp']['OPPORTUNITY_ID'].apply(lambda x: 'CLOpp-26209' if x == 'ClOpp-26209' else x)

    # creating obiee rev fact table - rec/def + bill ev + po
    ebs['revfact'] = pd.concat([ebs['billev'], ebs['defrecrev'], ebs['poh']])

    # typecast lookup amounts to int for consistent lookups
    # ebs['revfact']['PARTYNUMBER_AKA_REGISTRYID'] = ebs['revfact']['PARTYNUMBER_AKA_REGISTRYID'].astype(int)
    # ebs['revfact']['BILL_TO_PARTY_ID'] = ebs['revfact']['BILL_TO_PARTY_ID'].astype(int)

    # ****************************************************************************************************
    # save ebs list to csv
    dirpath = 'dataflow/1datasources/obiee/'
    for ebs_data in ebs:
        ebs[ebs_data].to_csv(dirpath + ebs_data + '.csv', index=False, header=True, index_label=False)
        # ebs[ebs_data].to_csv(dirpath + '/hist/' + date1 + ebs_data + '.csv', index=False, header=True, index_label=False)
    return ebs
